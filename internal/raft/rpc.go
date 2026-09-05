package raft

import (
	"fmt"
	"log"
	"net"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"raftkv/internal/raftpb"
)

// RPCServer implements the raftpb.RaftServiceServer interface (generated from
// proto/raft.proto). It handles incoming RequestVote and AppendEntries RPCs
// by delegating to the core Raft logic functions defined in election.go.
//
// This struct is the gRPC transport layer only — it does not contain any Raft
// decision logic itself. See the explanation in the package-level architecture:
//   - rpc.go:       "how does the message arrive?" (gRPC plumbing)
//   - election.go:  "what does Raft say to do?"    (Figure 2 rules)
type RPCServer struct {
	raftpb.UnimplementedRaftServiceServer // forward-compatibility per gRPC convention

	state *State // shared Raft state — same instance used by election.go and raft.go
	             // Locking is handled by state.Lock()/Unlock(); RPCServer has no mutex of its own.
}

// NewRPCServer creates an RPCServer that operates on the given shared State.
// The caller must ensure that no other code accesses state without going through
// the same mutex (this will be enforced structurally once RaftNode exists in raft.go).
func NewRPCServer(state *State) *RPCServer {
	return &RPCServer{
		state: state,
	}
}

// RequestVote handles an incoming RequestVote RPC from a candidate.
// Figure 2: RequestVote RPC receiver implementation.
// Delegates to HandleRequestVote in election.go for the actual Raft logic.
// The state mutex is acquired here so that HandleRequestVote can assume it's held.
func (s *RPCServer) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	s.state.Lock()
	defer s.state.Unlock()
	return HandleRequestVote(s.state, req), nil
}

// AppendEntries handles an incoming AppendEntries RPC from the leader.
// Figure 2: AppendEntries RPC receiver implementation.
// Delegates to HandleAppendEntries in election.go for the actual Raft logic.
// The state mutex is acquired here so that HandleAppendEntries can assume it's held.
func (s *RPCServer) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	s.state.Lock()
	defer s.state.Unlock()
	return HandleAppendEntries(s.state, req), nil
}

// StartGRPCServer creates a gRPC server, registers the RaftService, and begins
// serving on the given address (e.g. "localhost:9001"). This function blocks
// until the server is stopped — the caller should run it in its own goroutine.
func StartGRPCServer(address string, rpcServer *RPCServer) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("rpc: failed to listen on %s: %w", address, err)
	}

	grpcServer := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(grpcServer, rpcServer)

	log.Printf("[Node %d] gRPC server listening on %s", rpcServer.state.NodeID, address)

	if err := grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("rpc: gRPC server failed: %w", err)
	}
	return nil
}
