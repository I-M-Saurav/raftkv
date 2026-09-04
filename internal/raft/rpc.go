package raft

import (
	"fmt"
	"log"
	"net"
	"sync"

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

	state *State     // shared Raft state — same instance used by election.go and raft.go
	mu    sync.Mutex // guards all access to state; will move to RaftNode in raft.go later
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
//
// Figure 2: RequestVote RPC receiver implementation.
//
// TODO: The actual vote-granting logic (term checks, votedFor checks, log
// up-to-dateness per §5.4.1) belongs in election.go's HandleRequestVote().
// This stub always denies the vote — it will be replaced once election.go
// is implemented. The handler's final shape will be:
//
//	s.mu.Lock()
//	defer s.mu.Unlock()
//	resp := HandleRequestVote(s.state, req)
//	return resp, nil
func (s *RPCServer) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("[Node %d] received RequestVote from candidate %d (term=%d) — STUB: denying",
		s.state.NodeID, req.CandidateId, req.Term)

	// Temporary stub: always deny. Replaced in election.go integration.
	return &raftpb.RequestVoteResponse{
		Term:        int64(s.state.CurrentTerm),
		VoteGranted: false,
	}, nil
}

// AppendEntries handles an incoming AppendEntries RPC from the leader.
//
// Figure 2: AppendEntries RPC receiver implementation.
//
// TODO: The actual heartbeat/log-replication logic belongs in election.go
// (heartbeat timer reset, term checks) and later in replication.go (Phase 4
// log matching). This stub always rejects — it will be replaced once
// election.go is implemented. The handler's final shape will be:
//
//	s.mu.Lock()
//	defer s.mu.Unlock()
//	resp := HandleAppendEntries(s.state, req)
//	return resp, nil
func (s *RPCServer) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("[Node %d] received AppendEntries from leader %d (term=%d, entries=%d) — STUB: rejecting",
		s.state.NodeID, req.LeaderId, req.Term, len(req.Entries))

	// Temporary stub: always reject. Replaced in election.go integration.
	return &raftpb.AppendEntriesResponse{
		Term:    int64(s.state.CurrentTerm),
		Success: false,
	}, nil
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
