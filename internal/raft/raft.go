package raft

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"raftkv/internal/raftpb"
)

// ─────────────────────────────────────────────────────────────────────────────
// RaftNode — top-level orchestrator for a single Raft participant
// ─────────────────────────────────────────────────────────────────────────────

// RaftNode ties together the Raft state, the gRPC server (rpc.go), and the
// election timer + heartbeat goroutines (election.go). It is the single entry
// point that main.go calls to start a Raft node.
type RaftNode struct {
	State      *State
	listenAddr string    // gRPC listen address (e.g. "localhost:9001")
	rpcServer  *RPCServer
}

// NewRaftNode constructs a RaftNode. It creates the RPCServer internally —
// the caller only needs to provide the shared State and the gRPC listen address.
func NewRaftNode(state *State, listenAddr string) *RaftNode {
	return &RaftNode{
		State:      state,
		listenAddr: listenAddr,
		rpcServer:  NewRPCServer(state),
	}
}

// Start launches all the background goroutines that make this node a
// functioning Raft participant:
//
//  1. gRPC server — listens for incoming RequestVote and AppendEntries RPCs
//  2. Election timer — starts elections when no heartbeat is received (§5.2)
//  3. Leadership supervisor — spawns/stops the heartbeat goroutine based on
//     leadership transitions (BecameLeaderCh / StepDownCh)
//
// Start itself does NOT block — it launches goroutines and returns immediately.
// The caller should block on something else (e.g. the HTTP server, or a signal).
func (rn *RaftNode) Start() {
	// 1. Start the gRPC server in its own goroutine (it blocks on Serve).
	go func() {
		if err := StartGRPCServer(rn.listenAddr, rn.rpcServer); err != nil {
			log.Fatalf("[Node %d] gRPC server fatal error: %v", rn.State.NodeID, err)
		}
	}()

	// 2. Start the election timer goroutine.
	go RunElectionTimer(rn.State)

	// 3. Start the leadership supervisor goroutine.
	go rn.runSupervisor()
}

// runSupervisor manages the heartbeat goroutine lifecycle. It selects on
// BecameLeaderCh and StepDownCh to spawn and stop the heartbeat loop as
// this node gains and loses leadership.
//
// Invariant: at most ONE heartbeat goroutine is running at any time, and
// any existing heartbeat goroutine is fully terminated before a new one starts.
func (rn *RaftNode) runSupervisor() {
	var stopHeartbeatCh chan struct{}
	var heartbeatDone chan struct{}

	stopHeartbeat := func() {
		if stopHeartbeatCh != nil {
			close(stopHeartbeatCh)
			<-heartbeatDone
			stopHeartbeatCh = nil
			heartbeatDone = nil
		}
	}

	for {
		select {
		case <-rn.State.BecameLeaderCh:
			// Drain any stale StepDownCh signal from a prior term so it doesn't
			// immediately cancel the new leadership session on the next iteration.
			select {
			case <-rn.State.StepDownCh:
			default:
			}

			// Ensure previous heartbeat goroutine is stopped and fully terminated
			// before spawning a new one.
			stopHeartbeat()

			// Re-verify under lock that we are still leader and capture the term.
			rn.State.Lock()
			isLeader := rn.State.CurrentState == Leader
			term := rn.State.CurrentTerm
			rn.State.Unlock()

			if !isLeader {
				continue
			}

			stopHeartbeatCh = make(chan struct{})
			heartbeatDone = make(chan struct{})
			log.Printf("[Node %d] supervisor: spawning heartbeat goroutine for term %d", rn.State.NodeID, term)
			go func(t int, stopCh <-chan struct{}, done chan<- struct{}) {
				defer close(done)
				sendHeartbeats(rn.State, t, stopCh)
			}(term, stopHeartbeatCh, heartbeatDone)

		case <-rn.State.StepDownCh:
			// We just stepped down from leader. Stop the heartbeat goroutine.
			if stopHeartbeatCh != nil {
				log.Printf("[Node %d] supervisor: stopping heartbeat goroutine (stepped down)", rn.State.NodeID)
				stopHeartbeat()
			}

		case <-rn.State.StopCh:
			// Shutdown. Stop heartbeats if running and exit.
			stopHeartbeat()
			log.Printf("[Node %d] supervisor: shutting down", rn.State.NodeID)
			return
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Heartbeat Loop — Leader sends empty AppendEntries to maintain authority
// ─────────────────────────────────────────────────────────────────────────────

// sendHeartbeats runs a ticker at 50ms intervals and sends empty AppendEntries
// RPCs to all peers on each tick. This implements §5.2's heartbeat mechanism:
//
//	"The leader sends periodic heartbeats (AppendEntries RPCs that carry no
//	 log entries) to all followers in order to maintain its authority."
//
// 50ms is well below the 150-300ms election timeout range, satisfying §5.2's
// requirement that "broadcastTime ≪ electionTimeout."
//
// The function exits when:
//   - stopCh is closed (supervisor detected step-down or new leadership)
//   - state.StopCh is closed (node shutdown)
//   - a peer response reveals a higher term (self-initiated step-down)
//   - state.CurrentState != Leader or state.CurrentTerm != term (demoted or term changed)
func sendHeartbeats(state *State, term int, stopCh <-chan struct{}) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Snapshot the state we need under the lock, then release
			// immediately before doing any network I/O.
			state.Lock()
			if state.CurrentState != Leader || state.CurrentTerm != term {
				// We're no longer leader for this term. Stop sending heartbeats.
				state.Unlock()
				return
			}
			currentTerm := state.CurrentTerm
			nodeID := state.NodeID
			peers := state.Peers
			// Phase 3: heartbeat only — no log entries.
			// Phase 4 will populate these from the leader's log.
			commitIndex := state.CommitIndex
			state.Unlock()

			// Send empty AppendEntries to all peers concurrently.
			// Same goroutine-per-peer pattern as startElection's RequestVote fan-out.
			for _, peer := range peers {
				go func(peerAddr string, peerID int64) {
					ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
					defer cancel()

					conn, err := grpc.DialContext(ctx, peerAddr,
						grpc.WithTransportCredentials(insecure.NewCredentials()),
						grpc.WithBlock(),
					)
					if err != nil {
						// Peer is down — not fatal, it will rejoin later.
						// Don't log every failure at 50ms intervals to avoid log spam.
						return
					}
					defer conn.Close()

					client := raftpb.NewRaftServiceClient(conn)
					resp, err := client.AppendEntries(ctx, &raftpb.AppendEntriesRequest{
						Term:         int64(currentTerm),
						LeaderId:     int64(nodeID),
						PrevLogIndex: 0, // Phase 3: ignored by the heartbeat-only receiver
						PrevLogTerm:  0, // Phase 3: ignored
						Entries:      nil, // empty = heartbeat (§5.2)
						LeaderCommit: int64(commitIndex),
					})
					if err != nil {
						return
					}

					// Figure 2, "Rules for Servers → All Servers":
					// "If RPC response contains term T > currentTerm:
					// set currentTerm = T, convert to follower."
					if resp.Term > int64(currentTerm) {
						state.Lock()
						if resp.Term > int64(state.CurrentTerm) {
							wasLeader := state.CurrentState == Leader
							log.Printf("[Node %d] heartbeat response from peer %d has higher term %d > %d — stepping down",
								nodeID, peerID, resp.Term, state.CurrentTerm)
							state.CurrentTerm = int(resp.Term)
							state.VotedFor = -1
							state.CurrentState = Follower
							if wasLeader {
								notifyStepDown(state)
							}
						}
						state.Unlock()
					}
				}(peer.Address, peer.ID)
			}

		case <-stopCh:
			// Supervisor told us to stop (step-down or new leadership term).
			return

		case <-state.StopCh:
			// Node shutdown.
			return
		}
	}
}
