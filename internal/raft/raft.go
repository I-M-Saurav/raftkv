package raft

import (
	"context"
	"log"
	"time"

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
	peerPool   *PeerConnections
}

// NewRaftNode constructs a RaftNode. It creates the RPCServer internally and
// eagerly initializes persistent gRPC client connections to all peers.
func NewRaftNode(state *State, listenAddr string) (*RaftNode, error) {
	peerPool, err := NewPeerConnections(state.Peers)
	if err != nil {
		return nil, err
	}
	return &RaftNode{
		State:      state,
		listenAddr: listenAddr,
		rpcServer:  NewRPCServer(state),
		peerPool:   peerPool,
	}, nil
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

	// 2. Start the election timer goroutine with persistent peer connections.
	go RunElectionTimer(rn.State, rn.peerPool)

	// 3. Start the leadership supervisor goroutine.
	go rn.runSupervisor()
}

// Close terminates all persistent gRPC peer connections.
func (rn *RaftNode) Close() {
	if rn.peerPool != nil {
		rn.peerPool.Close()
	}
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
				sendHeartbeats(rn.State, t, rn.peerPool, stopCh)
			}(term, stopHeartbeatCh, heartbeatDone)

		case <-rn.State.StepDownCh:
			// We just stepped down from leader. Stop the heartbeat goroutine.
			if stopHeartbeatCh != nil {
				log.Printf("[Node %d] supervisor: stopping heartbeat goroutine (stepped down)", rn.State.NodeID)
				stopHeartbeat()
			}

		case <-rn.State.StopCh:
			// Shutdown. Stop heartbeats if running, close peer connections, and exit.
			stopHeartbeat()
			rn.peerPool.Close()
			log.Printf("[Node %d] supervisor: shutting down", rn.State.NodeID)
			return
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Replication Loop — Leader sends AppendEntries with real log entries (§5.3)
// ─────────────────────────────────────────────────────────────────────────────

// sendHeartbeats runs the leader's replication/heartbeat loop. On each
// iteration (triggered by either the 50ms periodic ticker OR a signal from
// TriggerReplicationCh after a ProposeCommand), it sends AppendEntries RPCs
// to all peers with the correct per-peer log entries.
//
// §5.3: "The leader appends the command to its log as a new entry, then
// issues AppendEntries RPCs in parallel to each of the other servers to
// replicate the entry."
//
// When no new entries exist for a peer (nextIndex > lastLogIndex), the RPC
// carries an empty entries slice — this is a valid heartbeat per §5.2.
//
// 50ms is well below the 150-300ms election timeout range, satisfying §5.2's
// requirement that "broadcastTime ≪ electionTimeout."
//
// The function exits when:
//   - stopCh is closed (supervisor detected step-down or new leadership)
//   - state.StopCh is closed (node shutdown)
//   - a peer response reveals a higher term (self-initiated step-down)
//   - state.CurrentState != Leader or state.CurrentTerm != term (demoted or term changed)
func sendHeartbeats(state *State, term int, peerPool *PeerConnections, stopCh <-chan struct{}) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Periodic tick — send heartbeats/replication to all peers.

		case <-state.TriggerReplicationCh:
			// ProposeCommand just appended a new entry — replicate immediately
			// rather than waiting up to 50ms for the next tick.

		case <-stopCh:
			// Supervisor told us to stop (step-down or new leadership term).
			return

		case <-state.StopCh:
			// Node shutdown.
			return
		}

		// ── Snapshot state under the lock ────────────────────────────────
		// We snapshot everything we need (per-peer nextIndex, the log, etc.)
		// and release the lock before doing any network I/O.
		state.Lock()
		if state.CurrentState != Leader || state.CurrentTerm != term {
			// We're no longer leader for this term. Stop.
			state.Unlock()
			return
		}
		currentTerm := state.CurrentTerm
		nodeID := state.NodeID
		peers := state.Peers
		commitIndex := state.CommitIndex

		// Snapshot per-peer nextIndex and the log for entry computation.
		// Figure 2, "Rules for Servers → Leaders":
		// "If last log index ≥ nextIndex for a follower: send AppendEntries
		// RPC with log entries starting at nextIndex"
		peerNextIndex := make(map[int64]int, len(peers))
		for _, peer := range peers {
			peerNextIndex[peer.ID] = state.NextIndex[int(peer.ID)]
		}
		// Snapshot the full log slice header. Since we only append (never
		// mutate in-place) and hold the lock during the snapshot, this is
		// safe. The goroutines below read from this snapshot, not from
		// state.Log directly.
		logSnapshot := make([]LogEntry, len(state.Log))
		copy(logSnapshot, state.Log)
		lastLogIndex := len(logSnapshot) - 1 // same as state.LastLogIndex()
		state.Unlock()

		// ── Send AppendEntries to all peers concurrently ─────────────────
		for _, peer := range peers {
			go func(peerAddr string, peerID int64) {
				client, ok := peerPool.Client(peerID)
				if !ok {
					return
				}

				nextIdx := peerNextIndex[peerID]

				// Figure 2: prevLogIndex and prevLogTerm are the index/term
				// of the log entry immediately preceding the new ones we're
				// about to send. The follower uses these for consistency
				// checking (sub-step 2).
				prevLogIndex := nextIdx - 1
				prevLogTerm := 0
				if prevLogIndex >= 0 && prevLogIndex < len(logSnapshot) {
					prevLogTerm = logSnapshot[prevLogIndex].Term
				}

				// Build the entries slice: everything from nextIndex through
				// the end of the leader's log. May be empty (= pure heartbeat).
				var entries []*raftpb.LogEntry
				if nextIdx <= lastLogIndex {
					for i := nextIdx; i <= lastLogIndex; i++ {
						entry := logSnapshot[i]
						// Convert our internal LogEntry to the proto LogEntry.
						// LogEntry.Command is interface{} in state.go but is
						// always []byte when set via ProposeCommand.
						var cmdBytes []byte
						if entry.Command != nil {
							cmdBytes, _ = entry.Command.([]byte)
						}
						entries = append(entries, &raftpb.LogEntry{
							Term:    int64(entry.Term),
							Index:   int64(entry.Index),
							Command: cmdBytes,
						})
					}
				}

				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()

				resp, err := client.AppendEntries(ctx, &raftpb.AppendEntriesRequest{
					Term:         int64(currentTerm),
					LeaderId:     int64(nodeID),
					PrevLogIndex: int64(prevLogIndex),
					PrevLogTerm:  int64(prevLogTerm),
					Entries:      entries,
					LeaderCommit: int64(commitIndex),
				})
				if err != nil {
					return
				}

				// ── Handle AppendEntries response ────────────────────────
				// Figure 2, "Rules for Servers → All Servers":
				// "If RPC response contains term T > currentTerm:
				// set currentTerm = T, convert to follower."
				if resp.Term > int64(currentTerm) {
					state.Lock()
					if resp.Term > int64(state.CurrentTerm) {
						wasLeader := state.CurrentState == Leader
						log.Printf("[Node %d] AppendEntries response from peer %d has higher term %d > %d — stepping down",
							nodeID, peerID, resp.Term, state.CurrentTerm)
						state.CurrentTerm = int(resp.Term)
						state.VotedFor = -1
						state.CurrentState = Follower
						if wasLeader {
							notifyStepDown(state)
						}
					}
					state.Unlock()
					return
				}

				// Figure 2, "Rules for Servers → Leaders":
				state.Lock()
				// Re-check we're still leader for this term before mutating
				// volatile leader state.
				if state.CurrentState != Leader || state.CurrentTerm != term {
					state.Unlock()
					return
				}

				if resp.Success {
					// "If successful: update nextIndex and matchIndex for
					// follower" (Figure 2)
					//
					// lastSentIndex is the index of the last entry we included
					// in this RPC. If we sent no entries, these values stay
					// unchanged (the peer already had everything).
					//
					// MONOTONIC GUARD: Because the 50ms ticker can fire while
					// a previous RPC is still in-flight (100ms timeout > 50ms
					// interval), two AppendEntries to the same peer can be
					// in-flight simultaneously. If their responses arrive out
					// of order (newer first, older second), the older response
					// carries a smaller lastSentIndex. Without this guard, it
					// would REGRESS matchIndex/nextIndex backward, corrupting
					// the leader's view of what the follower actually has.
					// Only advance forward, never backward.
					if len(entries) > 0 {
						lastSentIndex := int(entries[len(entries)-1].Index)
						if lastSentIndex > state.MatchIndex[int(peerID)] {
							state.MatchIndex[int(peerID)] = lastSentIndex
						}
						if lastSentIndex+1 > state.NextIndex[int(peerID)] {
							state.NextIndex[int(peerID)] = lastSentIndex + 1
						}
					}
				} else {
					// "If AppendEntries fails because of log inconsistency:
					// decrement nextIndex and retry" (Figure 2, §5.3)
					//
					// STALE-FAILURE GUARD: Only decrement if nextIndex hasn't
					// been advanced past what THIS RPC was working with. If a
					// newer RPC already succeeded and moved nextIndex forward
					// (state.NextIndex > nextIdx), this failure is from a stale
					// in-flight RPC — the follower demonstrably has entries
					// beyond what we were trying to send, so decrementing would
					// be wrong.
					//
					// TODO(phase4-optimization): Implement optimized conflict-term
					// backtracking from the paper's extended version (§5.3) for
					// faster log reconciliation. The current bare-minimum approach
					// decrements by 1 each round, which is O(n) in the worst case
					// but correct.
					if state.NextIndex[int(peerID)] <= nextIdx && state.NextIndex[int(peerID)] > 1 {
						state.NextIndex[int(peerID)]--
						log.Printf("[Node %d] AppendEntries to peer %d failed (log inconsistency) — decremented nextIndex to %d",
							nodeID, peerID, state.NextIndex[int(peerID)])
					}
				}
				state.Unlock()

				// NOTE: commitIndex advancement is NOT implemented here.
				// That's Phase 4, sub-step 2 — it depends on matchIndex
				// tracking working first.


			}(peer.Address, peer.ID)
		}
	}
}

