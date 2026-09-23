package raft

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"time"

	"raftkv/internal/raftpb"
)

// ─────────────────────────────────────────────────────────────────────────────
// Election Timer
// ─────────────────────────────────────────────────────────────────────────────

// randomElectionTimeout returns a random duration between 150ms and 300ms.
// §5.2: "Raft uses randomized election timeouts to ensure that split votes
// are rare and that they are resolved quickly." A NEW random value is
// generated on every call — this is critical; using a single fixed timeout
// computed at startup would cause synchronized elections after a leader crash.
func randomElectionTimeout() time.Duration {
	// rand.IntN is safe for concurrent use (Go 1.22+), no seed needed.
	ms := 150 + rand.IntN(151) // [150, 300] inclusive
	return time.Duration(ms) * time.Millisecond
}

// RunElectionTimer is meant to be run as a goroutine (go RunElectionTimer(state)).
// It manages the election timeout for this node using a time.Timer and a select
// loop, as described in the implementation plan.
//
// The timer fires after a random 150-300ms. If it fires without being reset,
// the node starts an election (§5.2). Three events reset the timer:
//   - A valid AppendEntries heartbeat from the current leader (via HandleAppendEntries)
//   - This node grants a vote to a candidate (via HandleRequestVote)
//   - This node starts a new election itself (via startElection)
//
// These events send on state.ResetElectionTimerCh. A shutdown signal on
// state.StopCh terminates the goroutine cleanly.
func RunElectionTimer(state *State, peerPool *PeerConnections) {
	timer := time.NewTimer(randomElectionTimeout())
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			// Election timeout fired — no heartbeat or vote grant arrived in time.
			// §5.2: "If a follower receives no communication over a period of time
			// called the election timeout, then it assumes there is no viable leader
			// and begins an election to choose a new leader."
			//
			// startElection runs in its own goroutine so that the timer loop
			// remains responsive to ResetElectionTimerCh and StopCh while the
			// election is in progress. This is safe because:
			//   1. startElection does its own state.Lock()/Unlock() internally —
			//      it never assumes the caller holds the lock.
			//   2. All shared state access goes through the mutex; the RPC fan-out
			//      phase uses only captured local snapshots, not live state fields.
			//   3. If the timer fires again before a previous election finishes,
			//      the new startElection will increment the term further. The old
			//      election's stale-term guards (electionTerm != state.CurrentTerm)
			//      will cause it to discard any late-arriving votes harmlessly.
			go startElection(state, peerPool)

			// Reset the timer immediately for the next round. If the election
			// completes and we become Leader, startElection checks CurrentState
			// and returns early on subsequent calls.
			timer.Reset(randomElectionTimeout())

		case <-state.ResetElectionTimerCh:
			// A heartbeat arrived, or we granted a vote, or we started a new election.
			// Reset the timer with a fresh random duration.
			// time.Timer.Reset requires the timer to be stopped or drained first.
			if !timer.Stop() {
				// Timer already fired; drain the channel to avoid a spurious trigger.
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(randomElectionTimeout())

		case <-state.StopCh:
			// Shutdown signal — terminate the timer goroutine.
			log.Printf("[Node %d] election timer stopped (shutdown)", state.NodeID)
			return
		}
	}
}

// resetElectionTimer performs a non-blocking send on state.ResetElectionTimerCh
// to signal RunElectionTimer to restart its countdown. Non-blocking so that
// RPC handlers (which hold the state mutex) never block waiting for the timer
// goroutine to drain the channel.
//
// CALLER MUST HOLD state.Lock() — this function is called from HandleRequestVote
// and HandleAppendEntries, which are invoked with the lock already held.
func resetElectionTimer(state *State) {
	select {
	case state.ResetElectionTimerCh <- struct{}{}:
	default:
		// Channel already has a pending signal; the timer will reset anyway.
	}
}

// notifyStepDown performs a non-blocking send on state.StepDownCh to signal
// raft.go's supervisor that this node has stepped down FROM Leader. The
// supervisor uses this to stop the heartbeat-sending goroutine.
//
// CALLER MUST HOLD state.Lock(). Only call this when the node WAS Leader
// before the transition — Candidate/Follower step-downs should NOT signal here.
func notifyStepDown(state *State) {
	select {
	case state.StepDownCh <- struct{}{}:
	default:
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Leader Election — Candidate Logic
// ─────────────────────────────────────────────────────────────────────────────

// startElection implements the candidate's election procedure (§5.2).
//
// Steps (Figure 2, "Rules for Servers → Candidates"):
//  1. Increment currentTerm
//  2. Vote for self
//  3. Reset election timer
//  4. Send RequestVote RPCs to all other servers
//
// Votes are collected concurrently via a channel. As soon as a majority is
// reached, we transition to Leader immediately without waiting for stragglers.
// If any peer reports a higher term, we step down to Follower and abandon
// the election (Figure 2, "Rules for Servers → All Servers").
func startElection(state *State, peerPool *PeerConnections) {
	state.Lock()

	// If we're already a Leader, don't start an election. This can happen if
	// the timer fires just as we finished winning a previous election.
	if state.CurrentState == Leader {
		state.Unlock()
		return
	}

	// §5.2 step 1: "Increment currentTerm"
	state.CurrentTerm++
	electionTerm := state.CurrentTerm

	// §5.2 step 2: "Vote for itself"
	state.VotedFor = state.NodeID
	state.CurrentState = Candidate
	state.LeaderID = -1 // presumed leader is gone; unknown until a new one confirms itself

	// Capture log state for the RequestVote RPC while we hold the lock.
	// These values are used in the §5.4.1 "up-to-date" check by receivers.
	lastLogIndex := state.LastLogIndex()
	lastLogTerm := state.LastLogTerm()
	nodeID := state.NodeID
	peers := state.Peers

	log.Printf("[Node %d] starting election for term %d (lastLogIndex=%d, lastLogTerm=%d)",
		nodeID, electionTerm, lastLogIndex, lastLogTerm)

	state.Unlock()

	// §5.2 step 3: "Reset election timer" — signal the timer goroutine.
	resetElectionTimer(state)

	// If there are no peers, we're the only node — we win immediately.
	if len(peers) == 0 {
		state.Lock()
		if state.CurrentTerm == electionTerm && state.CurrentState == Candidate {
			log.Printf("[Node %d] won election for term %d (single-node cluster)", nodeID, electionTerm)
			becomeLeader(state)
		}
		state.Unlock()
		return
	}

	// §5.2 step 4: "Send RequestVote RPCs to all other servers"
	// One goroutine per peer, results collected on a channel.
	type voteResult struct {
		peerID      int64
		term        int64
		voteGranted bool
		err         error
	}

	voteCh := make(chan voteResult, len(peers))

	for _, peer := range peers {
		go func(peerAddr string, peerID int64) {
			client, ok := peerPool.Client(peerID)
			if !ok {
				voteCh <- voteResult{peerID: peerID, err: fmt.Errorf("no persistent client for peer %d", peerID)}
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			resp, err := client.RequestVote(ctx, &raftpb.RequestVoteRequest{
				Term:         int64(electionTerm),
				CandidateId:  int64(nodeID),
				LastLogIndex: int64(lastLogIndex),
				LastLogTerm:  int64(lastLogTerm),
			})
			if err != nil {
				voteCh <- voteResult{peerID: peerID, err: err}
				return
			}
			voteCh <- voteResult{
				peerID:      peerID,
				term:        resp.Term,
				voteGranted: resp.VoteGranted,
			}
		}(peer.Address, peer.ID)
	}

	// Count votes. We start with 1 (our own vote).
	// Majority = (len(peers) + 1) / 2 + 1, i.e. more than half the cluster.
	votesGranted := 1                       // count our own vote
	totalNodes := len(peers) + 1            // cluster size including self
	majority := totalNodes/2 + 1            // strict majority
	votesReceived := 0                      // how many responses we've processed

	for votesReceived < len(peers) {
		result := <-voteCh
		votesReceived++

		if result.err != nil {
			// RPC failure (peer down, timeout). Not a vote — just skip.
			// §5.2: "If a follower or candidate crashes, then future
			// RequestVote [...] RPCs sent to it will fail. Raft handles
			// these failures by retrying indefinitely" — but within a
			// single election round, a failed RPC simply doesn't contribute.
			log.Printf("[Node %d] RequestVote to peer %d failed: %v", nodeID, result.peerID, result.err)
			continue
		}

		// Figure 2, "Rules for Servers → All Servers":
		// "If RPC response contains term T > currentTerm: set currentTerm = T,
		// convert to follower."
		if result.term > int64(electionTerm) {
			state.Lock()
			if result.term > int64(state.CurrentTerm) {
				wasLeader := state.CurrentState == Leader
				log.Printf("[Node %d] peer %d has higher term %d (ours: %d) — stepping down",
					nodeID, result.peerID, result.term, state.CurrentTerm)
				state.CurrentTerm = int(result.term)
				state.VotedFor = -1
				state.LeaderID = -1
				state.CurrentState = Follower
				if wasLeader {
					notifyStepDown(state)
				}
			}
			state.Unlock()
			return // abandon this election
		}

		if result.voteGranted {
			votesGranted++
			log.Printf("[Node %d] received vote from peer %d (%d/%d needed)",
				nodeID, result.peerID, votesGranted, majority)

			if votesGranted >= majority {
				// §5.2: "A candidate wins an election if it receives votes
				// from a majority of the servers in the full cluster for
				// the same term."
				state.Lock()
				// Guard: only become leader if we're still a Candidate in
				// the same term. Another goroutine (e.g. a higher-term
				// AppendEntries) may have already demoted us.
				if state.CurrentTerm == electionTerm && state.CurrentState == Candidate {
					log.Printf("[Node %d] won election for term %d (%d votes)",
						nodeID, electionTerm, votesGranted)
					becomeLeader(state)
				}
				state.Unlock()
				return // don't wait for straggler votes
			}
		} else {
			log.Printf("[Node %d] vote denied by peer %d (term=%d)", nodeID, result.peerID, result.term)
		}
	}

	// If we get here, we processed all responses without reaching a majority.
	// §5.2: "If election timeout elapses: start new election" — the timer
	// goroutine in RunElectionTimer will handle this automatically.
	log.Printf("[Node %d] election for term %d failed (got %d/%d votes, needed %d)",
		nodeID, electionTerm, votesGranted, totalNodes, majority)
}

// becomeLeader transitions this node to the Leader state and initializes the
// volatile leader-only state (Figure 2).
//
// CALLER MUST HOLD state.Lock().
//
// After setting the state and initializing nextIndex/matchIndex, it signals
// BecameLeaderCh so that raft.go's supervisor can spawn the heartbeat goroutine.
func becomeLeader(state *State) {
	state.CurrentState = Leader
	state.LeaderID = state.NodeID // we are now the leader
	// Figure 2, "Volatile state on leaders (Reinitialized after election)":
	// "nextIndex[]: initialized to leader last log index + 1"
	// "matchIndex[]: initialized to 0"
	nextIdx := state.LastLogIndex() + 1
	for _, peer := range state.Peers {
		state.NextIndex[int(peer.ID)] = nextIdx
		state.MatchIndex[int(peer.ID)] = 0
	}

	// Signal raft.go's supervisor to start sending heartbeats.
	// Non-blocking: if a previous signal hasn't been consumed yet (shouldn't
	// happen normally), we don't block — the supervisor will still process it.
	select {
	case state.BecameLeaderCh <- struct{}{}:
	default:
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// RequestVote RPC Handler — Figure 2 Receiver Implementation
// ─────────────────────────────────────────────────────────────────────────────

// HandleRequestVote implements the receiver side of the RequestVote RPC,
// following Figure 2 of the Raft paper exactly.
//
// CALLER MUST HOLD state.Lock() — this is called from rpc.go's RequestVote
// handler, which acquires the lock before calling us. Do NOT lock again here
// or you will deadlock.
//
// Returns: *raftpb.RequestVoteResponse with the current term and vote decision.
func HandleRequestVote(state *State, req *raftpb.RequestVoteRequest) *raftpb.RequestVoteResponse {
	// ── Figure 2, RequestVote RPC, rule 1: ──────────────────────────────
	// "Reply false if term < currentTerm (§5.1)"
	//
	// A candidate from a stale term has no authority. Reject immediately
	// and tell it our (higher) term so it can step down.
	if int(req.Term) < state.CurrentTerm {
		log.Printf("[Node %d] rejecting RequestVote from candidate %d: stale term %d < %d (§5.1)",
			state.NodeID, req.CandidateId, req.Term, state.CurrentTerm)
		return &raftpb.RequestVoteResponse{
			Term:        int64(state.CurrentTerm),
			VoteGranted: false,
		}
	}

	// ── Figure 2, "Rules for Servers → All Servers": ────────────────────
	// "If RPC request contains term T > currentTerm: set currentTerm = T,
	// convert to follower (§5.1)"
	//
	// WHY THIS MUST HAPPEN BEFORE EVALUATING THE VOTE:
	// A term change is a clean slate. The VotedFor field records who we
	// voted for in CurrentTerm. If req.Term > CurrentTerm, we're entering
	// a new term where we haven't voted yet — so VotedFor must be reset
	// to -1 BEFORE the votedFor check below. If we checked votedFor first,
	// we'd reject this candidate based on a stale vote from a previous term.
	// Example: we're in term 3, voted for node 5. Candidate node 7 arrives
	// with term 5. Without step-down-first, we'd see VotedFor=5 ≠ 7 and
	// reject — even though we've never voted in term 5. With step-down-first,
	// we update to term 5, set VotedFor=-1, then correctly evaluate the vote.
	if int(req.Term) > state.CurrentTerm {
		wasLeader := state.CurrentState == Leader
		log.Printf("[Node %d] RequestVote from candidate %d has higher term %d > %d — stepping down to Follower before evaluating (§5.1)",
			state.NodeID, req.CandidateId, req.Term, state.CurrentTerm)
		state.CurrentTerm = int(req.Term)
		state.VotedFor = -1
		state.LeaderID = -1
		state.CurrentState = Follower
		if wasLeader {
			notifyStepDown(state)
		}
	}

	// ── Figure 2, RequestVote RPC, rule 2 (first part): ─────────────────
	// "If votedFor is null or candidateId..."
	//
	// If we've already voted for a DIFFERENT candidate in this term, reject.
	// VotedFor == -1 means "null" (haven't voted). VotedFor == candidateId
	// means we already voted for this same candidate (idempotent — can happen
	// if the candidate retries a failed RPC).
	candidateID := int(req.CandidateId)
	if state.VotedFor != -1 && state.VotedFor != candidateID {
		log.Printf("[Node %d] rejecting RequestVote from candidate %d: already voted for %d in term %d (Figure 2 votedFor check)",
			state.NodeID, req.CandidateId, state.VotedFor, state.CurrentTerm)
		return &raftpb.RequestVoteResponse{
			Term:        int64(state.CurrentTerm),
			VoteGranted: false,
		}
	}

	// ── Figure 2, RequestVote RPC, rule 2 (second part): ────────────────
	// "...and candidate's log is at least as up-to-date as receiver's log,
	// grant vote (§5.4.1)"
	//
	// §5.4.1 defines "up-to-date" precisely:
	//   "If the logs have last entries with different terms, then the log
	//    with the later term is more up-to-date."
	//   "If the logs end with the same term, then whichever log is longer
	//    is more up-to-date."
	//
	// We reject if the candidate's log is LESS up-to-date than ours.
	ourLastLogTerm := state.LastLogTerm()
	ourLastLogIndex := state.LastLogIndex()
	candidateLastLogTerm := int(req.LastLogTerm)
	candidateLastLogIndex := int(req.LastLogIndex)

	candidateUpToDate := false
	if candidateLastLogTerm > ourLastLogTerm {
		// Candidate's last entry is from a later term — strictly more up-to-date.
		candidateUpToDate = true
	} else if candidateLastLogTerm == ourLastLogTerm && candidateLastLogIndex >= ourLastLogIndex {
		// Same term — candidate's log is at least as long as ours.
		candidateUpToDate = true
	}

	if !candidateUpToDate {
		log.Printf("[Node %d] rejecting RequestVote from candidate %d: log not up-to-date (candidate: term=%d idx=%d, ours: term=%d idx=%d) (§5.4.1)",
			state.NodeID, req.CandidateId, candidateLastLogTerm, candidateLastLogIndex, ourLastLogTerm, ourLastLogIndex)
		return &raftpb.RequestVoteResponse{
			Term:        int64(state.CurrentTerm),
			VoteGranted: false,
		}
	}

	// ── All checks passed — grant the vote. ─────────────────────────────
	state.VotedFor = candidateID
	log.Printf("[Node %d] granting vote to candidate %d for term %d (§5.2)",
		state.NodeID, req.CandidateId, state.CurrentTerm)

	// §5.2: A follower that grants a vote should reset its election timer.
	// If it doesn't, it might time out immediately and start a competing
	// election against the candidate it just endorsed — defeating the purpose
	// of the vote.
	resetElectionTimer(state)

	return &raftpb.RequestVoteResponse{
		Term:        int64(state.CurrentTerm),
		VoteGranted: true,
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// AppendEntries RPC Handler — Full Figure 2 Receiver (Phase 4)
// ─────────────────────────────────────────────────────────────────────────────

// HandleAppendEntries implements the receiver side of the AppendEntries RPC,
// following Figure 2 of the Raft paper exactly (rules 1-5). It handles:
//   - Term checking and step-down (Figure 2, "Rules for Servers")
//   - Election timer reset (so followers don't time out while leader is alive)
//   - PrevLogIndex/PrevLogTerm consistency check (rule 2)
//   - Conflict truncation (rule 3)
//   - Idempotent append of new entries (rule 4)
//   - Follower commitIndex advancement (rule 5)
//
// CALLER MUST HOLD state.Lock() — same convention as HandleRequestVote.
func HandleAppendEntries(state *State, req *raftpb.AppendEntriesRequest) *raftpb.AppendEntriesResponse {
	// ── Figure 2, AppendEntries RPC, rule 1: ────────────────────────────
	// "Reply false if term < currentTerm (§5.1)"
	//
	// A leader from a stale term has been superseded. Reject so it learns
	// our higher term and steps down.
	if int(req.Term) < state.CurrentTerm {
		log.Printf("[Node %d] rejecting AppendEntries from leader %d: stale term %d < %d (§5.1)",
			state.NodeID, req.LeaderId, req.Term, state.CurrentTerm)
		return &raftpb.AppendEntriesResponse{
			Term:    int64(state.CurrentTerm),
			Success: false,
		}
	}

	// ── Figure 2, "Rules for Servers → All Servers": ────────────────────
	// "If RPC request contains term T > currentTerm: set currentTerm = T,
	// convert to follower (§5.1)"
	//
	// Also handles the case where term == currentTerm but we're a Candidate:
	// receiving an AppendEntries from a leader in our term means someone else
	// won the election — we must step down (§5.2: "While waiting for votes,
	// a candidate may receive an AppendEntries RPC from another server
	// claiming to be leader. If the leader's term [...] is at least as large
	// as the candidate's current term, then the candidate recognizes the
	// leader as legitimate and returns to follower state.").
	if int(req.Term) > state.CurrentTerm {
		wasLeader := state.CurrentState == Leader
		log.Printf("[Node %d] AppendEntries from leader %d has higher term %d > %d — updating term, stepping down (§5.1)",
			state.NodeID, req.LeaderId, req.Term, state.CurrentTerm)
		state.CurrentTerm = int(req.Term)
		state.VotedFor = -1
		state.LeaderID = -1
		state.CurrentState = Follower
		if wasLeader {
			notifyStepDown(state)
		}
	} else if state.CurrentState == Candidate {
		// Same term, but a leader exists — step down. NOT a leader demotion,
		// so we do NOT signal StepDownCh (no heartbeat goroutine to stop).
		log.Printf("[Node %d] AppendEntries from leader %d in same term %d — stepping down from Candidate to Follower (§5.2)",
			state.NodeID, req.LeaderId, req.Term)
		state.CurrentState = Follower
	}

	// Valid heartbeat from the current leader — reset the election timer
	// so we don't start a spurious election while the leader is alive.
	resetElectionTimer(state)

	// Record the current leader so the HTTP layer (Phase 5) can forward
	// client writes here. Set on every valid heartbeat/append, regardless
	// of whether the log-consistency check below ultimately succeeds.
	state.LeaderID = int(req.LeaderId)

	// ── Figure 2, AppendEntries RPC, rules 2-5: ─────────────────────────
	//
	// WHY THE ORDER OF RULES 2-5 MATTERS:
	//
	// Rule 2 MUST be checked before rules 3/4. Rule 2 establishes that we
	// have a valid "anchor point" — an entry at prevLogIndex whose term
	// matches prevLogTerm — confirming that the leader and follower agree
	// on the log up to (and including) that point. Without this anchor,
	// rules 3/4 would operate on a log that may have gaps or completely
	// divergent entries. Concretely: if prevLogIndex=5 and our log only
	// goes to index 2, rule 3 would try to access index 6+ (out of bounds
	// or nonsensical), and rule 4 would append entries at index 6 with
	// indices 3-5 missing — creating a gap that violates the contiguous
	// log invariant.
	//
	// Rules 3/4 MUST happen before rule 5. Rule 5 advances commitIndex,
	// which tells the apply loop "these entries are safe to execute."
	// If we advanced commitIndex before fixing conflicts (rule 3) or
	// appending entries (rule 4), the apply loop could try to execute
	// entries that haven't been written yet, or execute conflicting entries
	// that should have been truncated.

	prevLogIndex := int(req.PrevLogIndex)
	prevLogTerm := int(req.PrevLogTerm)

	// ── Rule 2: "Reply false if log doesn't contain an entry at ──────────
	//     prevLogIndex whose term matches prevLogTerm" (§5.3)
	//
	// This is the consistency check. The leader sends prevLogIndex/prevLogTerm
	// so we can verify our log matches the leader's up to the point just
	// before the new entries. If it doesn't match, we reject — the leader
	// will decrement nextIndex and retry with an earlier starting point,
	// walking backward until it finds the point where we agree.
	//
	// Special case: prevLogIndex == 0 means "sending from the very beginning
	// of the log" (the sentinel entry). This always matches — there's
	// nothing to check.
	if prevLogIndex > 0 {
		// Check 1: Do we even HAVE an entry at prevLogIndex?
		if prevLogIndex >= len(state.Log) {
			log.Printf("[Node %d] AppendEntries rule 2 FAIL: log too short (prevLogIndex=%d, logLen=%d) — rejecting",
				state.NodeID, prevLogIndex, len(state.Log)-1)
			return &raftpb.AppendEntriesResponse{
				Term:    int64(state.CurrentTerm),
				Success: false,
			}
		}
		// Check 2: Does the entry at prevLogIndex have the expected term?
		if state.Log[prevLogIndex].Term != prevLogTerm {
			log.Printf("[Node %d] AppendEntries rule 2 FAIL: term mismatch at prevLogIndex=%d (ours=%d, leader=%d) — rejecting",
				state.NodeID, prevLogIndex, state.Log[prevLogIndex].Term, prevLogTerm)
			return &raftpb.AppendEntriesResponse{
				Term:    int64(state.CurrentTerm),
				Success: false,
			}
		}
	}

	// ── Rules 3 & 4: Conflict truncation + idempotent append ────────────
	//
	// Rule 3: "If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that follow it."
	//
	// Rule 4: "Append any new entries not already in the log."
	//
	// We process these together in a single pass. For each incoming entry:
	//   - If our log already has an entry at that index with the SAME term,
	//     it's already correctly present (idempotent — skip it, rule 4).
	//   - If our log has an entry at that index with a DIFFERENT term,
	//     truncate at that point (rule 3), then append the rest.
	//   - If our log doesn't reach that index, just append (rule 4).
	if len(req.Entries) > 0 {
		for i, protoEntry := range req.Entries {
			entryIndex := int(protoEntry.Index)
			entryTerm := int(protoEntry.Term)

			if entryIndex < len(state.Log) {
				// We have an entry at this index — check for conflict.
				if state.Log[entryIndex].Term == entryTerm {
					// Same term — entry already present and correct. Skip it.
					// (Rule 4: "Append any new entries not already in the log")
					continue
				}
				// Different term — CONFLICT. Truncate here and everything after.
				// (Rule 3: "delete the existing entry and all that follow it")
				log.Printf("[Node %d] AppendEntries rule 3: conflict at index %d (ours term=%d, leader term=%d) — truncating log from %d entries to %d",
					state.NodeID, entryIndex, state.Log[entryIndex].Term, entryTerm, len(state.Log)-1, entryIndex-1)
				state.Log = state.Log[:entryIndex]
			}

			// Append this entry and all remaining entries from the request.
			// Once we've either found a conflict (truncated) or reached the
			// end of our log, ALL subsequent entries from the request are new
			// and need to be appended.
			for _, remaining := range req.Entries[i:] {
				state.Log = append(state.Log, LogEntry{
					Term:    int(remaining.Term),
					Index:   int(remaining.Index),
					Command: remaining.Command, // []byte from proto, stored as interface{}
				})
			}
			log.Printf("[Node %d] AppendEntries rules 3/4: appended %d entries (lastIndex now %d)",
				state.NodeID, len(req.Entries)-i, state.LastLogIndex())
			break // we've handled all entries from index i onward
		}
	}

	// ── Rule 5: "If leaderCommit > commitIndex, set commitIndex = ───────
	//     min(leaderCommit, index of last new entry)" (§5.3)
	//
	// This is how followers learn that entries are safe to apply. The leader
	// piggybacks its commitIndex on every AppendEntries RPC. If the leader
	// has committed further than we knew, we advance our own commitIndex.
	//
	// The min() with "index of last new entry" is a safety bound: we can't
	// commit entries we don't have yet. If the leader is ahead of us in the
	// log, we only commit up to what we've actually received.
	leaderCommit := int(req.LeaderCommit)
	if leaderCommit > state.CommitIndex {
		lastNewEntryIndex := state.LastLogIndex()
		newCommitIndex := leaderCommit
		if lastNewEntryIndex < newCommitIndex {
			newCommitIndex = lastNewEntryIndex
		}
		if newCommitIndex > state.CommitIndex {
			oldCommitIndex := state.CommitIndex
			state.CommitIndex = newCommitIndex
			log.Printf("[Node %d] AppendEntries rule 5: commitIndex %d → %d (leaderCommit=%d)",
				state.NodeID, oldCommitIndex, newCommitIndex, leaderCommit)

			// Signal the apply loop that new entries are ready to apply.
			// §5.3: "once a follower learns that a log entry is committed,
			// it applies the entry to its local state machine"
			notifyCommitIndexUpdated(state)
		}
	}

	return &raftpb.AppendEntriesResponse{
		Term:    int64(state.CurrentTerm),
		Success: true,
	}
}

