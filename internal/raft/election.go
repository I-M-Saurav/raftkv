package raft

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"time"

	"raftkv/internal/raftpb"
)

func randomElectionTimeout() time.Duration {
	ms := 150 + rand.IntN(151)
	return time.Duration(ms) * time.Millisecond
}

func RunElectionTimer(state *State, peerPool *PeerConnections) {
	timer := time.NewTimer(randomElectionTimeout())
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			go startElection(state, peerPool)
			timer.Reset(randomElectionTimeout())

		case <-state.ResetElectionTimerCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(randomElectionTimeout())

		case <-state.StopCh:
			log.Printf("[Node %d] election timer stopped (shutdown)", state.NodeID)
			return
		}
	}
}

func resetElectionTimer(state *State) {
	select {
	case state.ResetElectionTimerCh <- struct{}{}:
	default:
	}
}

func notifyStepDown(state *State) {
	select {
	case state.StepDownCh <- struct{}{}:
	default:
	}
}

func startElection(state *State, peerPool *PeerConnections) {
	state.Lock()

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
	state.LeaderID = -1 // presumed leader gone; unknown until a new one confirms itself

	// PERSIST: term incremented and vote cast — must be durable before we
	// ask peers to trust this candidacy.
	state.persistTermVote()

	lastLogIndex := state.LastLogIndex()
	lastLogTerm := state.LastLogTerm()
	nodeID := state.NodeID
	peers := state.Peers

	log.Printf("[Node %d] starting election for term %d (lastLogIndex=%d, lastLogTerm=%d)",
		nodeID, electionTerm, lastLogIndex, lastLogTerm)

	state.Unlock()

	resetElectionTimer(state)

	if len(peers) == 0 {
		state.Lock()
		if state.CurrentTerm == electionTerm && state.CurrentState == Candidate {
			log.Printf("[Node %d] won election for term %d (single-node cluster)", nodeID, electionTerm)
			becomeLeader(state)
		}
		state.Unlock()
		return
	}

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
			voteCh <- voteResult{peerID: peerID, term: resp.Term, voteGranted: resp.VoteGranted}
		}(peer.Address, peer.ID)
	}

	votesGranted := 1
	totalNodes := len(peers) + 1
	majority := totalNodes/2 + 1
	votesReceived := 0

	for votesReceived < len(peers) {
		result := <-voteCh
		votesReceived++

		if result.err != nil {
			log.Printf("[Node %d] RequestVote to peer %d failed: %v", nodeID, result.peerID, result.err)
			continue
		}

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
				state.persistTermVote() // PERSIST before signaling step-down
				if wasLeader {
					notifyStepDown(state)
				}
			}
			state.Unlock()
			return
		}

		if result.voteGranted {
			votesGranted++
			log.Printf("[Node %d] received vote from peer %d (%d/%d needed)",
				nodeID, result.peerID, votesGranted, majority)

			if votesGranted >= majority {
				state.Lock()
				if state.CurrentTerm == electionTerm && state.CurrentState == Candidate {
					log.Printf("[Node %d] won election for term %d (%d votes)",
						nodeID, electionTerm, votesGranted)
					becomeLeader(state)
				}
				state.Unlock()
				return
			}
		} else {
			log.Printf("[Node %d] vote denied by peer %d (term=%d)", nodeID, result.peerID, result.term)
		}
	}

	log.Printf("[Node %d] election for term %d failed (got %d/%d votes, needed %d)",
		nodeID, electionTerm, votesGranted, totalNodes, majority)
}

func becomeLeader(state *State) {
	state.CurrentState = Leader
	state.LeaderID = state.NodeID

	nextIdx := state.LastLogIndex() + 1
	for _, peer := range state.Peers {
		state.NextIndex[int(peer.ID)] = nextIdx
		state.MatchIndex[int(peer.ID)] = 0
	}

	// Append a no-op entry in our own current term immediately upon
	// becoming leader. This is a standard Raft implementation technique
	// (not explicitly in Figure 2, but required for liveness): per §5.4.2,
	// a leader can only directly commit entries from its OWN current term.
	// If earlier entries are replicated but not yet committed from this
	// leader's point of view (e.g. recovered from WAL after a restart, or
	// left behind by a prior leader that crashed before advancing
	// commitIndex), this leader cannot commit them on their own merit.
	// Committing a NEW entry in the current term causes advanceCommitIndex
	// to jump commitIndex directly to that entry's index — which safely
	// commits everything before it too, by the Log Matching Property.
	newIndex := state.LastLogIndex() + 1
	state.Log = append(state.Log, LogEntry{
		Term:    state.CurrentTerm,
		Index:   newIndex,
		Command: nil, // no-op: apply.go's applyCommittedEntries skips nil/empty commands
	})
	state.persistLog()

	// Trigger immediate replication rather than waiting up to 50ms for the
	// next heartbeat tick.
	select {
	case state.TriggerReplicationCh <- struct{}{}:
	default:
	}

	select {
	case state.BecameLeaderCh <- struct{}{}:
	default:
	}
}

func HandleRequestVote(state *State, req *raftpb.RequestVoteRequest) *raftpb.RequestVoteResponse {
	if int(req.Term) < state.CurrentTerm {
		log.Printf("[Node %d] rejecting RequestVote from candidate %d: stale term %d < %d (§5.1)",
			state.NodeID, req.CandidateId, req.Term, state.CurrentTerm)
		return &raftpb.RequestVoteResponse{Term: int64(state.CurrentTerm), VoteGranted: false}
	}

	if int(req.Term) > state.CurrentTerm {
		wasLeader := state.CurrentState == Leader
		log.Printf("[Node %d] RequestVote from candidate %d has higher term %d > %d — stepping down to Follower before evaluating (§5.1)",
			state.NodeID, req.CandidateId, req.Term, state.CurrentTerm)
		state.CurrentTerm = int(req.Term)
		state.VotedFor = -1
		state.LeaderID = -1
		state.CurrentState = Follower
		state.persistTermVote() // PERSIST: term/vote reset must be durable before proceeding
		if wasLeader {
			notifyStepDown(state)
		}
	}

	candidateID := int(req.CandidateId)
	if state.VotedFor != -1 && state.VotedFor != candidateID {
		log.Printf("[Node %d] rejecting RequestVote from candidate %d: already voted for %d in term %d (Figure 2 votedFor check)",
			state.NodeID, req.CandidateId, state.VotedFor, state.CurrentTerm)
		return &raftpb.RequestVoteResponse{Term: int64(state.CurrentTerm), VoteGranted: false}
	}

	ourLastLogTerm := state.LastLogTerm()
	ourLastLogIndex := state.LastLogIndex()
	candidateLastLogTerm := int(req.LastLogTerm)
	candidateLastLogIndex := int(req.LastLogIndex)

	candidateUpToDate := false
	if candidateLastLogTerm > ourLastLogTerm {
		candidateUpToDate = true
	} else if candidateLastLogTerm == ourLastLogTerm && candidateLastLogIndex >= ourLastLogIndex {
		candidateUpToDate = true
	}

	if !candidateUpToDate {
		log.Printf("[Node %d] rejecting RequestVote from candidate %d: log not up-to-date (candidate: term=%d idx=%d, ours: term=%d idx=%d) (§5.4.1)",
			state.NodeID, req.CandidateId, candidateLastLogTerm, candidateLastLogIndex, ourLastLogTerm, ourLastLogIndex)
		return &raftpb.RequestVoteResponse{Term: int64(state.CurrentTerm), VoteGranted: false}
	}

	state.VotedFor = candidateID
	log.Printf("[Node %d] granting vote to candidate %d for term %d (§5.2)", state.NodeID, req.CandidateId, state.CurrentTerm)

	// PERSIST: this vote grant is THE promise Figure 2 requires be durable
	// before we tell the candidate "yes" — must happen before returning.
	state.persistTermVote()

	resetElectionTimer(state)

	return &raftpb.RequestVoteResponse{Term: int64(state.CurrentTerm), VoteGranted: true}
}

func HandleAppendEntries(state *State, req *raftpb.AppendEntriesRequest) *raftpb.AppendEntriesResponse {
	if int(req.Term) < state.CurrentTerm {
		log.Printf("[Node %d] rejecting AppendEntries from leader %d: stale term %d < %d (§5.1)",
			state.NodeID, req.LeaderId, req.Term, state.CurrentTerm)
		return &raftpb.AppendEntriesResponse{Term: int64(state.CurrentTerm), Success: false}
	}

	if int(req.Term) > state.CurrentTerm {
		wasLeader := state.CurrentState == Leader
		log.Printf("[Node %d] AppendEntries from leader %d has higher term %d > %d — updating term, stepping down (§5.1)",
			state.NodeID, req.LeaderId, req.Term, state.CurrentTerm)
		state.CurrentTerm = int(req.Term)
		state.VotedFor = -1
		state.LeaderID = -1
		state.CurrentState = Follower
		state.persistTermVote() // PERSIST before continuing / responding
		if wasLeader {
			notifyStepDown(state)
		}
	} else if state.CurrentState == Candidate {
		log.Printf("[Node %d] AppendEntries from leader %d in same term %d — stepping down from Candidate to Follower (§5.2)",
			state.NodeID, req.LeaderId, req.Term)
		state.CurrentState = Follower
	}

	// Record the current leader for HTTP-layer forwarding.
	state.LeaderID = int(req.LeaderId)

	resetElectionTimer(state)

	prevLogIndex := int(req.PrevLogIndex)
	prevLogTerm := int(req.PrevLogTerm)

	if prevLogIndex > 0 {
		if prevLogIndex >= len(state.Log) {
			log.Printf("[Node %d] AppendEntries rule 2 FAIL: log too short (prevLogIndex=%d, logLen=%d) — rejecting",
				state.NodeID, prevLogIndex, len(state.Log)-1)
			return &raftpb.AppendEntriesResponse{Term: int64(state.CurrentTerm), Success: false}
		}
		if state.Log[prevLogIndex].Term != prevLogTerm {
			log.Printf("[Node %d] AppendEntries rule 2 FAIL: term mismatch at prevLogIndex=%d (ours=%d, leader=%d) — rejecting",
				state.NodeID, prevLogIndex, state.Log[prevLogIndex].Term, prevLogTerm)
			return &raftpb.AppendEntriesResponse{Term: int64(state.CurrentTerm), Success: false}
		}
	}

	if len(req.Entries) > 0 {
		for i, protoEntry := range req.Entries {
			entryIndex := int(protoEntry.Index)
			entryTerm := int(protoEntry.Term)

			if entryIndex < len(state.Log) {
				if state.Log[entryIndex].Term == entryTerm {
					continue
				}
				log.Printf("[Node %d] AppendEntries rule 3: conflict at index %d (ours term=%d, leader term=%d) — truncating log from %d entries to %d",
					state.NodeID, entryIndex, state.Log[entryIndex].Term, entryTerm, len(state.Log)-1, entryIndex-1)
				state.Log = state.Log[:entryIndex]
			}

			for _, remaining := range req.Entries[i:] {
				state.Log = append(state.Log, LogEntry{
					Term:    int(remaining.Term),
					Index:   int(remaining.Index),
					Command: remaining.Command,
				})
			}
			log.Printf("[Node %d] AppendEntries rules 3/4: appended %d entries (lastIndex now %d)",
				state.NodeID, len(req.Entries)-i, state.LastLogIndex())
			break
		}

		// PERSIST: the log just changed (appended and/or truncated) — must
		// be durable before we let commitIndex/apply proceed below.
		state.persistLog()
	}

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
			notifyCommitIndexUpdated(state)
		}
	}

	return &raftpb.AppendEntriesResponse{Term: int64(state.CurrentTerm), Success: true}
}