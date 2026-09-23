package raft

import (
	"encoding/json"
	"log"

	"raftkv/internal/kvstore"
)

// ─────────────────────────────────────────────────────────────────────────────
// Command Encoding — simple JSON format for KV operations
// ─────────────────────────────────────────────────────────────────────────────

// CommandPayload is the wire format for KV operations stored in log entries.
// ProposeCommand callers encode this to []byte using EncodeCommand(); the apply
// loop decodes it back using DecodeCommand() when executing committed entries.
//
// This is intentionally simple — JSON with plain string fields. It's not the
// final production format, just enough to prove the full propose → replicate →
// commit → apply pipeline works end-to-end. Phase 5 may evolve this if needed.
type CommandPayload struct {
	Op    string `json:"op"`              // "PUT" or "DELETE"
	Key   string `json:"key"`             // the key to operate on
	Value string `json:"value,omitempty"` // the value (only for PUT)
}

// EncodeCommand serializes a CommandPayload to JSON bytes suitable for storing
// in a LogEntry.Command field and sending over the wire via AppendEntries.
func EncodeCommand(cmd CommandPayload) ([]byte, error) {
	return json.Marshal(cmd)
}

// DecodeCommand deserializes JSON bytes back into a CommandPayload.
// Used by the apply loop to interpret committed log entries.
func DecodeCommand(data []byte) (CommandPayload, error) {
	var cmd CommandPayload
	err := json.Unmarshal(data, &cmd)
	return cmd, err
}

// ─────────────────────────────────────────────────────────────────────────────
// Apply Loop — executes committed entries against the KV store (§5.3)
// ─────────────────────────────────────────────────────────────────────────────

// RunApplyLoop is a background goroutine that watches for commitIndex advancing
// and applies newly committed log entries to the KV store (state machine).
//
// §5.3: "Once a follower learns that a log entry is committed, it applies the
// entry to its local state machine (in log order)."
//
// This runs on EVERY node (leader AND followers). The leader commits entries
// via advanceCommitIndex (raft.go); followers learn about commits via rule 5
// of HandleAppendEntries (election.go). Both signal CommitIndexUpdatedCh,
// which wakes this loop.
//
// Design choice: signal channel vs. polling.
// We use a dedicated signal channel (CommitIndexUpdatedCh) rather than a
// polling loop with time.Sleep. This is consistent with the codebase's
// existing pattern — every state-change notification uses a buffered(1)
// channel with non-blocking send (ResetElectionTimerCh, TriggerReplicationCh,
// BecameLeaderCh, StepDownCh). A polling loop would waste CPU when idle and
// add unnecessary latency (up to one sleep interval) when entries are ready.
func RunApplyLoop(state *State, store *kvstore.Store) {
	for {
		select {
		case <-state.CommitIndexUpdatedCh:
			// commitIndex advanced — apply any entries between lastApplied+1
			// and commitIndex.
			applyCommittedEntries(state, store)

		case <-state.StopCh:
			log.Printf("[Node %d] apply loop stopped (shutdown)", state.NodeID)
			return
		}
	}
}

// applyCommittedEntries reads any unapplied entries from the log, decodes
// and applies each one to the KV store, then advances lastApplied.
//
// The lock is held only briefly to snapshot the entries and indices — the
// actual KV store operations happen outside the Raft state lock. This is
// safe because:
//   1. The KV store has its own internal RWMutex.
//   2. Committed entries are never modified or removed from the log (they
//      may be overwritten only if uncommitted, which can't happen here).
//   3. lastApplied is only written by this goroutine, so there's no race.
func applyCommittedEntries(state *State, store *kvstore.Store) {
	state.Lock()
	commitIndex := state.CommitIndex
	lastApplied := state.LastApplied

	if lastApplied >= commitIndex {
		// Nothing new to apply.
		state.Unlock()
		return
	}

	// Snapshot the entries we need to apply. We copy them so we can release
	// the lock before doing I/O (KV store operations).
	entriesToApply := make([]LogEntry, 0, commitIndex-lastApplied)
	for i := lastApplied + 1; i <= commitIndex; i++ {
		if i < len(state.Log) {
			entriesToApply = append(entriesToApply, state.Log[i])
		}
	}
	nodeID := state.NodeID
	state.Unlock()

	// Apply each entry to the KV store outside the Raft lock.
	for _, entry := range entriesToApply {
		// The sentinel entry at index 0 has a nil Command — skip it.
		// Also skip entries with nil commands (e.g. no-op entries if we
		// add those later for leader confirmation).
		cmdBytes, ok := entry.Command.([]byte)
		if !ok || len(cmdBytes) == 0 {
			log.Printf("[Node %d] apply: skipping entry at index %d (no command)", nodeID, entry.Index)
			// Still advance lastApplied past this entry.
			state.Lock()
			if entry.Index > state.LastApplied {
				state.LastApplied = entry.Index
			}
			state.Unlock()
			continue
		}

		cmd, err := DecodeCommand(cmdBytes)
		if err != nil {
			log.Printf("[Node %d] apply: failed to decode command at index %d: %v", nodeID, entry.Index, err)
			// Advance past the bad entry to avoid getting stuck.
			state.Lock()
			if entry.Index > state.LastApplied {
				state.LastApplied = entry.Index
			}
			state.Unlock()
			continue
		}

		switch cmd.Op {
		case "PUT":
			store.Put(cmd.Key, cmd.Value)
			log.Printf("[Node %d] apply: PUT %q = %q (index=%d, term=%d)",
				nodeID, cmd.Key, cmd.Value, entry.Index, entry.Term)
		case "DELETE":
			err := store.Delete(cmd.Key)
			if err != nil {
				// Key didn't exist — not fatal, just log it. The delete is
				// still considered "applied" (idempotent delete semantics).
				log.Printf("[Node %d] apply: DELETE %q at index=%d: %v (proceeding)",
					nodeID, cmd.Key, entry.Index, err)
			} else {
				log.Printf("[Node %d] apply: DELETE %q (index=%d, term=%d)",
					nodeID, cmd.Key, entry.Index, entry.Term)
			}
		default:
			log.Printf("[Node %d] apply: unknown op %q at index=%d (skipping)", nodeID, cmd.Op, entry.Index)
		}

		// Advance lastApplied after each successful apply.
		state.Lock()
		if entry.Index > state.LastApplied {
			state.LastApplied = entry.Index
		}
		state.Unlock()
	}

	log.Printf("[Node %d] apply: applied entries %d through %d (lastApplied now %d)",
		nodeID, lastApplied+1, commitIndex, func() int {
			state.Lock()
			defer state.Unlock()
			return state.LastApplied
		}())
}

// notifyCommitIndexUpdated performs a non-blocking send on CommitIndexUpdatedCh
// to signal the apply loop that new entries may be ready to apply.
//
// CALLER MUST HOLD state.Lock().
func notifyCommitIndexUpdated(state *State) {
	select {
	case state.CommitIndexUpdatedCh <- struct{}{}:
	default:
		// Channel already has a pending signal; the apply loop will wake and
		// process all entries up to the current commitIndex regardless.
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Leader Commit Index Advancement — Figure 2, §5.3 / §5.4.2
// ─────────────────────────────────────────────────────────────────────────────

// advanceCommitIndex implements Figure 2's leader commit rule:
//
//	"If there exists an N such that N > commitIndex, a majority of
//	 matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N"
//
// §5.4.2 SAFETY RESTRICTION: The check log[N].term == currentTerm is critical.
// A leader must NOT directly commit entries from previous terms by counting
// replicas alone. It can only commit entries from its own term; earlier entries
// are committed indirectly when a current-term entry after them is committed
// (the Log Matching Property guarantees all preceding entries are also safe).
// See the pre-implementation explanation for the classic Figure 8 counterexample.
//
// This function scans from commitIndex+1 upward. For each candidate N, it
// counts how many peers have matchIndex ≥ N (including self, since the leader
// trivially matches its own log). If a majority matches AND the entry at N is
// from the current term, commitIndex advances to N.
//
// CALLER MUST HOLD state.Lock().
func advanceCommitIndex(state *State, leaderTerm int) {
	lastLogIndex := state.LastLogIndex()
	startN := state.CommitIndex + 1

	// Track the highest N that satisfies the commit condition.
	newCommitIndex := state.CommitIndex

	for n := startN; n <= lastLogIndex; n++ {
		// §5.4.2: Only commit entries from the leader's current term.
		if state.Log[n].Term != leaderTerm {
			continue
		}

		// Count replicas that have this entry. Leader always has it (count=1).
		replicaCount := 1
		for _, peer := range state.Peers {
			if state.MatchIndex[int(peer.ID)] >= n {
				replicaCount++
			}
		}

		// Majority = more than half the cluster (peers + self).
		clusterSize := len(state.Peers) + 1
		if replicaCount > clusterSize/2 {
			newCommitIndex = n
		}
	}

	if newCommitIndex > state.CommitIndex {
		oldCommitIndex := state.CommitIndex
		state.CommitIndex = newCommitIndex
		log.Printf("[Node %d] advanceCommitIndex: %d → %d (term=%d)",
			state.NodeID, oldCommitIndex, newCommitIndex, leaderTerm)

		// Signal the apply loop that new entries are ready to apply.
		notifyCommitIndexUpdated(state)
	}
}
