package raft

import (
	"log"
)

// ─────────────────────────────────────────────────────────────────────────────
// Log Replication — Leader-side command proposal (§5.3)
// ─────────────────────────────────────────────────────────────────────────────

// ProposeCommand appends a new entry to the leader's log and triggers
// immediate replication to followers. This is the entry point that the HTTP
// layer (Phase 5) will call when a client sends a write request.
//
// Behavior:
//   - If this node is NOT the leader, returns isLeader=false immediately.
//     The caller is responsible for forwarding the request to the actual
//     leader (Phase 5 concern, not implemented here).
//   - If this node IS the leader: appends a new LogEntry with the current
//     term and the next sequential index, then returns the new entry's
//     index/term and isLeader=true.
//
// This function does NOT block waiting for the entry to be replicated to a
// majority of followers. Replication happens asynchronously via the leader's
// heartbeat/replication loop in sendHeartbeats() (raft.go). The caller learns
// about commitment separately (Phase 5).
//
// §5.3: "The leader appends the command to its log as a new entry, then
// issues AppendEntries RPCs in parallel to each of the other servers to
// replicate the entry."
//
// The "issues AppendEntries RPCs" part is handled by firing
// TriggerReplicationCh, which wakes the heartbeat loop immediately rather
// than waiting up to 50ms for the next periodic tick.
//
// Command encoding: the `command` parameter is an opaque []byte. Callers
// should use EncodeCommand(CommandPayload{...}) from apply.go to produce
// these bytes. The apply loop uses DecodeCommand() to interpret them when
// executing committed entries against the KV store.
func ProposeCommand(state *State, command []byte) (index int, term int, isLeader bool) {
	state.Lock()
	defer state.Unlock()

	// Only the leader accepts new log entries.
	// §5.1: "If a client contacts a follower, the follower redirects it
	// to the leader."
	if state.CurrentState != Leader {
		return 0, 0, false
	}

	// Create and append the new log entry.
	// §5.3: "Each log entry stores a state machine command along with the
	// term number when the entry was received by the leader."
	newIndex := state.LastLogIndex() + 1
	newEntry := LogEntry{
		Term:    state.CurrentTerm,
		Index:   newIndex,
		Command: command,
	}
	state.Log = append(state.Log, newEntry)

	log.Printf("[Node %d] ProposeCommand: appended entry at index %d, term %d (logLen=%d)",
		state.NodeID, newIndex, state.CurrentTerm, len(state.Log)-1)

	// Trigger immediate replication. Non-blocking send: if the channel
	// already has a pending signal, the loop will pick it up and send all
	// accumulated entries in one batch — no signal is lost.
	select {
	case state.TriggerReplicationCh <- struct{}{}:
	default:
	}

	return newIndex, state.CurrentTerm, true
}
