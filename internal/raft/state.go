package raft

// NodeState represents which of the 3 roles (§5.1) this node currently plays.
type NodeState int

const (
	Follower NodeState = iota // default state; only responds to RPCs, never initiates
	Candidate                 // trying to become leader; requesting votes
	Leader                    // accepts client writes, replicates log to followers
)

// String makes NodeState print nicely in logs (e.g. "Follower" instead of "0").
func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry is a single entry in the replicated log (§5.3).
// Command is opaque to Raft itself — Raft doesn't know or care that it's a
// KV Put/Delete. It just replicates bytes/commands in order.
type LogEntry struct {
	Term    int         // term when this entry was received by the leader
	Index   int         // position of this entry in the log (1-indexed per the paper)
	Command interface{} // the actual operation, e.g. a KV Put/Delete request
}

// State holds all the Raft-specific state for a single node, matching
// Figure 2 of the paper exactly. This is separate from the KV store —
// Raft doesn't know what's being replicated, it just manages consensus.
type State struct {
	// --- Persistent state (must survive crash — written to disk in Phase 6) ---
	CurrentTerm int        // latest term this node has seen (starts at 0, increases monotonically)
	VotedFor    int        // candidateId voted for in CurrentTerm, or -1 if none
	Log         []LogEntry // the replicated log; index 0 is unused (log is 1-indexed per paper convention)

	// --- Volatile state (all nodes, reset on restart, doesn't need persistence) ---
	CommitIndex int // highest log index known to be committed
	LastApplied int // highest log index applied to the local state machine

	// --- Volatile state (leader only, reinitialized after every election) ---
	NextIndex  map[int]int // for each follower: next log index to send them
	MatchIndex map[int]int // for each follower: highest log index known replicated

	// --- Local bookkeeping (not part of Figure 2 directly, but needed) ---
	CurrentState NodeState // Follower / Candidate / Leader
	NodeID       int       // this node's own ID
}

// NewState creates a fresh Raft state for a node starting up for the first time.
// Note: Log[0] is a dummy/sentinel entry so that real entries start at index 1,
// matching the paper's 1-indexed convention and avoiding off-by-one bugs later.
func NewState(nodeID int) *State {
	return &State{
		CurrentTerm:  0,
		VotedFor:     -1, // -1 means "haven't voted this term"
		Log:          make([]LogEntry, 1), // index 0 = dummy entry
		CommitIndex:  0,
		LastApplied:  0,
		NextIndex:    make(map[int]int),
		MatchIndex:   make(map[int]int),
		CurrentState: Follower, // §5.1: all servers start as followers
		NodeID:       nodeID,
	}
}
