package raft

import (
	"sync"

	"raftkv/internal/config"
)

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
//
// State embeds its own mutex (mu) — this is the single source-of-truth lock
// for ALL concurrent access to Raft state, whether from rpc.go's gRPC
// handlers, election.go's timer/heartbeat goroutines, or any future callers.
// Callers use the exported Lock()/Unlock() methods; the mu field itself is
// unexported to prevent external packages from bypassing the API.
type State struct {
	mu sync.Mutex // protects all fields below; access via Lock()/Unlock()

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

	// --- Election timer plumbing (used by election.go's RunElectionTimer) ---
	// ResetElectionTimerCh signals the election timer goroutine to restart with
	// a fresh random timeout. Sent on (non-blocking) when: a valid heartbeat
	// arrives, this node grants a vote, or this node starts a new election.
	ResetElectionTimerCh chan struct{}
	// StopCh is closed once at shutdown to terminate the election timer goroutine
	// and the heartbeat loop.
	StopCh chan struct{}

	// --- Leadership lifecycle channels (used by raft.go's supervisor) ---
	// BecameLeaderCh is signalled (non-blocking) by becomeLeader() after this
	// node wins an election. The supervisor in raft.go selects on this to
	// spawn the heartbeat-sending goroutine.
	BecameLeaderCh chan struct{}
	// StepDownCh is signalled (non-blocking) when this node transitions FROM
	// Leader TO Follower (higher term discovered). The supervisor selects on
	// this to stop the heartbeat goroutine. Only sent when the node WAS Leader
	// before the transition — Candidate/Follower step-downs do not signal here.
	StepDownCh chan struct{}

	// --- Cluster membership ---
	// Peers holds the addresses of all OTHER nodes in the cluster (excluding self).
	// Used by startElection() to fan out RequestVote RPCs.
	Peers []config.NodeConfig
}

// Lock acquires the State mutex. Every goroutine that reads or writes Raft
// state (gRPC handlers, election timer, heartbeat loop, etc.) must hold this
// lock. Exposed as a method so the unexported mu field stays encapsulated.
func (s *State) Lock()   { s.mu.Lock() }

// Unlock releases the State mutex.
func (s *State) Unlock() { s.mu.Unlock() }

// LastLogIndex returns the index of the last entry currently in the log.
// Because Log[0] is a dummy sentinel, an "empty" log (no real entries yet)
// correctly returns 0 here — which is exactly what §5.4.1's comparison expects.
func (s *State) LastLogIndex() int {
	return len(s.Log) - 1
}

// LastLogTerm returns the term of the last entry currently in the log.
// Used alongside LastLogIndex in the §5.4.1 "up-to-date" check during voting.
func (s *State) LastLogTerm() int {
	return s.Log[s.LastLogIndex()].Term
}

// NewState creates a fresh Raft state for a node starting up for the first time.
// Note: Log[0] is a dummy/sentinel entry so that real entries start at index 1,
// matching the paper's 1-indexed convention and avoiding off-by-one bugs later.
// peers should contain all cluster nodes EXCEPT this node (use config.PeersExcluding).
func NewState(nodeID int, peers []config.NodeConfig) *State {
	return &State{
		CurrentTerm:          0,
		VotedFor:             -1, // -1 means "haven't voted this term"
		Log:                  make([]LogEntry, 1), // index 0 = dummy entry
		CommitIndex:          0,
		LastApplied:          0,
		NextIndex:            make(map[int]int),
		MatchIndex:           make(map[int]int),
		CurrentState:         Follower, // §5.1: all servers start as followers
		NodeID:               nodeID,
		ResetElectionTimerCh: make(chan struct{}, 1), // buffered(1) so non-blocking send never drops if timer goroutine is busy
		StopCh:               make(chan struct{}),
		BecameLeaderCh:       make(chan struct{}, 1),
		StepDownCh:           make(chan struct{}, 1),
		Peers:                peers,
	}
}