package raft

import (
	"sync"

	"raftkv/internal/config"
)

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

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

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// State holds all Raft-specific state for a single node (Figure 2).
type State struct {
	mu sync.Mutex

	CurrentTerm int
	VotedFor    int
	Log         []LogEntry

	CommitIndex int
	LastApplied int

	NextIndex  map[int]int
	MatchIndex map[int]int

	CurrentState NodeState
	NodeID       int
	LeaderID     int // NodeID this follower believes is leader; -1 = unknown

	ResetElectionTimerCh chan struct{}
	StopCh               chan struct{}
	BecameLeaderCh       chan struct{}
	StepDownCh           chan struct{}
	TriggerReplicationCh chan struct{}
	CommitIndexUpdatedCh chan struct{}

	Peers []config.NodeConfig

	// Storage is the WAL handle for this node's persistent state
	// (currentTerm, votedFor, log). May be nil in unit tests that don't
	// exercise persistence — persistTermVote()/persistLog() no-op if so.
	// Production nodes must always set this via main.go after NewState.
	Storage *Storage
}

func (s *State) Lock()   { s.mu.Lock() }
func (s *State) Unlock() { s.mu.Unlock() }

func (s *State) LastLogIndex() int {
	return len(s.Log) - 1
}

func (s *State) LastLogTerm() int {
	return s.Log[s.LastLogIndex()].Term
}

// persistTermVote writes CurrentTerm/VotedFor to disk. CALLER MUST HOLD
// state.Lock(). No-op if Storage is nil (tests). Fatal on write failure —
// see mustPersist's reasoning in storage.go.
func (s *State) persistTermVote() {
	if s.Storage == nil {
		return
	}
	mustPersist(s.NodeID, "term/vote", s.Storage.PersistTermVote(s.CurrentTerm, s.VotedFor))
}

// persistLog writes the current log (excluding the dummy sentinel) to disk.
// CALLER MUST HOLD state.Lock(). No-op if Storage is nil (tests).
func (s *State) persistLog() {
	if s.Storage == nil {
		return
	}
	mustPersist(s.NodeID, "log", s.Storage.PersistLog(s.Log[1:]))
}

func NewState(nodeID int, peers []config.NodeConfig) *State {
	return &State{
		CurrentTerm:          0,
		VotedFor:             -1,
		Log:                  make([]LogEntry, 1),
		CommitIndex:          0,
		LastApplied:          0,
		NextIndex:            make(map[int]int),
		MatchIndex:           make(map[int]int),
		CurrentState:         Follower,
		NodeID:               nodeID,
		LeaderID:             -1,
		ResetElectionTimerCh: make(chan struct{}, 1),
		StopCh:               make(chan struct{}),
		BecameLeaderCh:       make(chan struct{}, 1),
		StepDownCh:           make(chan struct{}, 1),
		TriggerReplicationCh: make(chan struct{}, 1),
		CommitIndexUpdatedCh: make(chan struct{}, 1),
		Peers:                peers,
	}
}