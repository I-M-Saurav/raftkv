package raft

import "testing"

// TestNewState_InitialValues checks that a freshly created node starts in
// the state §5.1 mandates: term 0, no vote cast, Follower role.
func TestNewState_InitialValues(t *testing.T) {
	s := NewState(1)

	if s.CurrentTerm != 0 {
		t.Errorf("expected CurrentTerm 0, got %d", s.CurrentTerm)
	}
	if s.VotedFor != -1 {
		t.Errorf("expected VotedFor -1 (no vote), got %d", s.VotedFor)
	}
	if s.CurrentState != Follower {
		t.Errorf("expected initial state Follower, got %s", s.CurrentState)
	}
}

// TestLastLogIndex_EmptyLog checks the dummy-entry trick: a log with no real
// entries yet should report LastLogIndex 0, not -1 or a panic.
func TestLastLogIndex_EmptyLog(t *testing.T) {
	s := NewState(1)

	if got := s.LastLogIndex(); got != 0 {
		t.Errorf("expected LastLogIndex 0 for empty log, got %d", got)
	}
	if got := s.LastLogTerm(); got != 0 {
		t.Errorf("expected LastLogTerm 0 for empty log, got %d", got)
	}
}

// TestLastLogIndex_WithEntries checks that after appending real entries,
// the helpers correctly report the position/term of the last one — using
// Raft's 1-indexed convention (first real entry lands at index 1).
func TestLastLogIndex_WithEntries(t *testing.T) {
	s := NewState(1)
	s.Log = append(s.Log, LogEntry{Term: 1, Index: 1, Command: "PUT x=1"})
	s.Log = append(s.Log, LogEntry{Term: 2, Index: 2, Command: "PUT y=2"})

	if got := s.LastLogIndex(); got != 2 {
		t.Errorf("expected LastLogIndex 2, got %d", got)
	}
	if got := s.LastLogTerm(); got != 2 {
		t.Errorf("expected LastLogTerm 2, got %d", got)
	}
}