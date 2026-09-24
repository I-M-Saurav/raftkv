package raft

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// ─────────────────────────────────────────────────────────────────────────────
// Storage — Write-Ahead persistence for Raft's persistent state (Figure 2)
// ─────────────────────────────────────────────────────────────────────────────
//
// Figure 2 requires exactly three fields to survive a crash/restart:
// currentTerm, votedFor, and log[]. Everything else is volatile and safely
// recomputed after restart (a restarted node just gets caught up again by
// the current leader).
//
// Persistence rule (project spec): "Persist currentTerm and votedFor before
// responding to any RPC." A node that grants a vote (or updates its term)
// and crashes BEFORE writing that to disk could, on restart, contradict a
// promise it already made over the network — e.g. voting twice in the same
// term for different candidates, exactly what Raft's safety depends on never
// happening.
//
// Two files per node:
//   - meta.json : currentTerm + votedFor. Tiny, rewritten atomically on every
//     change (temp file, fsync, atomic rename) so a crash mid-write never
//     leaves a corrupt file.
//   - log.json  : the full replicated log (excluding the index-0 dummy
//     sentinel), rewritten atomically the same way.
//
// TRADE-OFF (flagged honestly): rewriting the entire log file on every change
// is O(n), not a true append-only O(1) WAL. For a project at this scale, this
// is a reasonable simplification — correct, with far less code than a real
// append/compaction/segment-file scheme. A production system (e.g. etcd)
// uses genuine append-only segments plus periodic snapshotting to bound both
// file size and rewrite cost — the natural next optimization here.
type Storage struct {
	dir      string
	metaPath string
	logPath  string
}

type persistedMeta struct {
	CurrentTerm int `json:"current_term"`
	VotedFor    int `json:"voted_for"`
}

// persistedLogEntry is the on-disk shape of one log entry. Command is stored
// as []byte (JSON auto-base64-encodes []byte) — on disk we know it's always
// concretely []byte, unlike the in-memory LogEntry.Command interface{}.
type persistedLogEntry struct {
	Term    int    `json:"term"`
	Index   int    `json:"index"`
	Command []byte `json:"command"`
}

// NewStorage creates (if needed) the data directory for one node and returns
// a Storage handle for it. dataDir is the parent dir (e.g. "data"); this
// node's files live under dataDir/nodeN/.
func NewStorage(dataDir string, nodeID int) (*Storage, error) {
	dir := filepath.Join(dataDir, fmt.Sprintf("node%d", nodeID))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("storage: failed to create data dir %s: %w", dir, err)
	}
	return &Storage{
		dir:      dir,
		metaPath: filepath.Join(dir, "meta.json"),
		logPath:  filepath.Join(dir, "log.json"),
	}, nil
}

// LoadState reads whatever persisted state exists and reconstructs
// (currentTerm, votedFor, log). A brand-new node with no prior WAL files
// returns Figure 2 defaults (term=0, votedFor=-1, empty log) with no error —
// a missing file just means a fresh start, not a failure.
//
// The returned log does NOT include the index-0 dummy sentinel — the caller
// (main.go) prepends it when constructing State.Log.
func (s *Storage) LoadState() (currentTerm int, votedFor int, entries []LogEntry, err error) {
	currentTerm = 0
	votedFor = -1

	if data, readErr := os.ReadFile(s.metaPath); readErr == nil {
		var meta persistedMeta
		if jsonErr := json.Unmarshal(data, &meta); jsonErr != nil {
			return 0, 0, nil, fmt.Errorf("storage: corrupt meta file %s: %w", s.metaPath, jsonErr)
		}
		currentTerm = meta.CurrentTerm
		votedFor = meta.VotedFor
	} else if !os.IsNotExist(readErr) {
		return 0, 0, nil, fmt.Errorf("storage: failed to read meta file: %w", readErr)
	}

	if data, readErr := os.ReadFile(s.logPath); readErr == nil {
		var persisted []persistedLogEntry
		if jsonErr := json.Unmarshal(data, &persisted); jsonErr != nil {
			return 0, 0, nil, fmt.Errorf("storage: corrupt log file %s: %w", s.logPath, jsonErr)
		}
		entries = make([]LogEntry, len(persisted))
		for i, pe := range persisted {
			entries[i] = LogEntry{Term: pe.Term, Index: pe.Index, Command: pe.Command}
		}
	} else if !os.IsNotExist(readErr) {
		return 0, 0, nil, fmt.Errorf("storage: failed to read log file: %w", readErr)
	}

	return currentTerm, votedFor, entries, nil
}

// PersistTermVote atomically overwrites meta.json. Called whenever
// CurrentTerm or VotedFor changes, while state.Lock() is held, BEFORE
// responding to the RPC that caused the change.
func (s *Storage) PersistTermVote(currentTerm, votedFor int) error {
	return atomicWriteJSON(s.metaPath, persistedMeta{CurrentTerm: currentTerm, VotedFor: votedFor})
}

// PersistLog atomically overwrites log.json with the given entries (should
// be state.Log[1:] — excluding the dummy sentinel).
func (s *Storage) PersistLog(entries []LogEntry) error {
	persisted := make([]persistedLogEntry, len(entries))
	for i, e := range entries {
		cmdBytes, _ := e.Command.([]byte) // always []byte in practice
		persisted[i] = persistedLogEntry{Term: e.Term, Index: e.Index, Command: cmdBytes}
	}
	return atomicWriteJSON(s.logPath, persisted)
}

// atomicWriteJSON writes v as JSON to a temp file, fsyncs it, then atomically
// renames it over path. A crash mid-write never leaves a corrupt file —
// readers only ever see the fully-old or fully-new version.
func atomicWriteJSON(path string, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("storage: failed to marshal: %w", err)
	}

	tmpPath := path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("storage: failed to create temp file %s: %w", tmpPath, err)
	}

	w := bufio.NewWriter(f)
	if _, err := w.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("storage: failed to write temp file: %w", err)
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return fmt.Errorf("storage: failed to flush temp file: %w", err)
	}
	// fsync: force the OS to actually write to physical storage, not just
	// its in-memory page cache. Without this, a power loss (not just a
	// process crash) could still lose "persisted" data.
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("storage: failed to fsync temp file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("storage: failed to close temp file: %w", err)
	}

	// Atomic rename: Go's os.Rename replaces the destination atomically on
	// both Windows and POSIX — readers never see a half-written file.
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("storage: failed to atomically rename %s -> %s: %w", tmpPath, path, err)
	}

	return nil
}

// mustPersist logs a fatal error and exits if a persistence write fails.
// Treated as fatal rather than silently ignored: continuing after failing to
// make a promised state change durable (e.g. a granted vote) would risk
// violating Raft's safety guarantees on the next crash. Better to stop the
// node than run with a broken promise.
func mustPersist(nodeID int, what string, err error) {
	if err != nil {
		log.Fatalf("[Node %d] FATAL: failed to persist %s: %v", nodeID, what, err)
	}
}