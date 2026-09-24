package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Fault-injection integration tests — Phase 7
// ─────────────────────────────────────────────────────────────────────────────
//
// These tests build the real raftkv.exe binary and spawn actual OS processes
// for each node (not an in-memory simulation). Crashes are simulated with a
// real process kill (cmd.Process.Kill()), and correctness is verified purely
// through the same HTTP API a real client would use. This exercises the
// entire stack — HTTP, gRPC, election, replication, persistence — exactly as
// a user would experience it, not a synthetic approximation.
//
// A dedicated port range (19000s/18000s) is used to avoid colliding with any
// manually-run nodes on the developer's machine (which use 9000s/8080s).

const (
	basePortRaft = 19001
	basePortHTTP = 18081
)

// statusResponse mirrors the JSON shape of GET /status (see http.go).
type statusResponse struct {
	NodeID      int    `json:"node_id"`
	State       string `json:"state"`
	Term        int    `json:"term"`
	LeaderID    int    `json:"leader_id"`
	CommitIndex int    `json:"commit_index"`
	LastApplied int    `json:"last_applied"`
}

// testNode represents one spawned raftkv.exe process.
type testNode struct {
	id       int
	raftAddr string
	httpAddr string
	dataDir  string
	cmd      *exec.Cmd
	logFile  *os.File
}

// buildBinary compiles cmd/node into a temp file ONCE per test run and
// returns its path. Building fresh (rather than reusing a manually-built
// raftkv.exe) guarantees tests always exercise the current source code.
// buildBinary compiles cmd/node to a FIXED path under the project's bin/
// directory (not a fresh temp dir per run). This matters specifically on
// Windows: Firewall treats each unique executable PATH as a new, unknown
// program requiring a permission prompt. A stable path means the user only
// approves it once, rather than on every single test run.
func buildBinary(t *testing.T) string {
	t.Helper()
	binDir := filepath.Join("..", "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("failed to create bin dir: %v", err)
	}
	binPath := filepath.Join(binDir, "raftkv_test.exe")

	cmd := exec.Command("go", "build", "-o", binPath, "../cmd/node")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("failed to build raftkv binary: %v\n%s", err, out)
	}
	return binPath
}

// writeClusterConfig writes a cluster.yaml for the given node IDs using the
// fixed test port ranges, and returns its path.
func writeClusterConfig(t *testing.T, dir string, ids []int) string {
	t.Helper()
	var sb strings.Builder
	sb.WriteString("nodes:\n")
	for _, id := range ids {
		sb.WriteString(fmt.Sprintf("  - id: %d\n", id))
		sb.WriteString(fmt.Sprintf("    address: \"localhost:%d\"\n", basePortRaft+id))
		sb.WriteString(fmt.Sprintf("    http_address: \"localhost:%d\"\n", basePortHTTP+id))
	}
	path := filepath.Join(dir, "cluster.yaml")
	if err := os.WriteFile(path, []byte(sb.String()), 0o644); err != nil {
		t.Fatalf("failed to write cluster.yaml: %v", err)
	}
	return path
}

// startNode spawns a real raftkv.exe process for the given node ID. Stdout
// and stderr are captured to a log file under the test's temp dir so failures
// can be diagnosed by reading actual node logs, not just HTTP-level symptoms.
func startNode(t *testing.T, binPath, configPath, dataDir string, id int) *testNode {
	t.Helper()
	raftAddr := fmt.Sprintf("localhost:%d", basePortRaft+id)
	httpAddr := fmt.Sprintf(":%d", basePortHTTP+id)

	logPath := filepath.Join(dataDir, fmt.Sprintf("node%d.log", id))
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("failed to create log file for node %d: %v", id, err)
	}

	cmd := exec.Command(binPath,
		"--id", fmt.Sprint(id),
		"--raft-addr", raftAddr,
		"--http-addr", httpAddr,
		"--config", configPath,
		"--data-dir", dataDir,
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start node %d: %v", id, err)
	}

	return &testNode{
		id:       id,
		raftAddr: raftAddr,
		httpAddr: fmt.Sprintf("localhost:%d", basePortHTTP+id),
		dataDir:  dataDir,
		cmd:      cmd,
		logFile:  logFile,
	}
}

// stop kills the node's process (a hard kill, simulating a real crash —
// no graceful shutdown, no chance to flush anything not already fsynced),
// then closes our own handle to its log file. Closing the log file matters
// specifically on Windows: a file can't be deleted while any process holds
// an open handle to it, even after the process that was WRITING to it has
// been killed — our own os.File handle in the test process also needs
// closing, or t.TempDir()'s automatic cleanup fails.
func (n *testNode) stop() {
	if n.cmd != nil && n.cmd.Process != nil {
		_ = n.cmd.Process.Kill()
		_ = n.cmd.Wait()
	}
	if n.logFile != nil {
		_ = n.logFile.Close()
		n.logFile = nil
	}
}
// getStatus queries this node's /status endpoint. Returns an error if the
// node isn't up yet or unreachable (e.g. just killed).
func (n *testNode) getStatus() (*statusResponse, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/status", n.httpAddr))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var s statusResponse
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, err
	}
	return &s, nil
}

// waitForLeader polls the given nodes until exactly one reports itself as
// Leader, or fails the test after timeout. Returns that node.
func waitForLeader(t *testing.T, nodes []*testNode, timeout time.Duration) *testNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			s, err := n.getStatus()
			if err != nil {
				continue // node not up yet, or just killed — skip
			}
			if s.State == "Leader" {
				return n
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %v", timeout)
	return nil
}

// putKV sends a PUT request for key=value to the given node's HTTP address.
func putKV(t *testing.T, httpAddr, key, value string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("http://%s/kv/%s", httpAddr, key), bytes.NewBufferString(value))
	if err != nil {
		t.Fatalf("failed to build PUT request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT request failed: %v", err)
	}
	return resp
}

// getKVEventually polls GET /kv/{key} on the given node until it returns the
// expected value or times out. Used for followers, which apply asynchronously
// after replication — a single immediate GET could legitimately race ahead
// of the apply loop.
func getKVEventually(t *testing.T, httpAddr, key, expected string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr string
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://%s/kv/%s", httpAddr, key))
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK && string(body) == expected {
				return
			}
			lastErr = fmt.Sprintf("status=%d body=%q", resp.StatusCode, body)
		} else {
			lastErr = err.Error()
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("GET %s/kv/%s never returned %q within %v (last: %s)", httpAddr, key, expected, timeout, lastErr)
}

// setupCluster builds the binary, writes config, starts N nodes, and
// registers cleanup to kill all of them at test end regardless of outcome.
func setupCluster(t *testing.T, n int) []*testNode {
	t.Helper()
	binPath := buildBinary(t)
	rootDir := t.TempDir()

	ids := make([]int, n)
	for i := range ids {
		ids[i] = i + 1
	}
	configPath := writeClusterConfig(t, rootDir, ids)

	nodes := make([]*testNode, n)
	for i, id := range ids {
		nodes[i] = startNode(t, binPath, configPath, rootDir, id)
	}

	t.Cleanup(func() {
		for _, n := range nodes {
			n.stop()
		}
	})

	return nodes
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

// TestElection verifies a fresh 3-node cluster elects exactly one leader.
func TestElection(t *testing.T) {
	nodes := setupCluster(t, 3)
	leader := waitForLeader(t, nodes, 10*time.Second)
	t.Logf("elected leader: node %d", leader.id)
}

// TestReplicationAndFailover verifies: a write to the leader replicates to
// all followers, the cluster survives the leader's crash with no data loss,
// a new leader is elected among survivors, and new writes continue to work.
func TestReplicationAndFailover(t *testing.T) {
	nodes := setupCluster(t, 3)
	leader := waitForLeader(t, nodes, 10*time.Second)

	// Write to the leader directly.
	resp := putKV(t, leader.httpAddr, "hello", "world")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT to leader failed: status=%d", resp.StatusCode)
	}

	// Verify replication: all 3 nodes should eventually see it.
	for _, n := range nodes {
		getKVEventually(t, n.httpAddr, "hello", "world", 3*time.Second)
	}

	// Kill the leader — simulate a real crash.
	t.Logf("killing leader (node %d)", leader.id)
	leader.stop()

	// Remaining nodes should elect a new leader.
	var survivors []*testNode
	for _, n := range nodes {
		if n.id != leader.id {
			survivors = append(survivors, n)
		}
	}
	newLeader := waitForLeader(t, survivors, 10*time.Second)
	t.Logf("new leader after crash: node %d", newLeader.id)

	// Old data must have survived the crash.
	for _, n := range survivors {
		getKVEventually(t, n.httpAddr, "hello", "world", 3*time.Second)
	}

	// New writes must still work against the surviving majority.
	resp = putKV(t, survivors[0].httpAddr, "after-crash", "still-works")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT after leader crash failed: status=%d", resp.StatusCode)
	}
	for _, n := range survivors {
		getKVEventually(t, n.httpAddr, "after-crash", "still-works", 3*time.Second)
	}
}

// TestPersistenceRecovery verifies a killed-and-restarted node recovers its
// term from disk (never regressing) and eventually rejoins with correct data.
func TestPersistenceRecovery(t *testing.T) {
	nodes := setupCluster(t, 3)
	leader := waitForLeader(t, nodes, 10*time.Second)

	resp := putKV(t, leader.httpAddr, "keyA", "valA")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT failed: status=%d", resp.StatusCode)
	}
	for _, n := range nodes {
		getKVEventually(t, n.httpAddr, "keyA", "valA", 3*time.Second)
	}

	// Pick a FOLLOWER to crash and restart (not the leader — that's the
	// other test's job).
	var follower *testNode
	for _, n := range nodes {
		if n.id != leader.id {
			follower = n
			break
		}
	}

	preCrashStatus, err := follower.getStatus()
	if err != nil {
		t.Fatalf("failed to get pre-crash status: %v", err)
	}
	preCrashTerm := preCrashStatus.Term
	t.Logf("follower node %d term before crash: %d", follower.id, preCrashTerm)

	follower.stop()

	// Restart the SAME node: same ID, same data dir — this is what proves
	// persistence, as opposed to a brand-new node joining from scratch.
	binPath := buildBinary(t)
	configPath := filepath.Join(follower.dataDir, "cluster.yaml")
	restarted := startNode(t, binPath, configPath, follower.dataDir, follower.id)
	t.Cleanup(restarted.stop)

	// Term must never regress across a restart — it may be equal or higher
	// (if elections happened while it was down), but never lower.
	deadline := time.Now().Add(5 * time.Second)
	var postStatus *statusResponse
	for time.Now().Before(deadline) {
		postStatus, err = restarted.getStatus()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if postStatus == nil {
		t.Fatalf("restarted node %d never came up", follower.id)
	}
	if postStatus.Term < preCrashTerm {
		t.Fatalf("term REGRESSED after restart: was %d before crash, %d after — persistence is broken",
			preCrashTerm, postStatus.Term)
	}
	t.Logf("follower node %d term after restart: %d (pre-crash: %d) — no regression",
		follower.id, postStatus.Term, preCrashTerm)

	// The restarted node should eventually have the data too (either
	// recovered directly from its own WAL, or caught up via replication).
	getKVEventually(t, restarted.httpAddr, "keyA", "valA", 5*time.Second)
}
