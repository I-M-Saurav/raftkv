package config

import (
	"path/filepath"
	"runtime"
	"testing"
)

// testConfigPath returns the absolute path to config/cluster.yaml at the project root.
// We locate it relative to this test file: internal/config/ -> ../../config/cluster.yaml
func testConfigPath() string {
	_, thisFile, _, _ := runtime.Caller(0)
	projectRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")
	return filepath.Join(projectRoot, "config", "cluster.yaml")
}

func TestLoadConfig(t *testing.T) {
	cfg, err := LoadConfig(testConfigPath())
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// The sample cluster.yaml has exactly 3 nodes.
	if got := len(cfg.Nodes); got != 3 {
		t.Fatalf("expected 3 nodes, got %d", got)
	}

	// Verify a known node: ID 2 should have address "localhost:9002".
	found := false
	for _, n := range cfg.Nodes {
		if n.ID == 2 {
			found = true
			if n.Address != "localhost:9002" {
				t.Errorf("node 2 address: expected \"localhost:9002\", got %q", n.Address)
			}
			break
		}
	}
	if !found {
		t.Error("node with ID 2 not found in config")
	}
}

func TestPeersExcluding(t *testing.T) {
	cfg, err := LoadConfig(testConfigPath())
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// Excluding node 1 from a 3-node cluster should return 2 peers.
	peers := cfg.PeersExcluding(1)
	if got := len(peers); got != 2 {
		t.Fatalf("expected 2 peers excluding node 1, got %d", got)
	}

	// None of the returned peers should have ID 1.
	for _, p := range peers {
		if p.ID == 1 {
			t.Error("PeersExcluding(1) returned a node with ID 1")
		}
	}

	// Excluding an ID that doesn't exist should return all 3 nodes.
	all := cfg.PeersExcluding(999)
	if got := len(all); got != 3 {
		t.Fatalf("expected 3 peers excluding non-existent ID 999, got %d", got)
	}
}
