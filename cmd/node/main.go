package main

import (
	"flag"
	"log"

	"raftkv/internal/config"
	"raftkv/internal/kvstore"
	"raftkv/internal/raft"
	"raftkv/internal/server"
)

func main() {
	// ── Command-line flags ──────────────────────────────────────────────
	nodeID := flag.Int("id", 0, "This node's unique ID (must match an entry in cluster.yaml)")
	raftAddr := flag.String("raft-addr", "", "gRPC listen address for Raft RPCs (e.g. localhost:9001)")
	httpAddr := flag.String("http-addr", ":8080", "HTTP listen address for client-facing REST API")
	configPath := flag.String("config", "config/cluster.yaml", "Path to cluster configuration YAML file")
	flag.Parse()

	if *nodeID == 0 {
		log.Fatal("--id is required (must be a positive integer matching cluster.yaml)")
	}
	if *raftAddr == "" {
		log.Fatal("--raft-addr is required (e.g. localhost:9001)")
	}

	// ── Load cluster configuration ──────────────────────────────────────
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	peers := cfg.PeersExcluding(int64(*nodeID))
	log.Printf("[Node %d] loaded config: %d total nodes, %d peers", *nodeID, len(cfg.Nodes), len(peers))

	// ── Initialize Raft ─────────────────────────────────────────────────
	state := raft.NewState(*nodeID, peers)
	node := raft.NewRaftNode(state, *raftAddr)
	node.Start() // launches gRPC server, election timer, and supervisor goroutines

	log.Printf("[Node %d] Raft started (raft-addr=%s)", *nodeID, *raftAddr)

	// ── Start HTTP server (blocks) ──────────────────────────────────────
	// TODO(Phase 5): The HTTP server currently writes directly to the local
	// KV store, bypassing Raft consensus entirely. In Phase 5, writes will
	// be routed through Raft so that all nodes agree on the order of
	// operations. Non-leader nodes will forward writes to the leader.
	// For now this is fine — HTTP is just a placeholder for local testing.
	store := kvstore.NewStore()
	httpServer := server.NewHTTPServer(store)

	log.Printf("[Node %d] HTTP server starting on %s", *nodeID, *httpAddr)
	if err := httpServer.Start(*httpAddr); err != nil {
		log.Fatalf("[Node %d] HTTP server failed: %v", *nodeID, err)
	}
}