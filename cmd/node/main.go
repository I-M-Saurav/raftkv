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
	id := flag.Int64("id", 0, "this node's unique ID (must match cluster.yaml)")
	raftAddr := flag.String("raft-addr", "", "gRPC listen address for Raft RPCs (e.g. localhost:9001)")
	httpAddr := flag.String("http-addr", ":8080", "HTTP listen address for the client-facing KV API")
	configPath := flag.String("config", "config/cluster.yaml", "path to cluster.yaml")
	dataDir := flag.String("data-dir", "data", "directory for this node's persisted WAL files")
	flag.Parse()

	if *id == 0 || *raftAddr == "" {
		log.Fatalf("--id and --raft-addr are required")
	}

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	peers := cfg.PeersExcluding(*id)
	log.Printf("[Node %d] loaded config: %d total nodes, %d peers", *id, len(cfg.Nodes), len(peers))

	// ── Phase 6: load persisted state from disk (if any) ────────────────
	storage, err := raft.NewStorage(*dataDir, int(*id))
	if err != nil {
		log.Fatalf("[Node %d] failed to initialize storage: %v", *id, err)
	}

	loadedTerm, loadedVotedFor, loadedEntries, err := storage.LoadState()
	if err != nil {
		log.Fatalf("[Node %d] failed to load persisted state: %v", *id, err)
	}

	state := raft.NewState(int(*id), peers)
	state.CurrentTerm = loadedTerm
	state.VotedFor = loadedVotedFor
	if len(loadedEntries) > 0 {
		state.Log = append(state.Log[:1], loadedEntries...) // keep dummy at index 0
	}
	state.Storage = storage // attach AFTER hydrating, so no persistence fires during setup

	log.Printf("[Node %d] recovered from WAL: term=%d, votedFor=%d, logEntries=%d",
		*id, loadedTerm, loadedVotedFor, len(loadedEntries))

	store := kvstore.NewStore()

	raftNode, err := raft.NewRaftNode(state, *raftAddr, store)
	if err != nil {
		log.Fatalf("[Node %d] failed to create Raft node: %v", *id, err)
	}
	raftNode.Start()
	log.Printf("[Node %d] Raft started (raft-addr=%s)", *id, *raftAddr)

	httpServer := server.NewHTTPServer(store, state, cfg)
	log.Printf("[Node %d] HTTP server starting on %s", *id, *httpAddr)
	if err := httpServer.Start(*httpAddr); err != nil {
		log.Fatalf("[Node %d] HTTP server failed: %v", *id, err)
	}
}