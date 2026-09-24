package server

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
	"encoding/json"

	"raftkv/internal/config"
	"raftkv/internal/kvstore"
	"raftkv/internal/raft"
)

// HTTPServer is the client-facing REST API for one Raft node. It serves
// GET/PUT/DELETE on /kv/{key}. Writes (PUT/DELETE) are only accepted when
// this node is the current Raft leader; otherwise the request is
// transparently forwarded to whichever node IS the leader, per the
// original project spec: "Client hits any node via HTTP -> if not leader,
// forwards to leader."
type HTTPServer struct {
	store      *kvstore.Store
	state      *raft.State
	cfg        *config.ClusterConfig
	httpClient *http.Client
}

// NewHTTPServer creates a server backed by the given store, Raft state, and
// cluster config (needed to look up the leader's HTTP address for forwarding).
func NewHTTPServer(store *kvstore.Store, state *raft.State, cfg *config.ClusterConfig) *HTTPServer {
	return &HTTPServer{
		store: store,
		state: state,
		cfg:   cfg,
		httpClient: &http.Client{
			Timeout: 3 * time.Second,
		},
	}
}

// withCORS wraps a handler to allow cross-origin requests from the
// dashboard (Phase 8), which runs on a different origin (e.g.
// localhost:3000) than the node's own HTTP API. Without this, browsers
// block the dashboard's fetch() calls per the same-origin policy.
func withCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next(w, r)
	}
}



// handleCluster returns the full list of nodes in the cluster (ID + HTTP
// address), sourced from cluster.yaml. Used by the dashboard (Phase 8) to
// discover cluster topology dynamically — it doesn't need any node count
// hardcoded, so the dashboard scales to any cluster size (3, 5, 10+ nodes)
// without code changes.
func (h *HTTPServer) handleCluster(w http.ResponseWriter, r *http.Request) {
	type clusterNode struct {
		ID          int64  `json:"id"`
		HTTPAddress string `json:"http_address"`
	}
	nodes := make([]clusterNode, 0, len(h.cfg.Nodes))
	for _, n := range h.cfg.Nodes {
		nodes = append(nodes, clusterNode{ID: n.ID, HTTPAddress: n.HTTPAddress})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(struct {
		Nodes []clusterNode `json:"nodes"`
	}{Nodes: nodes})
}


// Start registers routes and begins listening on addr (e.g. ":8080").
func (h *HTTPServer) Start(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", withCORS(h.handleKV))
	mux.HandleFunc("/status", withCORS(h.handleStatus))
	mux.HandleFunc("/cluster", withCORS(h.handleCluster))

	log.Printf("HTTP server listening on %s", addr)
	return http.ListenAndServe(addr, mux)
}

// handleKV routes GET/PUT/DELETE on /kv/{key} to the right handler.
func (h *HTTPServer) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.handleGet(w, key)
	case http.MethodPut:
		h.handlePut(w, r, key)
	case http.MethodDelete:
		h.handleDelete(w, r, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleGet reads directly from the LOCAL KV store on whichever node
// received the request — NOT forwarded to the leader. This is an
// eventually-consistent read: a follower may briefly lag the leader until
// replication + apply completes. This mirrors how many real distributed KV
// stores default (e.g. etcd allows any member to serve reads unless you
// explicitly request a linearizable read).
func (h *HTTPServer) handleGet(w http.ResponseWriter, key string) {
	value, err := h.store.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(value))
}

// handlePut proposes a PUT command through Raft if this node is the leader,
// or forwards the request to the current leader otherwise.
func (h *HTTPServer) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	h.state.Lock()
	isLeader := h.state.CurrentState == raft.Leader
	leaderID := h.state.LeaderID
	h.state.Unlock()

	if !isLeader {
		h.forwardToLeader(w, r, leaderID)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	cmd := raft.CommandPayload{Op: "PUT", Key: key, Value: string(body)}
	encoded, err := raft.EncodeCommand(cmd)
	if err != nil {
		http.Error(w, "failed to encode command", http.StatusInternalServerError)
		return
	}

	h.proposeAndWait(w, encoded)
}

// handleDelete proposes a DELETE command through Raft if this node is the
// leader, or forwards the request to the current leader otherwise.
func (h *HTTPServer) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	h.state.Lock()
	isLeader := h.state.CurrentState == raft.Leader
	leaderID := h.state.LeaderID
	h.state.Unlock()

	if !isLeader {
		h.forwardToLeader(w, r, leaderID)
		return
	}

	cmd := raft.CommandPayload{Op: "DELETE", Key: key}
	encoded, err := raft.EncodeCommand(cmd)
	if err != nil {
		http.Error(w, "failed to encode command", http.StatusInternalServerError)
		return
	}

	h.proposeAndWait(w, encoded)
}

// proposeAndWait sends an encoded command through Raft and blocks until it
// has been applied to the local state machine, or times out. By the time
// this returns success, the write is durable (committed by majority) AND
// visible to subsequent GETs on this node — a "read your own write"
// guarantee against the leader.
func (h *HTTPServer) proposeAndWait(w http.ResponseWriter, encodedCmd []byte) {
	index, _, ok := raft.ProposeCommand(h.state, encodedCmd)
	if !ok {
		// Lost leadership between our check above and ProposeCommand itself
		// (a narrow race). The client should simply retry.
		http.Error(w, "leadership changed, please retry", http.StatusServiceUnavailable)
		return
	}

	if !raft.WaitForApply(h.state, index, 2*time.Second) {
		http.Error(w, "write accepted but not yet applied (timed out) — it may still complete shortly", http.StatusGatewayTimeout)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// forwardToLeader proxies the incoming request to whichever node is the
// current known leader. The original method, path, and body are preserved;
// the leader's response is relayed back to the original client unchanged.
func (h *HTTPServer) forwardToLeader(w http.ResponseWriter, r *http.Request, leaderID int) {
	if leaderID == -1 {
		http.Error(w, "no leader currently known, please retry shortly", http.StatusServiceUnavailable)
		return
	}

	node, ok := h.cfg.NodeByID(int64(leaderID))
	if !ok {
		http.Error(w, "internal error: unknown leader node ID", http.StatusInternalServerError)
		return
	}

	url := fmt.Sprintf("http://%s%s", node.HTTPAddress, r.URL.Path)
	forwardReq, err := http.NewRequest(r.Method, url, r.Body)
	if err != nil {
		http.Error(w, "failed to build forwarded request", http.StatusInternalServerError)
		return
	}
	forwardReq.Header = r.Header.Clone()

	log.Printf("forwarding %s %s to leader (node %d, %s)", r.Method, r.URL.Path, leaderID, node.HTTPAddress)

	resp, err := h.httpClient.Do(forwardReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to reach leader: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

		for k, values := range resp.Header {
		// Skip CORS headers from the leader's response — this node's own
		// withCORS wrapper has already set them once on w. Copying the
		// leader's copy on top via Add() would result in TWO
		// Access-Control-Allow-Origin headers in the final response, which
		// browsers reject outright (fails with a generic "Failed to fetch"),
		// even though the underlying write succeeded.
		if strings.HasPrefix(k, "Access-Control-") {
			continue
		}
		for _, v := range values {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}


// handleStatus returns this node's current Raft status as JSON — role, term,
// believed leader, and commit/apply progress. Used by automated tests
// (Phase 7) and the optional dashboard (Phase 8) to observe cluster state
// without parsing log output.
func (h *HTTPServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	h.state.Lock()
	status := struct {
		NodeID      int    `json:"node_id"`
		State       string `json:"state"`
		Term        int    `json:"term"`
		LeaderID    int    `json:"leader_id"`
		CommitIndex int    `json:"commit_index"`
		LastApplied int    `json:"last_applied"`
	}{
		NodeID:      h.state.NodeID,
		State:       h.state.CurrentState.String(),
		Term:        h.state.CurrentTerm,
		LeaderID:    h.state.LeaderID,
		CommitIndex: h.state.CommitIndex,
		LastApplied: h.state.LastApplied,
	}
	h.state.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}