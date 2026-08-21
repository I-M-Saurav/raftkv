package server

import (
	"io"
	"log"
	"net/http"
	"strings"

	"raftkv/internal/kvstore"
)

// HTTPServer wraps the KV store and exposes it over REST.
type HTTPServer struct {
	store *kvstore.Store
}

// NewHTTPServer creates a server backed by the given store.
func NewHTTPServer(store *kvstore.Store) *HTTPServer {
	return &HTTPServer{store: store}
}

// Start registers routes and begins listening on addr (e.g. ":8080").
func (h *HTTPServer) Start(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", h.handleKV)

	log.Printf("HTTP server listening on %s", addr)
	return http.ListenAndServe(addr, mux)
}

// handleKV routes GET/PUT/DELETE on /kv/{key} to the right store method.
// This single handler dispatches by method because net/http's default mux
// doesn't distinguish HTTP methods on its own — we do that manually here.
func (h *HTTPServer) handleKV(w http.ResponseWriter, r *http.Request) {
	// Extract key from path: "/kv/foo" -> "foo"
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
		h.handleDelete(w, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *HTTPServer) handleGet(w http.ResponseWriter, key string) {
	value, err := h.store.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(value))
}

func (h *HTTPServer) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	h.store.Put(key, string(body))
	w.WriteHeader(http.StatusOK)
}

func (h *HTTPServer) handleDelete(w http.ResponseWriter, key string) {
	err := h.store.Delete(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}