package raft

import (
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"raftkv/internal/config"
	"raftkv/internal/raftpb"
)

// PeerConnections manages persistent, reusable gRPC connections and RPC clients
// for all peer nodes in the cluster.
//
// Instead of dialing peers on every RPC (which causes high TCP overhead and
// connection setup latency), connections are created once at startup and reused.
// gRPC handles reconnection and keep-alive internally.
type PeerConnections struct {
	conns   map[int64]*grpc.ClientConn
	clients map[int64]raftpb.RaftServiceClient
}

// NewPeerConnections eagerly creates persistent gRPC client connections to all
// peers. It uses grpc.NewClient (without WithBlock), which returns immediately
// and establishes connections in the background as RPCs are made.
func NewPeerConnections(peers []config.NodeConfig) (*PeerConnections, error) {
	conns := make(map[int64]*grpc.ClientConn, len(peers))
	clients := make(map[int64]raftpb.RaftServiceClient, len(peers))

	for _, p := range peers {
		conn, err := grpc.NewClient(p.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			for _, c := range conns {
				_ = c.Close()
			}
			return nil, fmt.Errorf("failed to create client connection to peer %d (%s): %w", p.ID, p.Address, err)
		}
		conns[p.ID] = conn
		clients[p.ID] = raftpb.NewRaftServiceClient(conn)
	}

	return &PeerConnections{
		conns:   conns,
		clients: clients,
	}, nil
}

// Client returns the persistent RaftServiceClient for the given peer ID.
// Safe for concurrent use without locking because the clients map is immutable
// after construction.
func (pc *PeerConnections) Client(peerID int64) (raftpb.RaftServiceClient, bool) {
	client, ok := pc.clients[peerID]
	return client, ok
}

// Close closes all underlying persistent gRPC connections.
func (pc *PeerConnections) Close() {
	for _, conn := range pc.conns {
		_ = conn.Close()
	}
}
