package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// NodeConfig holds the identity and gRPC address of a single Raft node.
// ID is int64 to match the protobuf RPC schema (candidate_id, leader_id are int64).
type NodeConfig struct {
	ID      int64  `yaml:"id"`
	Address string `yaml:"address"` // host:port for gRPC, NOT the HTTP client port
}

// ClusterConfig holds the full list of nodes that form the Raft cluster.
type ClusterConfig struct {
	Nodes []NodeConfig `yaml:"nodes"`
}

// LoadConfig reads a YAML cluster configuration file from the given path
// and returns the parsed ClusterConfig. Returns an error if the file can't
// be read or the YAML is malformed.
func LoadConfig(path string) (*ClusterConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: failed to read file %s: %w", path, err)
	}

	var cfg ClusterConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config: failed to parse YAML: %w", err)
	}

	if len(cfg.Nodes) == 0 {
		return nil, fmt.Errorf("config: cluster must have at least one node")
	}

	return &cfg, nil
}

// PeersExcluding returns all nodes in the cluster EXCEPT the one with the
// given selfID. A node uses this to get the list of peers it needs to send
// RequestVote and AppendEntries RPCs to — it should never RPC to itself.
func (c *ClusterConfig) PeersExcluding(selfID int64) []NodeConfig {
	var peers []NodeConfig
	for _, node := range c.Nodes {
		if node.ID != selfID {
			peers = append(peers, node)
		}
	}
	return peers
}
