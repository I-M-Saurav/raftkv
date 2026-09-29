# RaftKV

A distributed, fault-tolerant key-value store implementing the **Raft consensus algorithm from scratch** in Go — no external consensus libraries (no `etcd/raft`, no `hashicorp/raft`). Every RPC, election, log replication, and persistence rule is implemented directly from the Raft paper's specification.

> **Paper:** Diego Ongaro and John Ousterhout, *"In Search of an Understandable Consensus Algorithm (Extended Version)"*, Stanford University, 2014. [raft.github.io/raft.pdf](https://raft.github.io/raft.pdf)

## What this is

A cluster of 3–10 nodes that together behave like a single, highly-available key-value store. Clients read and write via a plain HTTP REST API on *any* node — the cluster internally elects a leader, replicates every write to a majority of nodes before acknowledging it, and automatically recovers from node crashes and leader failures with zero data loss.

This is the same class of problem solved by real production systems — etcd (which powers Kubernetes), CockroachDB, and Kafka's controller quorum all rely on Raft or a closely related consensus protocol.

## Demo

A companion visual dashboard is available at [raftkv-dashboard](https://github.com/I-M-Saurav/raftkv-dashboard) — it shows live cluster state (current leader, term, commit index per node) and lets you issue reads/writes from the browser, so you can watch leader election and failover happen in real time.

## Architecture

```
                     ┌─────────────┐
        HTTP client ─┤   any node  │
                     └──────┬──────┘
                            │  (forwarded to leader if this node isn't it)
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
         ┌─────────┐  ┌─────────┐  ┌─────────┐
         │ Node 1  │  │ Node 2  │  │ Node 3  │
         │ (Raft)  │◄─┤ (Raft)  │─►│ (Raft)  │   gRPC: RequestVote,
         └────┬────┘  └────┬────┘  └────┬────┘         AppendEntries
              │            │            │
         ┌────▼────┐  ┌────▼────┐  ┌────▼────┐
         │  KV     │  │  KV     │  │  KV     │
         │  store  │  │  store  │  │  store  │
         └─────────┘  └─────────┘  └─────────┘
              │            │            │
         ┌────▼────┐  ┌────▼────┐  ┌────▼────┐
         │  WAL    │  │  WAL    │  │  WAL    │   term, votedFor, log
         │ (disk)  │  │ (disk)  │  │ (disk)  │   persisted before
         └─────────┘  └─────────┘  └─────────┘   responding to any RPC
```

- **Client-facing API:** plain HTTP/REST (`net/http`), so any HTTP client can talk to the cluster — no special client library needed.
- **Node-to-node communication:** gRPC with Protocol Buffers, used exclusively for the two Raft RPCs — `RequestVote` and `AppendEntries`.
- **State machine:** an in-memory `map[string]string`, replicated across the cluster via the Raft log.
- **Persistence:** a write-ahead log per node — `currentTerm`, `votedFor`, and the full replicated log are flushed to disk (atomic write + fsync) before any RPC response is sent, so a crash never causes a node to contradict a promise it already made.
- **Cluster membership:** static, defined in `config/cluster.yaml` and loaded once per node at startup.

## Tech stack

| Layer | Technology |
|---|---|
| Core implementation | Go |
| Node-to-node RPC | gRPC + Protocol Buffers |
| Client API | HTTP (`net/http`) |
| Persistence | Custom write-ahead log (flat JSON files, atomic writes + fsync) |
| Testing | Go's `testing` package + custom fault-injection (real process spawning/killing) |
| Dashboard (optional) | Next.js + TypeScript, polling the cluster's HTTP API |

## What's implemented

- **Leader election** (§5.2) — randomized election timeouts (150–300ms), concurrent `RequestVote` fan-out, majority-based leader selection, correct term/vote handling on every RPC path.
- **Log replication** (§5.3) — leader-driven `AppendEntries` replication with per-follower `nextIndex`/`matchIndex` tracking, log consistency checking (conflict detection and truncation), and idempotent entry application.
- **Safety** (§5.4) — the election restriction (a candidate with a stale log cannot win), and the leader commit restriction (§5.4.2: a leader only directly commits entries from its own current term, never an older term, closing the classic "stranded entry" gap described in the paper).
- **Client interaction** — writes to any node are transparently forwarded to the current leader; writes block until the entry is committed *and* applied, giving read-your-own-write consistency against the node you talked to.
- **Persistence** — write-ahead log with atomic file writes (temp file + fsync + rename) so a crash mid-write never corrupts state; a restarted node recovers its exact term, vote, and log rather than starting blank.
- **Fault tolerance, empirically verified** — automated integration tests spawn real OS processes for each node, kill them mid-test to simulate crashes, and assert (via the real HTTP API) that the cluster elects a new leader, preserves already-committed data, and continues accepting writes.

## Project structure

```
raftkv/
├── cmd/node/            # entrypoint — wires config, Raft, HTTP, and storage together
├── internal/
│   ├── raft/            # core consensus: state, election, replication, persistence
│   ├── kvstore/         # the in-memory state machine (thread-safe map)
│   ├── server/          # HTTP REST API + leader-forwarding logic
│   ├── config/          # cluster.yaml loading
│   └── raftpb/          # generated gRPC/protobuf code
├── proto/raft.proto     # RequestVote / AppendEntries RPC definitions
├── integration/         # fault-injection integration tests (real process spawning)
├── scripts/             # cluster automation (generate config + launch N nodes)
├── config/cluster.yaml  # cluster membership definition
└── data/                # per-node WAL files (gitignored)
```

## Running the cluster

### Prerequisites

- Go 1.22+
- `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc` (only needed if you modify `proto/raft.proto`)

### Build

```powershell
go build -o raftkv.exe ./cmd/node
```

### Start a cluster (recommended: automated scripts)

```powershell
.\scripts\start-cluster.ps1 -NodeCount 5
```

This regenerates `config/cluster.yaml` for the requested node count (1–10), clears old WAL data for a clean run, and launches each node in its own terminal window.

### Start a cluster manually

```powershell
.\raftkv.exe --id 1 --raft-addr localhost:9001 --http-addr :8081
.\raftkv.exe --id 2 --raft-addr localhost:9002 --http-addr :8082
.\raftkv.exe --id 3 --raft-addr localhost:9003 --http-addr :8083
```

| Flag | Description |
|---|---|
| `--id` | This node's unique ID (must match `cluster.yaml`) |
| `--raft-addr` | gRPC address for Raft-to-Raft RPCs |
| `--http-addr` | HTTP address for the client-facing REST API |
| `--config` | Path to `cluster.yaml` (default `config/cluster.yaml`) |
| `--data-dir` | Directory for this node's WAL files (default `data`) |

### Using the API

```powershell
# Write — can be sent to ANY node; non-leaders forward automatically
curl.exe -X PUT -d "world" http://localhost:8081/kv/hello

# Read — served from whichever node you ask (may briefly lag the leader)
curl.exe http://localhost:8081/kv/hello

# Delete
curl.exe -X DELETE http://localhost:8081/kv/hello

# Node status — role, term, believed leader, commit/apply progress
curl.exe http://localhost:8081/status

# Cluster topology — used by the dashboard for auto-discovery
curl.exe http://localhost:8081/cluster
```

## Testing

```powershell
go test ./...
```

Unit tests cover Raft state helpers and cluster config parsing. The integration suite (`go test ./integration/... -v`) builds the real binary, spawns a live multi-node cluster as separate OS processes, and runs three fault-injection scenarios:

- **`TestElection`** — a fresh cluster elects exactly one leader.
- **`TestReplicationAndFailover`** — a write to the leader replicates to all nodes; the leader is then hard-killed; the surviving majority elects a new leader, retains all previously committed data, and continues accepting new writes.
- **`TestPersistenceRecovery`** — a follower is killed and restarted; its recovered term is asserted to never regress across the restart, proving the write-ahead log is actually durable.

## Design notes and known trade-offs

- **Static cluster membership.** Nodes are defined once in `cluster.yaml` at startup; adding or removing a node requires a full cluster restart. The paper's §6 dynamic membership-change protocol (joint consensus) is not implemented — a deliberate scope decision, as it's a materially separate feature from the core algorithm in Figure 2.
- **`nextIndex` backtracking is unoptimized.** On a log inconsistency, `nextIndex` decrements by one and retries, which is correct but O(n) in the worst case. The paper's extended version describes a faster conflict-term-based backtracking scheme as a known follow-up optimization.
- **No log compaction/snapshotting.** The replicated log grows unboundedly; a production system would periodically snapshot the state machine and truncate the log. Not implemented here, as it's an orthogonal scaling concern rather than a correctness one.
- **WAL uses full-file rewrites, not true append-only segments.** `log.json` is rewritten in full (atomically) on every change rather than appended to incrementally. Correct and simple at this scale; a production WAL would use append-only segment files to avoid O(n) rewrite cost as the log grows.

## Why build this instead of using an existing Raft library

The value of this project is in implementing consensus correctly, not in the KV store itself — the map is intentionally the simplest possible state machine so the focus stays on the algorithm. Building this surfaced (and required understanding, then fixing) several of Raft's genuinely subtle correctness requirements first-hand: the vote-then-term-update ordering, the leader-commit term restriction and the "stranded entry" failure mode it causes on leader change, and the concurrency discipline needed when multiple goroutines (election timers, heartbeat loops, RPC handlers) share state under real network latency and out-of-order message delivery.