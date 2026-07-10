# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a 12-week distributed systems learning project with the goal of building `mini-raft-kv`: a minimal fault-tolerant distributed key-value store with Raft consensus, WAL, snapshots, retry deduplication, and fault injection tests.

The repo is organized by week, each building on the previous:

```
week01_tcp/              # TCP echo server, timeouts, connection lifecycle
week02_rpc/              # RPC framework, retry, idempotency (client_id + seq)
week03_kv_wal/           # Single-node KV store with WAL and crash recovery
week04_snapshot_cas/     # Snapshot, versioning, CAS
week05_primary_backup/   # Primary-backup replication
week06_consistency_lab/  # Stale read experiments, consistency models
week07_sharding/         # Sharded KV with routing table
week08_shard_controller/ # Shard controller, data migration, resharding
week09_raft_election/    # Raft leader election (3 nodes)
week10_raft_log/         # Raft log replication, commit, apply
week11_raft_kv/          # Raft persistence, replicated KV with linearizable reads
week12_fault_injection/  # Network simulator, partition tests, project integration docs
```

## Language Strategy

- **Experiments / rapid prototyping**: Python
- **Core project (Raft KV)**: C++
- Rationale: Python for fast iteration on network failure, timeout, and retry patterns; C++ for the final implementation that maps to downstream projects (BlockServe, ClusterPilot, IM).

## Build & Run Commands

As weeks progress, each `weekXX_*/` directory is self-contained. Prefer the following patterns:

### Python (weeks 1–8, experiments)

```bash
# Run server
python3 server.py

# Run client
python3 client.py

# Run tests
python3 -m pytest test.py -v
# or if using unittest:
python3 -m unittest test.py -v
```

### C++ (weeks 9–12, Raft & KV)

```bash
# Build
mkdir -p build && cd build && cmake .. && make -j$(nproc)

# Run a single node
./build/raft_node --id 1 --port 8001

# Run tests
cd build && ctest --output-on-failure
# or a single test:
./build/raft_test --gtest_filter=RaftElection.SingleLeader
```

## Weekly Deliverables

Every week must produce three files inside its directory:

| File | Purpose |
|---|---|
| `README.md` | How to start, how to test, what's supported, what's not yet |
| `design.md` | Data structures, request flow, failure handling, design rationale |
| `test_report.md` | What was tested, how, results, bugs found |

## Design Principles

1. **Network is unreliable** — every RPC must handle timeout, retry, and idempotency.
2. **Crash recovery is explicit** — WAL is written before acknowledging; state is rebuilt by replaying WAL on startup.
3. **Log replication drives consistency** — all nodes apply the same log in the same order to reach the same state.
4. **Majority is enough** — Raft commits when a majority of nodes have persisted the log entry; minority partitions must not commit.
5. **Fault injection is first-class** — every component should be testable under delay, drop, partition, crash, and restart.

## Commit Convention

Weekly commits follow the pattern:

```
week03: add wal append
week03: recover kv from wal
week03: handle corrupted wal tail
week03: add crash recovery test
```

At least 5 commits per week: skeleton → data structures → core flow → fault tests → fixes & summary.
