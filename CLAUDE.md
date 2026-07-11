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
week07_sharded_kv/       # Sharded KV with routing table
week08_shard_controller/ # Shard controller, data migration, resharding
week09_raft_election/    # Raft leader election (3 nodes)
week10_raft_log_replication/ # Raft log replication, commit, apply
week11_raft_kv/          # Raft persistence, replicated KV with linearizable reads
week12_fault_injection/  # Network simulator, partition tests, project integration docs
```

The master plan is at `docs/plan.md` (the final version; the earlier draft was deleted).

## Language Strategy

- **Weeks 1–12**: Python with `asyncio` for all distributed logic. The goal is to understand distributed systems, not fight C++ memory/threading simultaneously.
- **After week 12**: C++ reimplementation of core modules (RPC, WAL, Raft state machine) for migration to BlockServe, ClusterPilot, and IM systems.
- See `docs/plan.md` for the full 12-week roadmap and design rationale.

## Concurrency Model: Actor Pattern (Mandatory)

All Raft nodes (weeks 9–12) MUST use the Actor model:

```python
class RaftNode:
    def __init__(self):
        self.inbox = asyncio.Queue()
        ...

    async def run(self):
        while True:
            event = await self.inbox.get()
            await self.handle_event(event)
```

Hard rules:
- Raft state (`current_term`, `voted_for`, `log`, `commit_index`, etc.) can ONLY be modified inside `handle_event`.
- No `threading.Lock`, no multi-threaded Raft nodes, no multiple coroutines modifying Raft state concurrently.
- Timers only post events (`ElectionTimeout`, `HeartbeatTick`) to the inbox — they never mutate state directly.

## RPC Protocol (Week 2 Onward)

Length-prefix JSON over TCP:

```
[4 bytes big-endian length][json bytes]
```

Read MUST use `readexactly` — never bare `read()`:

```python
header = await reader.readexactly(4)
size = int.from_bytes(header, "big")
body = await reader.readexactly(size)
```

`reader.read(n)` does NOT guarantee n bytes; TCP is a byte stream with no message boundaries.

Request/response shape:
```json
{"request_id": 1, "client_id": "c1", "seq": 1, "method": "Put", "params": {"key": "x", "value": "1"}}
{"request_id": 1, "ok": true, "result": "OK", "error": ""}
```

Week 1 uses a simpler text protocol for learning: `request_id:1,body:hello` (colon-separated, newline-delimited).

## WAL and fsync Rules

WAL supports two modes: `sync=always` (fsync every write) and `sync=batch` (fsync every N records or T ms).

**Never call `os.fsync()` directly in the event loop.** It's a blocking syscall that freezes timers and heartbeats:

```python
# WRONG
os.fsync(fd)

# CORRECT
loop = asyncio.get_running_loop()
await loop.run_in_executor(None, os.fsync, fd)
```

Snapshot atomicity: write to `.tmp` → fsync `.tmp` → rename to final → fsync directory.

## Client Deduplication (client_table)

Every write request carries `client_id` + `seq`. The server tracks `{client_id: {last_seq, last_result}}`.

This table MUST be included in snapshots. If `client_table` is memory-only, a restart forgets processed requests and replays duplicates.

## Execution Red Lines

These are non-negotiable rules from the master plan:

1. No `threading` for Raft — Actor + asyncio only
2. No C++ before week 12 is complete
3. No gRPC before Raft runs on length-prefix JSON
4. No fixed election timeout — must be randomized
5. No `os.fsync()` directly in the event loop
6. All length-prefix reads must use `readexactly`
7. Every write's WAL success must have explicit fsync semantics
8. Every client write request must carry `client_id` + `seq`
9. `client_table` must be in every snapshot
10. All tests must be repeatable

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

## Build & Run Commands

### Python (all weeks 1–12)

```bash
# Run server
python3 server.py

# Run client
python3 client.py

# Run tests (when test framework is added)
python3 -m pytest test.py -v
```

### C++ (post-week-12 reimplementation, not yet active)

```bash
mkdir -p build && cd build && cmake .. && make -j$(nproc)
./build/raft_node --id 1 --port 8001
cd build && ctest --output-on-failure
```

## Commit Convention

Weekly commits follow the pattern:

```
week03: add wal append
week03: recover kv from wal
week03: handle corrupted wal tail
week03: add crash recovery test
```

At least 5 commits per week: skeleton → data structures → core flow → fault tests → fixes & summary.
