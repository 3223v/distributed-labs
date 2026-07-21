# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Working Mode: Division of Labor

The user is learning distributed systems by writing all implementation code themselves. Claude's role:

- **Claude does**: tests, documentation (README.md, design.md), code review, acceptance verification, architecture guidance, refactoring wiring (imports/names/syntax) when asked.
- **User does**: all business logic and feature implementation.
- **No per-week milestone docs** — only `docs/design.md` is maintained as the cumulative architecture document. No `docs/milestones/week-XX.md` files.

## Project Overview

This is a 12-week distributed systems learning project building a **single evolving system**: `mini-raft-kv/` — a fault-tolerant distributed KV store with length-prefix JSON RPC, pluggable replication engines, WAL, snapshots, request deduplication, sharding, Raft consensus, and fault-injection tests.

The master plan is `mini-raft-kv/docs/plan.md` (Chinese). Key principle: **one repo, one main branch, one continuously evolving system** — no per-week throwaway projects. An earlier per-week layout (`week01_tcp/`, `week02_rpc/`, ...) was deleted and replaced by this single-project structure.

Everything is Python asyncio for the first 12 weeks; C++ reimplementation only comes after. **No third-party dependencies** — even YAML parsing is hand-rolled in `config.py` on purpose; do not introduce PyYAML, pytest plugins, gRPC, etc.

## Current Progress

- **Week 3 (LocalEngine + KV + WAL)**: Core done — layer architecture (Server→Engine→StateMachine→WAL), ReplicationEngine interface + LocalEngine, WAL append/replay with crc32 + tail truncation, request dedup (client_id+seq in state machine apply), crash recovery. Acceptance tests pass (Put/Get/Delete, kill-9 restart, WAL corruption, dedup). **Remaining**: WAL `sync=batch` mode, state machine del/CAS edge case fixes.
- **Implemented modules**: `common/{log,codec,config,command,query}`, `rpc/server`, `client/client`, `replication/{base,local}`, `kv/{state_machine,client_table}`, `storage/wal`, `scripts/start_local`.
- **Not started**: network simulator, PrimaryBackupEngine, snapshot, Raft, sharding, `pyproject.toml`, `docker-compose.yml`.
- **Package layout deviates from plan §4**: `codec`, `config`, `command`, `query` are in `common/` instead of their original planned locations (`rpc/`, root, `kv/`). This is intentional — shared modules live in `common/`.

## Architecture

### Fixed data path (from week 3 onward, components stubbed until built)

```
Client -> RpcClient -> RpcServer -> Router -> ShardGroup
       -> ReplicationEngine -> KVStateMachine -> WAL / Snapshot
```

Early stages use the simplest implementation of each layer (single-shard router, one group, LocalEngine); later weeks swap internals without changing callers.

### ReplicationEngine abstraction (core of the whole plan)

All replication modes implement one interface (`start/submit/read/stop`), selected by config:

```
LocalEngine (wk 3–4) -> PrimaryBackupEngine (wk 5–8) -> RaftEngine (wk 9–12)
```

Old engines are kept as consistency baselines, never deleted. Never build a second KV or second RPC stack for a new engine.

### Package layout

`src/` layout: code lives in `mini-raft-kv/src/mini_raft_kv/` with package-absolute imports (`from mini_raft_kv.rpc import codec`). Running anything requires `src` on `PYTHONPATH` (entry scripts insert it via `sys.path`). Subpackages: `rpc/`, `client/`, `network/`, `storage/`, `kv/`, `common/`; planned: `replication/`, `raft/`, `sharding/`, `observability/`.

## Non-Negotiable Technical Rules

From `docs/plan.md` §10 (execution red lines):

1. **Actor model for Raft**: one `asyncio.Queue` inbox per node; state (`current_term`, `voted_for`, `log`, `commit_index`) modified ONLY inside `handle_event`. No `threading`, no locks, no concurrent coroutine mutation. Timers only post events (`ElectionTimeout`, `HeartbeatTick`).
2. **RPC protocol**: `[4-byte big-endian length][JSON]`. Reads MUST use `await reader.readexactly(n)` — never bare `read(n)` (TCP has no message boundaries).
3. **fsync never blocks the event loop**: `await loop.run_in_executor(None, os.fsync, fd)` (or `asyncio.to_thread`). WAL modes: `sync=always` (fsync per record) and `sync=batch` (every N records / T ms) with documented loss semantics.
4. **Every write carries `client_id` + `seq`**; retries reuse the same `seq`. Server dedup table (`client_id -> {last_seq, last_result}`) MUST be included in every snapshot, or restarts replay duplicates.
5. **Snapshot atomicity**: write `.tmp` → fsync `.tmp` → rename → fsync directory.
6. **Election timeout must be randomized** (e.g., 300–600ms) and greater than heartbeat interval (100ms).
7. No C++ before week 12; no gRPC before Raft works on length-prefix JSON.
8. Fault testing goes through the shared application-level Network Simulator (delay/drop/reorder/partition/heal/crash/restart) — real network faults (tc/iptables/Docker) only in week 12 on Linux.
9. End-to-end path must stay runnable at every week's end; no rewrite-from-scratch between weeks; temporary implementations must be isolated behind interfaces.

### Message shapes

```json
{"request_id": 1, "client_id": "c1", "seq": 1, "method": "Put", "params": {"key": "x", "value": "1"}}
{"request_id": 1, "ok": true, "result": "OK", "error": ""}
```

WAL record: one JSON per line, with `crc32` field; replay validates checksums and truncates at the first corrupted record.

## Run & Test Commands

```bash
cd mini-raft-kv

# Server
python3 scripts/start_local.py
# or
PYTHONPATH=src python3 -m mini_raft_kv.app

# Integration tests
PYTHONPATH=src python3 tests/integration/test_basic.py
PYTHONPATH=src python3 tests/integration/test_recovery.py
PYTHONPATH=src python3 tests/integration/test_dedup.py

# Unit tests
PYTHONPATH=src python3 tests/unit/test_wal.py

# Verify all modules compile
find src scripts tests -name "*.py" -exec python3 -m py_compile {} +

# Single module verify
PYTHONPATH=src python3 -c "from mini_raft_kv.common import codec; print('ok')"
```

Config: `config/local.yaml` (server/client/wal/log sections); loaded via `mini_raft_kv.common.config.load_config`. Planned configs: `primary-backup.yaml`, `raft-3.yaml`, `raft-5.yaml`, `sharded-raft.yaml`.

## Documentation & Commit Conventions

**Only `docs/design.md`** is maintained as the cumulative architecture document (updated each week). No per-week milestone files. README.md kept current by Claude.

Commits are **module-prefixed**:

```
storage: add wal append and checksum
replication: add primary-backup engine
raft: advance commit index after quorum
tests: add leader partition recovery case
```

At least 5 meaningful commits per week (skeleton → data structures → end-to-end → fault tests → fixes/docs); weeks may end with a tag like `week-03-local-kv`.
