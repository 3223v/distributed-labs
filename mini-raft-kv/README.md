# mini-raft-kv

一个在 12 周内持续演进的分布式 KV 存储学习项目。Python asyncio，零第三方依赖。

## 快速开始

```bash
cd mini-raft-kv

# 启动 server
python3 scripts/start_local.py

# 另开终端，用内置 client 操作
PYTHONPATH=src python3 -c "
import asyncio
from mini_raft_kv.client.client import Client
async def main():
    c = Client('127.0.0.1', 8000, v=1, client_id='demo', timeout=3)
    await c.call('put', {'key':'name','value':'alice'})
    r = await c.call('get', {'key':'name'})
    print(r)
asyncio.run(main())
"
```

## 配置

编辑 `config/local.yaml`：

```yaml
server:
  host: "127.0.0.1"
  port: 8000
  v: 1

wal:
  path: "data/wal.log"
  sync_mode: always        # always | batch（batch 暂未实现）

log:
  level: info
```

## 运行测试

```bash
cd mini-raft-kv

# 单元测试
PYTHONPATH=src python3 tests/unit/test_wal.py

# 集成测试
PYTHONPATH=src python3 tests/integration/test_basic.py
PYTHONPATH=src python3 tests/integration/test_recovery.py
PYTHONPATH=src python3 tests/integration/test_dedup.py

# 编译检查所有模块
find src -name "*.py" -exec python3 -m py_compile {} +
```

## 当前支持

| 功能 | 状态 |
|---|---|
| Put / Get / Delete | ✅ |
| Ping / Echo | ✅ |
| WAL (crc32, replay, 尾部截断) | ✅ |
| 崩溃恢复 (sync=always) | ✅ |
| 请求去重 (client_id + seq) | ✅ |
| 集群 / Raft | ❌ |
| CAS / Snapshot | ❌ |
| sync=batch | ❌ |

## 架构

```
Client → RPC Server → LocalEngine → KVStateMachine → WAL
```

引擎可替换（计划: LocalEngine → PrimaryBackupEngine → RaftEngine），上层不受影响。

详见 `docs/design.md`。
