# 代码迁移指南：weekXX → src/mini_raft_kv/

## 迁移原则

- 只迁已验证的代码（Week 1～3 的核心模块）
- 迁过去后删掉旧目录，打 git tag 保留历史
- 迁完后从仓库根目录 `python scripts/start_server.py` 能启动

## 文件映射表

### Week 2 → 新结构

| 旧路径 | 新路径 |
|--------|--------|
| `common/log.py` | `src/mini_raft_kv/common/log.py` |
| `week02_rpc/rpc/codec.py` | `src/mini_raft_kv/rpc/codec.py` |
| `week02_rpc/rpc/client.py` | `src/mini_raft_kv/client/client.py`（注意：客户端逻辑移到 client/） |
| `week02_rpc/rpc/server.py` | `src/mini_raft_kv/rpc/server.py` |
| `week02_rpc/network/simulator.py` | `src/mini_raft_kv/network/simulator.py` |

### Week 3 → 新结构

| 旧路径 | 新路径 |
|--------|--------|
| `week03_kv_wal/storage/wal.py` | `src/mini_raft_kv/storage/wal.py` |
| `week03_kv_wal/kv/store.py` | `src/mini_raft_kv/kv/store.py`（Week 3 版本，不含 version/CAS） |

### Week 1（仅作历史保留，不迁移代码）

Week 1 的 threading + 文本协议已不再使用，打 tag 保留即可。

## 注意事项

1. **所有 import 路径改为 `from mini_raft_kv.xxx import yyy`**

   ```python
   # 旧
   from common import log
   from rpc import codec
   from storage.wal import WAL

   # 新
   from mini_raft_kv.common import log
   from mini_raft_kv.rpc import codec
   from mini_raft_kv.storage.wal import WAL
   ```

2. **去掉所有 `sys.path.insert` 的 workaround**，新结构是标准 Python 包，安装后直接 import。

3. **启动脚本放在 `scripts/`**

   ```python
   # scripts/start_server.py
   from mini_raft_kv.rpc.server import Server
   import asyncio

   async def main():
       srv = Server("data/wal.log")
       await srv.kvstore.recover()
       await srv.run()
   asyncio.run(main())
   ```

4. **安装为可编辑包**（开发阶段）

   ```bash
   pip install -e .
   ```

   这样 `from mini_raft_kv.rpc import codec` 在任何目录都能工作。

## 迁移后的目录

```
distributed-labs/
├── src/
│   └── mini_raft_kv/
│       ├── rpc/          # codec.py, server.py
│       ├── network/      # simulator.py
│       ├── storage/      # wal.py
│       ├── kv/           # store.py
│       ├── common/       # log.py
│       └── client/       # client.py
├── scripts/              # start_server.py, run_client.py
├── tests/                # unit/, integration/, fault/
├── data/                 # WAL + snapshot 文件
├── config/               # 以后放 yaml
├── docs/                 # 设计文档、里程碑
├── pyproject.toml        # 包配置
└── README.md
```
