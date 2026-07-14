# Week 3：单机 KV + WAL + fsync

## 本周目标

理解分布式系统里最核心的一个概念：**"先写日志，再返回成功"**。

后续 Raft 的本质就是：复制日志 → 所有节点按同样顺序 apply → 得到相同状态。本周先把单机版的日志 + 状态机跑通。

---

## 你应该达到的水平

完成本周后，你要能回答：

1. **为什么 WAL 要写在业务逻辑之前？**（提示：如果先改内存再写 WAL，写完 WAL 前崩溃了会怎样）
2. **fsync 做了什么？为什么不能省？**（提示：OS 的 page cache 不会立即把数据写到磁盘）
3. **WAL replay 是什么流程？**（提示：启动时从头到尾重放 WAL 中的每一条记录，重建内存状态）
4. **WAL 文件末尾损坏了怎么办？**（提示：crc32 校验 → 截断损坏的记录 → 保留前面的完整记录）
5. **sync=always 和 sync=batch 的区别？**（提示：一条一刷 vs. 攒一批再刷，性能和安全性取舍）

---

## 代码任务

### 目录结构

```
week03_kv_wal/
├── kv/
│   ├── __init__.py
│   └── store.py          # KvStore：Put/Get/Delete + WAL
├── storage/
│   ├── __init__.py
│   └── wal.py            # WAL：append、replay、checksum、truncate
├── server_main.py         # 启动 RPC Server + KvStore
├── client_main.py         # 交互式客户端
├── data/                  # WAL 文件存放目录（运行时自动创建）
└── tests/
    ├── test_wal.py
    └── test_crash_recovery.py
```

### 第一步：`storage/wal.py` — WAL 模块

这是本周最核心的模块。你要实现：

```python
class WAL:
    def __init__(self, path: str, sync_mode="always"):
        """path: wal 文件路径，如 data/wal.log"""

    async def append(self, record: dict) -> None:
        """追加一条记录到 WAL。sync=always 时每条都 fsync"""

    async def replay(self) -> list[dict]:
        """启动时重放 WAL，返回所有有效记录。遇损坏记录则截断"""

    async def close(self):
        """关闭 WAL 文件"""
```

**WAL 记录格式**（每条一行 JSON）：

```json
{"crc32": 123456789, "op": "Put", "key": "x", "value": "1", "client_id": "c1", "seq": 1}
```

每条记录占一行（`\n` 分隔），写入流程：
```
1. 构造 record dict
2. json.dumps(record) → 计算 crc32
3. 写入 {"crc32": xxx, ...}\n
4. 如果 sync=always: flush + fsync
```

**replay 流程**：
```
1. 打开 WAL 文件
2. 逐行读取
3. 每行：json.loads → 取出 crc32 → 用剩余字段重新计算 crc32 → 对比
4. crc32 匹配：加入结果列表
5. crc32 不匹配 / JSON 解析失败：这是损坏行 → truncate 文件到这一行之前 → 停止 replay
```

**关键点**：
- **必须用 `await loop.run_in_executor(None, os.fsync, fd)`**，不能直接 `os.fsync(fd)`
- `sync=batch` 时每 N 条或每 T 毫秒 fsync 一次（先做 always，batch 可选）
- crc32 用 Python 内置的 `zlib.crc32` 或 `binascii.crc32`

### 第二步：`kv/store.py` — KvStore

```python
class KvStore:
    def __init__(self, wal_path: str, sync_mode="always"):
        """创建 KvStore，内部持有 WAL 实例"""

    async def put(self, key: str, value: str, client_id: str, seq: int) -> dict:
        """写操作：先写 WAL → fsync → 再改内存 → 返回"""

    async def get(self, key: str) -> Optional[str]:
        """读操作：直接读内存"""

    async def delete(self, key: str, client_id: str, seq: int) -> dict:
        """删除操作：也是先写 WAL"""

    async def recover(self):
        """启动恢复：replay WAL → 重建内存 HashMap"""

    async def close(self):
        """关闭"""
```

**Put 流程**（这是本周最关键的流程）：
```
1. 构造 WAL 记录：{"op": "Put", "key": key, "value": value, "client_id": cid, "seq": seq}
2. await wal.append(record)      ← 先写 WAL
3. 如果 sync=always: 此时 WAL 已 fsync
4. self.data[key] = value        ← 再改内存
5. 返回 {"ok": true, "result": "OK"}
```

**recover 流程**：
```
1. records = await wal.replay()
2. 遍历 records，按顺序 apply 到内存：
   - "Put": self.data[key] = value
   - "Delete": del self.data[key]
3. 同时重建去重表 client_table
```

### 第三步：`server_main.py` — 集成

把 Week 2 的 RPC Server 和本周的 KvStore 对接：

```python
# server_main.py
from rpc import server as rpc_server   # 复用 Week 2 的 codec + TCP 框架
from kv.store import KvStore
from common import log

store = KvStore("data/wal.log", sync_mode="always")
await store.recover()  # 启动时恢复

# RPC Server 的 dispatch 中：
# Put  → await store.put(key, value, client_id, seq)
# Get  → await store.get(key)
# Delete → await store.delete(key, client_id, seq)
```

**复用 Week 2**：直接把 `week02_rpc/rpc/` 的 codec 和 server 框架复制过来，dispatch 方法换成调 KvStore。去重逻辑可以放在 KvStore 里，也可以留在 dispatch 层——你自己决定。

### 第四步：`tests/`

两个测试文件：

**test_wal.py**：
- append → replay 往返一致性
- crc32 校验正确
- 损坏行被截断
- sync=always 重启不丢数据
- 大 value 不损坏

**test_crash_recovery.py**：
- Put 100 个 key → 模拟 kill → 重启 → Get 这 100 个 key 全部存在
- 手动破坏 WAL 最后一行 → 重启 → 前面的数据完整恢复
- Put 过程中 kill（WAL 写了一半）→ 重启不崩溃

---

## 验收标准（9 条）

- [ ] WAL append + replay：写入后重启能恢复全部数据
- [ ] crc32 校验：手动改 WAL 文件一行 → replay 能检测并截断
- [ ] sync=always 下，返回成功的数据重启不丢
- [ ] WAL 最后一行损坏时，前面的完整记录能恢复
- [ ] Put/Get/Delete 三个操作均可正常使用
- [ ] 启动恢复流程：空 WAL 不崩溃、有 WAL 正确重建状态
- [ ] 去重表（client_id + seq）在 recover 时同步重建
- [ ] `os.fsync()` 不能直接写在事件循环中，必须用 `run_in_executor`
- [ ] 所有测试可重复运行（每次测试前清理 data/ 目录）

---

## 本周不做

- 不做 snapshot（Week 4）
- 不做 CAS / version（Week 4）
- 不做多节点复制（Week 5）
- 不做 Raft（Week 9）
- 不做 WAL 分段 / 日志轮转

---

## 建议实现顺序

```
第 1 步：storage/wal.py   — append + replay（先不写 fsync）
第 2 步：kv/store.py      — 对接 WAL，实现 Put/Get/Delete
第 3 步：server_main.py   — 复用 Week 2 RPC 框架
第 4 步：加 fsync         — sync=always 模式
第 5 步：加 crc32         — 校验 + 损坏截断
第 6 步：tests/           — 测试 + crash recovery
```

写完第一步告诉我，我验收。
