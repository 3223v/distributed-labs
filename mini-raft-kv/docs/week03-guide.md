# 第三周改造指南：从「RPC 内联 KV」到「LocalEngine + 状态机 + WAL」

> 本文档是行动指南，不是实现代码。每一步告诉你：做什么、为什么、接口长什么样、怎么自测。
> 对照 `docs/plan.md` 第 7 节「第 3 周」和第 2.2 节「ReplicationEngine 抽象」。

---

## 1. 第三周结束时系统应该长什么样

```
client -> RPC(Server) -> LocalEngine -> KVStateMachine -> WAL
                              |               |
                              |               +-- client_table（去重表）
                              +-- ReplicationEngine 接口（第 5 周换 PrimaryBackup、第 9 周换 Raft）
```

验收标准（plan.md 第 3 周原文）：

1. `sync=always` 下成功返回的数据重启不丢
2. `sync=batch` 的数据丢失边界有文档说明
3. WAL 尾部损坏不影响前面完整记录
4. 重复请求不会重复 apply
5. 第 2 周的 RPC 和重试代码被直接复用（不重写）

---

## 2. 现状盘点

### 已经有的（可以直接复用）

| 模块 | 状态 |
|---|---|
| `rpc/codec.py` | ✅ length-prefix JSON，readexactly，无需改动 |
| `client/client.py` | ✅ timeout + retry + seq 固定，第三周基本不用动（见 3.4 的一个坑） |
| `storage/wal.py` | ✅ append + crc32 + replay + 尾部截断，已经覆盖必做项 1~5 |
| `common/log.py` / `config.py` | ✅ 直接用 |

### 缺的（第三周要建的）

| 模块 | 作用 |
|---|---|
| `replication/base.py` | `ReplicationEngine` 抽象接口（全计划的核心骨架） |
| `replication/local.py` | `LocalEngine`：单机实现，submit = WAL + apply |
| `kv/command.py` | 定义「命令」——写进 WAL、被状态机 apply 的统一数据单元 |
| `kv/state_machine.py` | `KVStateMachine`：apply(command) 是唯一改状态的入口 |
| `kv/client_table.py` | 去重表，独立成模块（第 4 周要进 snapshot，第 11 周要进 Raft） |
| `storage/recovery.py` | 启动恢复流程：replay WAL → 重建 KV + client_table |
| `storage/fsync.py` | fsync 封装 + batch 模式的计数/定时逻辑 |
| WAL `sync=batch` | 目前只有 always，必做项 6 |

### 现有代码的问题清单（改造前先修，都是你自己修）

1. **`app.py:4`**：`asyncio def main()` —— 语法错误，跑不起来。
2. **`scripts/start_local.py:1`**：行首有空格，IndentationError。
3. **`kv/store.py:10`**：用了 `wal.WAL` 但没有 `import`；反而 import 了用不到的 `codec`。
4. **`kv/store.py`**：`put` 里 `seq` 转成了 `str(seq)`，`delete` 里是 `int` —— 类型不一致。replay 出来的 seq 和内存里的 seq 比较会出错，去重直接失效。统一用 int。
5. **`rpc/server.py`**：KV 业务逻辑（hash_map、dedup_table）全部内联在 `dispatch` 里 —— 这正是第三周要拆掉的东西。
6. **`rpc/server.py:62` 隐藏 bug（思考题）**：`last_result` 缓存了完整响应，**包括 request_id**。而 client 重试时 `request_id` 会 +1、`seq` 不变。推演一下：请求成功但响应丢失 → client 用 request_id+1 重试 → server 命中去重，返回缓存的**旧 request_id** → client 在 `client.py:56` 检查 request_id 不匹配 → 丢弃响应继续重试 → 永远失败。**结论：去重表应缓存 result 本体，响应时用当前请求的 request_id 重新包壳。** 这个 bug 本周拆分时顺手消灭。
7. **`storage/wal.py:23`**：`append` 直接往调用者传入的 `record` dict 里塞 `crc32` —— 副作用污染调用方数据。编码逻辑抽到 `wal_record.py` 后应该 copy 或只在编码时加。
8. **`storage/wal.py:16`**：每次 append 都重新 open 文件。第三周可以接受，但 batch 模式需要常驻文件句柄（不然 flush/fsync 没有意义），建议这次一起改成 open 一次、持有句柄。
9. **`rpc/server.py:129`**：`run()` 写死 host/port，不读 config；`app.py` 也没调 `load_config`。

---

## 3. 分步施工顺序

按依赖顺序做，每步做完都能跑、能测。

### Step 1：定义 Command（`kv/command.py`）

**为什么先做这个**：Command 是贯穿全系统的数据单元 —— client 发的是它、WAL 存的是它、状态机 apply 的是它、第 10 周 Raft log entry 里装的还是它。先把它定下来，后面所有接口都围绕它。

格式沿用 plan.md 的 WAL 记录：

```python
{
  "op": "Put",          # Put | Get | Delete
  "key": "x",
  "value": "1",         # Get/Delete 时可为 None
  "client_id": "c1",
  "seq": 1              # int！
}
```

你要做的：

- 提供构造函数（如 `make_put(key, value, client_id, seq)`），保证字段齐全、seq 是 int。
- 提供校验函数：op 合法、写命令必须带 client_id + seq。
- **决策点**：Get 要不要带 client_id/seq？→ 不需要。Get 不改状态、天然幂等、不写 WAL、不进去重表。想清楚为什么，写进 design.md。

### Step 2：拆出 ClientTable（`kv/client_table.py`）

把 `rpc/server.py` 里的 `dedup_table` 逻辑搬出来，变成独立类：

```python
class ClientTable:
    def check(self, client_id, seq) -> "duplicate" | "stale" | "new"
    def record(self, client_id, seq, result)   # result 是业务结果，不含 request_id！
    def to_dict() / from_dict()                # 第 4 周 snapshot 用，现在就留好口子
```

注意问题清单第 6 条：`record` 存的必须是 **result 本体**（比如 `"OK"` 或 value），不是完整 RPC 响应。

### Step 3：状态机（`kv/state_machine.py`）

用现有 `kv/store.py` 改造（改完可以删掉 store.py，别留两套）。核心变化：**从「put/get/delete 三个方法」变成「一个 apply」**：

```python
class KVStateMachine:
    def __init__(self):
        self.data = {}
        self.client_table = ClientTable()

    def apply(self, command: dict) -> dict:
        # 1. 写命令先查 client_table：duplicate → 直接返回 last_result
        # 2. 执行 op，改 self.data
        # 3. record 到 client_table
        # 4. 返回 result
```

关键设计约束：

- **apply 是唯一修改 data 和 client_table 的入口**。恢复时 replay 的记录也走 apply —— 这样 client_table 自动从 WAL 重建出来，「重启后仍能识别已处理请求」就是免费的。这是本周最重要的一个设计动作。
- apply **不碰 WAL**。写 WAL 是引擎（LocalEngine）的职责，状态机只管确定性地执行命令。为什么要这样分？想想第 10 周：Raft commit 之后 apply，WAL（Raft log）早就写完了。现在分对了，第 10 周不用动状态机。
- apply 里不需要锁：单个 asyncio 事件循环 + 无 await 的纯内存操作，天然串行。现在 server 里那把 `asyncio.Lock` 拆完后可以消失（想明白为什么，写进 design.md）。

### Step 4：ReplicationEngine 接口 + LocalEngine（`replication/base.py`、`replication/local.py`）

`base.py` 照抄 plan.md 2.2 节：

```python
class ReplicationEngine:
    async def start(self) -> None: ...
    async def submit(self, command: dict) -> dict: ...   # 写路径
    async def read(self, query: dict) -> dict: ...       # 读路径
    async def stop(self) -> None: ...
```

`LocalEngine` 的 `submit` 就是本周的核心流程：

```
1. 查 client_table（通过状态机暴露的接口）→ 重复则直接返回，不写 WAL
2. await wal.append(command)      # 写前日志：先落盘
3. result = state_machine.apply(command)
4. return result
```

`read` 直接查状态机内存，不写 WAL。

**决策点（写进 design.md）**：去重检查放在 WAL append 之前还是之后？之前 —— 重复请求不该再占一条 WAL。但要想清楚推论：replay 时同一 (client_id, seq) 不会在 WAL 里出现两次，所以 replay 走 apply 是安全的。

### Step 5：Server 瘦身（`rpc/server.py`）

Server 退化成纯传输 + 协议层：

- `__init__(self, engine: ReplicationEngine, config)` —— 依赖注入，不自己建 KV。
- `dispatch` 只做三件事：校验请求字段 → 把 RPC 请求翻译成 command → `await engine.submit(cmd)` 或 `engine.read(query)` → 把 result 包成响应（**request_id 用当前请求的**）。
- 删掉 hash_map、dedup_table、那把 Lock。
- `run()` 从 config 读 host/port。
- ping/echo 可以保留在 dispatch 层（它们不进引擎）。

改完后 Server 完全不知道 KV 和 WAL 的存在 —— 第 5 周把 `LocalEngine` 换成 `PrimaryBackupEngine` 时，这个文件一行都不用改。这就是验收的隐含标准。

### Step 6：启动恢复流程（`storage/recovery.py` + `app.py`）

`recovery.py` 提供一个函数，按 plan.md 的恢复流程编排：

```
1. 建空 KVStateMachine
2. wal.replay() 拿到有效记录（crc 校验 + 尾部截断已在 wal.py 里）
3. 逐条 state_machine.apply(record)     # 同一入口，client_table 一起恢复
4. 返回就绪的 state_machine
```

`app.py` 改成正式入口（顺手修语法错误）：

```
load_config → recovery 得到 state_machine → 组装 LocalEngine → Server(engine, cfg) → run
```

注意 `data/` 目录不存在时要先创建，否则 WAL 第一次 append 就崩。

### Step 7：WAL 补强（`storage/fsync.py`、`storage/wal_record.py`、batch 模式）

现有 wal.py 拆两块出去 + 加一个模式：

- **`wal_record.py`**：单条记录的 encode/decode（json + crc32 计算/校验）。从 wal.py 里抽出来，顺手解决问题 7（不污染调用方 dict）。wal.py 只管文件读写和 fsync 时机。
- **`fsync.py`**：封装 `await asyncio.to_thread(os.fsync, fd)`（你已经用对了，抽出来复用）。
- **`sync=batch`**：每 N 条**或**每 T 毫秒 fsync 一次。最简单的实现：
  - WAL 持有常驻文件句柄（解决问题 8）+ 一个未 fsync 计数器；
  - append 时 `write + flush`，计数 +1，满 N 条 fsync 并清零；
  - `start()` 里起一个后台协程每 T 毫秒 fsync 一次（兜底低流量场景）；
  - `stop()` 时最后 fsync 一次再关闭。
- **必须写文档**：batch 模式崩溃时最多丢多少？（最近一批未 fsync 的记录，上界 = N 条或 T 毫秒窗口内的写入）。这是验收标准第 2 条，写进 `docs/design.md`。

红线提醒：fsync 永远不能直接在事件循环里调，包括后台协程里。

### Step 8：故障测试（`tests/` + `test_report.md`）

plan.md 第 3 周的测试场景，每个都要做，每个都要留日志证据：

| # | 场景 | 验证什么 |
|---|---|---|
| 1 | Put 100 个 key → `kill -9` → 重启 → Get 全部验证 | sync=always 不丢数据 |
| 2 | 手动改坏 `data/wal.log` 最后一条（改一个字节）→ 重启 | 尾部截断，前面记录完好 |
| 3 | 重启后重发相同 client_id + seq | 恢复出的 client_table 命中去重，返回 last_result，不重复 apply |
| 4 | 同一 client 连续两次相同 seq 的 Put | 第二次返回缓存结果，WAL 里只有一条 |
| 5 | seq 回退 | 拒绝 |
| 6 | sync=batch 下 kill -9 | 观察丢了几条，和文档写的边界一致 |
| 7 | client 超时重试（可用 `asyncio.sleep` 在 server 端人为延迟）| 重试后最终成功且只 apply 一次 —— 这个测试会逼出问题 6 的 request_id bug |

测试形式：先写成 `tests/integration/` 下可重复运行的脚本（起 server 子进程 → 跑 client → kill → 重启 → 断言）。没有 pytest 也行，`python3 tests/integration/test_recovery.py` 退出码 0/1 即可，但必须**可从仓库根目录一键重跑**（红线 16）。

---

## 4. 提交节奏（module 前缀，不用 week 前缀）

对应 plan.md 第 9 节的 5 次提交：

```
1. replication: add ReplicationEngine interface and skeleton   # 骨架
2. kv: add command, state machine and client table            # 数据结构
3. replication: wire rpc -> local engine -> state machine -> wal   # 端到端打通
4. tests: add crash recovery and dedup fault tests            # 故障测试
5. storage: add batch fsync mode; docs: update design.md      # 修复+文档
```

周末打标签：`week-03-local-kv`。

## 5. 本周必须更新的文档

- `README.md`：怎么装、怎么启动 local 模式、怎么跑测试、支持/不支持什么（目前是空文件）。
- `docs/design.md`（新建，累积文档）：分层图、Command 格式、WAL 格式和 fsync 语义（always/batch 各自的持久化承诺）、去重设计（含 request_id 那个坑的分析）、恢复流程。
- `docs/milestones/week-03.md`：本周增量、测试结果、发现的 bug、遗留问题。

## 6. 自查清单（做完逐条打勾）

- [ ] `python3 scripts/start_local.py` 能从仓库根目录启动
- [ ] `rpc/server.py` 里没有任何 KV / 去重 / WAL 逻辑
- [ ] 状态机的 data 和 client_table 只被 `apply()` 修改
- [ ] replay 恢复走的是和正常写入同一个 `apply()`
- [ ] 重启后重发旧 (client_id, seq) 能拿到缓存结果
- [ ] seq 全链路是 int
- [ ] kill -9 + 重启 + WAL 尾部损坏三件套全部有日志证据
- [ ] batch 模式的丢失边界写进了 design.md
- [ ] 换掉 LocalEngine 不需要改 Server（用眼睛 review 依赖方向）
