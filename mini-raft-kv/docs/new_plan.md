# 分布式系统 0 基础 12 周执行计划：单项目迭代版

## 1. 总目标

本计划不是连续完成 12 个互不相关的小实验，而是在 12 周内持续开发、重构和增强同一个项目：

```text
mini-raft-kv
```

最终系统形态：

```text
client
  -> RPC gateway / router
  -> shard controller
  -> shard group
  -> Raft replication group
  -> replicated KV state machine
  -> WAL + snapshot
```

最小可交付形态：

```text
client -> 3/5 节点 Raft 集群 -> replicated KV store
```

完整可交付形态：

```text
client -> router -> 多个 shard group -> 每个 shard group 使用 Raft 复制
```

12 周内，所有代码都必须进入同一个仓库。每周不是新建一个 `weekXX` 项目，而是给现有系统增加一个可运行的里程碑。

最终能力目标：

```text
1. 理解网络不可靠、超时、重试、幂等和请求去重
2. 理解 WAL、fsync、snapshot 和 crash recovery
3. 理解 primary-backup 复制及其局限
4. 理解 stale read、read-your-writes 和 linearizability
5. 理解分片、路由表、shard controller 和数据迁移
6. 理解 Raft leader election、log replication、commit 和 apply
7. 能写延迟、丢包、宕机、重启、乱序和网络分区测试
8. 能把这些能力迁移到 IM、BlockServe 和 ClusterPilot
```

---

## 2. 单项目执行原则

### 2.1 只有一个仓库、一个主分支、一个持续演进的系统

整个计划只维护：

```text
mini-raft-kv/
```

禁止采用以下方式：

```text
week01_tcp/
week02_rpc/
week03_kv_wal/
...
```

每周完成后，系统仍然必须能从仓库根目录启动和测试。

每周新增的能力必须满足至少一项：

```text
1. 成为最终系统的正式模块
2. 成为最终系统的测试基础设施
3. 成为可通过统一接口替换的中间实现
4. 成为架构决策或故障实验文档
```

不能写完后丢弃。

---

### 2.2 用可替换接口保证前期代码能进入最终系统

复制层统一抽象为：

```python
class ReplicationEngine:
    async def start(self) -> None:
        ...

    async def submit(self, command: dict) -> dict:
        ...

    async def read(self, query: dict) -> dict:
        ...

    async def stop(self) -> None:
        ...
```

12 周内依次实现：

```text
LocalEngine
  -> PrimaryBackupEngine
  -> RaftEngine
```

三者不是三个项目，而是同一系统中同一接口的不同实现。

运行配置示例：

```yaml
replication:
  mode: local          # 第 3～4 周
  mode: primary_backup # 第 5～8 周
  mode: raft           # 第 9～12 周
```

最终默认使用：

```text
mode=raft
```

`local` 和 `primary_backup` 保留，用于对照测试和理解复制模型。

---

### 2.3 每周必须保持端到端可运行

每个周末，至少有一条完整链路可以运行：

```text
client -> RPC -> 当前复制引擎 -> KV 状态机 -> storage
```

不能出现以下状态：

```text
1. 模块分别能运行，但无法组合
2. 只写数据结构，没有启动入口
3. 只写单元测试，没有端到端测试
4. 为下一周大规模推倒重写
```

允许功能暂时不完整，但不允许主干长期不可运行。

---

### 2.4 每周通过里程碑标签保存历史

不创建 12 个目录，而是使用 Git 标签或里程碑分支保存阶段状态：

```text
milestone/week-01-transport
milestone/week-02-rpc
milestone/week-03-local-kv
...
milestone/week-12-release
```

推荐每周结束打标签：

```bash
git tag week-01
git tag week-02
...
git tag week-12
```

这样既保留学习过程，又不会把代码拆成 12 份。

---

## 3. 核心技术原则

### 3.1 少看视频，多写代码

资料使用比例：

```text
文档阅读：30%
代码实现：60%
测试验证：10%
```

视频只作为卡住时的辅助材料，不作为主线。

---

### 3.2 前 12 周统一使用 Python asyncio

不要一开始用 C++ 写 Raft。

原因：

```text
1. 当前目标是理解分布式系统，不是同时解决 C++ 异步网络问题
2. Raft 本身已经足够复杂，不应同时引入锁、线程和对象生命周期问题
3. Python asyncio 适合定时器、心跳、超时、故障注入和多节点模拟
4. Python 版本跑通后，再用 C++ 复刻核心模块，收益更高
```

语言路线：

```text
阶段 1：Python asyncio 跑通所有分布式逻辑
阶段 2：C++ 复刻 RPC、WAL 和 Raft 状态机
阶段 3：迁移到 BlockServe、ClusterPilot 或 IM 系统
```

---

### 3.3 节点内部固定采用 Actor 模型

所有 Raft 节点采用：

```text
单线程事件循环 + Actor 模型
```

每个节点一个 `asyncio.Queue`，所有事件都进入队列：

```python
class RaftNode:
    def __init__(self):
        self.inbox = asyncio.Queue()
        self.role = "Follower"
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = 0
        self.last_applied = 0

    async def run(self):
        while True:
            event = await self.inbox.get()
            await self.handle_event(event)
```

强制规则：

```text
1. Raft 状态只能在 handle_event 内修改
2. 不允许多个协程同时修改 current_term、log 和 commit_index
3. 不使用 threading.Lock
4. 不用多线程模拟 Raft 节点
5. 定时器只投递事件，不直接修改状态
```

统一事件类型：

```text
ClientRequest
RequestVote
RequestVoteResponse
AppendEntries
AppendEntriesResponse
ElectionTimeout
HeartbeatTick
Crash
Restart
```

---

### 3.4 RPC 统一使用 length-prefix JSON

从第 2 周开始，系统内部和客户端协议统一使用：

```text
[4 bytes big-endian length][json bytes]
```

示例：

```json
{
  "type": "RequestVote",
  "request_id": 1001,
  "from": "node1",
  "to": "node2",
  "term": 3,
  "payload": {
    "last_log_index": 10,
    "last_log_term": 2
  }
}
```

读取必须使用：

```python
header = await reader.readexactly(4)
size = int.from_bytes(header, "big")
body = await reader.readexactly(size)
```

禁止使用：

```python
header = await reader.read(4)
body = await reader.read(size)
```

原因：

```text
TCP 是字节流，没有消息边界。
reader.read(n) 不保证读满 n 个字节。
readexactly(n) 才能正确处理半包。
```

---

### 3.5 WAL 必须明确 fsync 语义

WAL 支持：

```text
sync=always
sync=batch
```

`sync=always`：

```text
write -> flush -> fsync -> 返回成功
```

语义：

```text
只要返回成功，重启后就不应丢失该记录。
```

`sync=batch`：

```text
每 N 条或每 T 毫秒 fsync 一次。
```

语义：

```text
性能更高，但崩溃时可能丢失最近一批未 fsync 的记录。
```

禁止在事件循环中直接调用：

```python
os.fsync(fd)
```

必须放入执行器：

```python
loop = asyncio.get_running_loop()
await loop.run_in_executor(None, os.fsync, fd)
```

否则可能阻塞心跳和选举定时器，导致 leader 误超时。

---

### 3.6 `client_table` 必须进入 snapshot

所有写请求使用：

```text
client_id + seq
```

去重表：

```text
client_table:
  client_id -> {
    last_seq,
    last_result
  }
```

snapshot 必须保存：

```text
1. KV 数据
2. client_table
3. last_applied
```

节点重启后仍必须能识别已经执行过的请求。

---

### 3.7 故障测试优先使用统一 Network Simulator

第 2 周开始实现应用层故障模拟，后续所有模块共同使用。

必须支持：

```text
delay
drop
reorder
partition
heal
crash
restart
```

真实网络测试放到第 12 周：

```text
1. macOS 本机不强制依赖 tc 或 iptables
2. Docker for Mac 网络经过虚拟化，行为可能与 Linux 不同
3. 真实网络故障测试优先使用 Linux VM 或 Linux 云服务器
4. 主验收以应用层 Network Simulator 为准
```

---

## 4. 最终仓库结构

```text
mini-raft-kv/
  README.md
  pyproject.toml
  docker-compose.yml

  config/
    local.yaml
    primary-backup.yaml
    raft-3.yaml
    raft-5.yaml
    sharded-raft.yaml

  src/
    mini_raft_kv/
      __init__.py
      app.py
      config.py

      client/
        client.py
        retry.py
        session.py

      rpc/
        codec.py
        protocol.py
        client.py
        server.py

      network/
        transport.py
        simulator.py
        fault_rule.py

      storage/
        wal.py
        wal_record.py
        snapshot.py
        fsync.py
        recovery.py

      kv/
        command.py
        state_machine.py
        versioned_value.py
        client_table.py

      replication/
        base.py
        local.py
        primary_backup.py

      raft/
        node.py
        role.py
        message.py
        log.py
        election.py
        replication.py
        storage.py

      sharding/
        hash_ring.py
        router.py
        shard_group.py
        controller.py
        migration.py

      observability/
        logger.py
        event.py
        metrics.py

  scripts/
    start_local.py
    start_cluster.py
    run_client.py
    inject_fault.py
    inspect_wal.py

  tests/
    unit/
      test_codec.py
      test_wal.py
      test_snapshot.py
      test_state_machine.py
      test_raft_log.py

    integration/
      test_local_kv.py
      test_primary_backup.py
      test_sharding.py
      test_raft_cluster.py
      test_restart_recovery.py

    fault/
      test_delay.py
      test_drop.py
      test_partition.py
      test_leader_crash.py
      test_duplicate_request.py
      test_corrupted_wal.py

  docs/
    design.md
    protocol.md
    consistency.md
    failure-cases.md
    project-mapping.md

    decisions/
      0001-actor-model.md
      0002-length-prefix-json.md
      0003-fsync-semantics.md
      0004-replication-engine.md
      0005-linearizable-read.md

    milestones/
      week-01.md
      week-02.md
      week-03.md
      week-04.md
      week-05.md
      week-06.md
      week-07.md
      week-08.md
      week-09.md
      week-10.md
      week-11.md
      week-12.md
```

关键要求：

```text
1. 每周只修改或新增上述目录中的模块
2. 不建立 week01、week02 等独立代码工程
3. 测试按 unit、integration、fault 分类，而不是按周分类
4. 每周总结放到 docs/milestones，不复制代码
5. 最终启动入口始终是 src/mini_raft_kv/app.py 或 scripts/start_cluster.py
```

---

## 5. 系统演进路线

### 5.1 数据链路始终保持一致

从第 3 周开始，统一请求链路为：

```text
Client
  -> RpcClient
  -> RpcServer
  -> Router
  -> ShardGroup
  -> ReplicationEngine
  -> KVStateMachine
  -> WAL / Snapshot
```

早期未实现的组件使用最简单实现：

```text
Router：单 shard 固定路由
ShardGroup：只有一个 group
ReplicationEngine：LocalEngine
```

后续只替换内部实现，不改变上层调用方式。

---

### 5.2 各阶段使用的复制引擎

```text
第 1～2 周：尚无复制层，完成传输基础设施
第 3～4 周：LocalEngine
第 5～8 周：PrimaryBackupEngine
第 9～12 周：RaftEngine
```

`PrimaryBackupEngine` 不删除，保留作为：

```text
1. 一致性对照实现
2. Raft 之前的中间里程碑
3. 故障测试基准
4. 解释 primary-backup 局限的实例
```

---

### 5.3 分片和 Raft 的最终组合

最终模型：

```text
Shard 0 -> Raft Group A: node1, node2, node3
Shard 1 -> Raft Group B: node2, node3, node4
Shard 2 -> Raft Group C: node3, node4, node5
```

学习阶段可以先让所有 shard group 复用同一组节点，但代码结构必须区分：

```text
shard_id
raft_group_id
node_id
config_id
```

---

## 6. 推荐资料

主线资料：

```text
1. DDIA 中文版：重点第 5、6、7、8、9 章
2. Raft 中文译文：重点 leader election、log replication、safety
3. MIT 6.5840 Labs：重点参考实验设计和测试方式
4. TiDB / TiKV 中文文档：理解 Region、Raft Group、PD 和调度
5. Jepsen Consistency Models：查 linearizability、sequential consistency 和 eventual consistency
```

前期不建议直接阅读：

```text
1. Kubernetes 全源码
2. Kafka 全源码
3. TiKV 全源码
4. etcd 全源码
5. Paxos 原论文
6. Spanner、Dynamo、Chubby 原论文
```

当前阶段先把同一个系统写完整。

---

# 7. 12 周单项目里程碑

## 第 1 周：建立项目骨架和 TCP 传输层

### 本周在大项目中的位置

建立最终仓库、启动入口、配置系统、日志系统和 TCP transport。Echo 只是验证传输层，不单独成为项目。

### 新增或修改模块

```text
src/mini_raft_kv/app.py
src/mini_raft_kv/config.py
src/mini_raft_kv/rpc/server.py
src/mini_raft_kv/rpc/client.py
src/mini_raft_kv/observability/logger.py
scripts/start_local.py
tests/integration/test_transport.py
```

### 代码任务

实现 `asyncio.start_server` TCP 服务，初期可用换行分隔协议验证链路：

```text
request_id=1 body=hello\n
```

返回：

```text
request_id=1 ok=true body=hello\n
```

要求：

```text
1. 支持多个 client
2. 一个 client 卡住不能阻塞其他 client
3. 每个请求带 request_id
4. server 日志包含 remote_addr、request_id 和 body
5. client 支持 timeout
6. 连接关闭和异常不能导致进程崩溃
7. 从仓库根目录可以启动 server 和 client
```

### 故障测试

```text
1. client 连接后立刻断开
2. client 发一半数据后断开
3. server 延迟 5 秒返回
4. client timeout 后退出
5. 多个 client 并发请求
```

### 周末系统状态

```text
client -> TCP transport -> echo handler
```

### 验收标准

```text
1. 主项目骨架固定下来
2. server 不因异常连接崩溃
3. client timeout 能正确报错
4. 多 client 并发正常
5. 日志可以按 request_id 追踪请求
```

---

## 第 2 周：把传输层升级为正式 RPC 和故障模拟基础设施

### 本周在大项目中的位置

将第 1 周的临时换行协议替换为最终使用的 length-prefix JSON。Network Simulator 从本周开始成为所有集成测试的公共基础设施。

### 新增或修改模块

```text
src/mini_raft_kv/rpc/codec.py
src/mini_raft_kv/rpc/protocol.py
src/mini_raft_kv/rpc/client.py
src/mini_raft_kv/rpc/server.py
src/mini_raft_kv/network/transport.py
src/mini_raft_kv/network/simulator.py
src/mini_raft_kv/client/retry.py
src/mini_raft_kv/client/session.py
```

### 代码任务

实现：

```text
1. length-prefix JSON 编解码
2. RpcClient
3. RpcServer
4. timeout
5. retry
6. client_id + seq
7. Network Simulator 雏形
```

请求格式：

```json
{
  "request_id": 1,
  "client_id": "c1",
  "seq": 1,
  "method": "Put",
  "params": {
    "key": "x",
    "value": "1"
  }
}
```

返回格式：

```json
{
  "request_id": 1,
  "ok": true,
  "result": "OK",
  "error": ""
}
```

Network Simulator 接口：

```python
class Network:
    async def send(self, src, dst, message):
        ...

    def drop(self, src, dst):
        ...

    def delay(self, src, dst, ms):
        ...

    def recover(self, src, dst):
        ...

    def partition(self, group_a, group_b):
        ...

    def heal(self):
        ...
```

### 故障测试

```text
1. 半包
2. 粘包
3. 消息丢失
4. 500ms 延迟
5. client timeout 后 retry
6. server 执行成功但响应丢失
7. 重复 request_id 和重复 client_id + seq
```

### 周末系统状态

```text
client -> length-prefix JSON RPC -> test handler
                 |
                 -> Network Simulator
```

### 验收标准

```text
1. 正确处理半包和粘包
2. timeout 后可以 retry
3. retry 不改变 client_id + seq
4. Network Simulator 可以模拟 delay 和 drop
5. 后续模块不再自建 RPC 协议
```

---

## 第 3 周：接入单机 KV、状态机和 WAL

### 本周在大项目中的位置

把 RPC handler 替换为正式 KV 服务。实现最终系统会继续使用的 `KVStateMachine`、`WAL` 和 `LocalEngine`。

### 新增或修改模块

```text
src/mini_raft_kv/kv/command.py
src/mini_raft_kv/kv/state_machine.py
src/mini_raft_kv/kv/client_table.py
src/mini_raft_kv/storage/wal.py
src/mini_raft_kv/storage/wal_record.py
src/mini_raft_kv/storage/fsync.py
src/mini_raft_kv/storage/recovery.py
src/mini_raft_kv/replication/base.py
src/mini_raft_kv/replication/local.py
```

### 对外接口

```text
Put(key, value)
Get(key)
Delete(key)
```

### WAL 记录

```json
{
  "crc32": 123456,
  "record": {
    "op": "Put",
    "key": "x",
    "value": "1",
    "client_id": "c1",
    "seq": 1
  }
}
```

### 必须实现

```text
1. WAL append
2. WAL replay
3. WAL checksum
4. WAL 尾部损坏时自动截断
5. sync=always
6. sync=batch
7. client_id + seq 去重
8. LocalEngine 实现 ReplicationEngine 接口
9. RPC 请求通过 LocalEngine 提交到 KVStateMachine
```

### 启动恢复流程

```text
1. 初始化空状态机
2. 读取 wal.log
3. 校验 crc32
4. 顺序 replay
5. 遇到最后一条损坏记录时截断
6. 恢复 KV 和 client_table
7. 启动 RPC 服务
```

### 故障测试

```text
1. Put 100 个 key
2. kill server
3. 重启 server
4. Get 并验证 100 个 key
5. 手动破坏 WAL 最后一条记录
6. 重启后恢复前面的完整记录
7. 重发相同 client_id + seq
```

### 周末系统状态

```text
client -> RPC -> LocalEngine -> KVStateMachine -> WAL
```

### 验收标准

```text
1. sync=always 下成功返回的数据重启不丢
2. sync=batch 的数据丢失边界有文档说明
3. WAL 尾部损坏不影响前面完整记录
4. 重复请求不会重复 apply
5. 第 2 周 RPC 和故障模拟代码被直接复用
```

---

## 第 4 周：在同一 KV 引擎中加入 Snapshot、Version 和 CAS

### 本周在大项目中的位置

增强现有状态机和存储层，不新建第二套 KV。

### 新增或修改模块

```text
src/mini_raft_kv/kv/versioned_value.py
src/mini_raft_kv/kv/state_machine.py
src/mini_raft_kv/storage/snapshot.py
src/mini_raft_kv/storage/recovery.py
```

### Value 结构

```python
{
  "data": "value",
  "version": 3
}
```

### 新增接口

```text
CAS(key, expected_version, new_value)
Snapshot()
LoadSnapshot()
```

### Snapshot 内容

```json
{
  "last_applied": 1024,
  "kv": {
    "x": {
      "data": "1",
      "version": 3
    }
  },
  "client_table": {
    "c1": {
      "last_seq": 12,
      "last_result": "OK"
    }
  }
}
```

### 正确流程

```text
1. write snapshot.tmp
2. fsync snapshot.tmp
3. rename snapshot.tmp -> snapshot.dat
4. fsync directory
5. truncate 或滚动 wal.log
```

### 故障测试

```text
1. 写入 3000 次
2. 生成 snapshot
3. kill server
4. 重启后恢复
5. snapshot 写到一半 kill
6. 验证旧 snapshot 不被破坏
7. 从 snapshot 恢复 client_table 后重试旧请求
```

### 周末系统状态

```text
client -> RPC -> LocalEngine -> versioned KV -> WAL + snapshot
```

### 验收标准

```text
1. version 单调递增
2. CAS 版本不匹配时失败
3. snapshot + WAL 可以恢复完整状态
4. snapshot 包含 client_table
5. snapshot 过程中崩溃不破坏旧 snapshot
```

---

## 第 5 周：给同一系统接入 Primary-BackupEngine

### 本周在大项目中的位置

不另写一个 primary-backup 项目，而是在 `ReplicationEngine` 接口下新增 `PrimaryBackupEngine`，复用 RPC、WAL、状态机、去重表和故障模拟器。

### 新增或修改模块

```text
src/mini_raft_kv/replication/primary_backup.py
src/mini_raft_kv/config.py
config/primary-backup.yaml
scripts/start_cluster.py
tests/integration/test_primary_backup.py
```

### 结构

```text
client -> primary -> backup
```

### 写入流程

```text
1. client 把 Put 发给 primary
2. primary 把操作记录到本地持久化层
3. primary 把操作转发给 backup
4. backup 写 WAL
5. backup fsync
6. backup 返回 ack
7. primary 完成本地提交并向 client 返回成功
```

必须在 `docs/design.md` 中明确：

```text
1. primary 本地 WAL 和 backup WAL 的先后顺序
2. primary 崩溃时哪些状态可能出现
3. 为什么没有自动选主时它不是完整高可用系统
4. client 成功响应对应什么持久化语义
```

### 必须实现

```text
1. primary 到 backup 的内部 RPC
2. backup ack
3. primary 等待 backup ack
4. 两端都使用现有 WAL
5. 两端都使用同一个 KVStateMachine
6. client_id + seq 去重
7. 通过配置切换 local 和 primary_backup
```

### 故障测试

```text
1. backup 正常时写入成功
2. backup 挂掉时 primary 返回错误或不可用
3. backup 写 WAL 前崩溃时不能返回成功
4. backup ack 丢失后 client 重试不重复写
5. backup 重启后从 WAL 恢复
```

### 周末系统状态

```text
client -> RPC -> PrimaryBackupEngine
                     -> primary state machine + WAL
                     -> backup state machine + WAL
```

### 验收标准

```text
1. local 和 primary_backup 使用同一套外部 API
2. primary 和 backup 数据一致
3. backup 挂掉时不伪造写入成功
4. client 重试不会重复 apply
5. 第 3～4 周状态机和存储层不复制代码
```

---

## 第 6 周：在现有 Primary-Backup 系统上完成一致性实验

### 本周在大项目中的位置

不另写 consistency lab。给现有客户端和复制引擎增加可配置读策略，并把实验脚本保留为回归测试和性能分析工具。

### 新增或修改模块

```text
src/mini_raft_kv/replication/primary_backup.py
src/mini_raft_kv/client/client.py
scripts/run_consistency_experiment.py
tests/integration/test_read_consistency.py
docs/consistency.md
```

### 支持的读模式

```text
read_from_primary
read_from_backup
read_from_random
```

复制延迟：

```text
0ms
100ms
1s
5s
```

实验过程：

```text
1. Put x=i
2. 立刻 Get x
3. 重复 1000 次
4. 统计旧值次数
5. 输出 CSV 或 JSON 报告
```

### 测试矩阵

```text
读 primary × 0ms / 100ms / 1s / 5s
读 backup × 0ms / 100ms / 1s / 5s
随机读 × 0ms / 100ms / 1s / 5s
```

### 文档内容

`docs/consistency.md` 必须回答：

```text
1. 哪种模式能读到最新值
2. 哪种模式可能读到旧值
3. 延迟增大时旧读比例如何变化
4. 为什么 backup read 不一定线性一致
5. 什么是 read-your-writes
6. 为什么强一致读通常更贵
7. 当前 primary-backup 实现具有什么一致性语义
```

### 周末系统状态

```text
同一个 primary-backup KV
  + 多种读策略
  + 可重复一致性实验
  + 自动统计结果
```

### 验收标准

```text
1. 能稳定复现 stale read
2. 能解释 read-your-writes
3. 能解释 eventual consistency
4. 实验脚本成为 tests 或 scripts 的长期组成部分
5. 后续 Raft 线性一致读可与本周结果对照
```

---

## 第 7 周：在同一系统外围加入 Router 和固定分片

### 本周在大项目中的位置

在现有 KV 服务前增加路由层。每个 shard 仍然使用 `PrimaryBackupEngine`，后续再无缝换成 `RaftEngine`。

### 新增或修改模块

```text
src/mini_raft_kv/sharding/hash_ring.py
src/mini_raft_kv/sharding/router.py
src/mini_raft_kv/sharding/shard_group.py
src/mini_raft_kv/app.py
config/sharded-primary-backup.yaml
```

### 初始模型

```text
shard_id = hash(key) % 4

node1: shard 0, 1
node2: shard 2, 3
```

路由表：

```json
{
  "config_id": 1,
  "shards": {
    "0": "group1",
    "1": "group1",
    "2": "group2",
    "3": "group2"
  }
}
```

每个 shard group 内部当前使用：

```text
PrimaryBackupEngine
```

请求必须带：

```text
config_id
```

错误节点返回：

```json
{
  "ok": false,
  "error": "WrongShard"
}
```

### 必须实现

```text
1. client 缓存 config
2. Put/Get 根据 hash(key) 路由
3. server 校验 shard 所属关系
4. server 校验 config_id
5. WrongShard 后刷新路由表
6. config_id 单调递增
7. 单 shard 模式仍可运行
```

### 周末系统状态

```text
client -> router -> shard group -> PrimaryBackupEngine -> KV/WAL
```

### 验收标准

```text
1. Put/Get 路由到正确 shard group
2. 错误节点返回 WrongShard
3. client 使用旧配置时能刷新
4. 路由层不直接依赖 primary-backup 细节
5. 后续 shard group 可以替换为 RaftEngine
```

---

## 第 8 周：加入 Shard Controller 和在线迁移

### 本周在大项目中的位置

继续扩展同一项目的控制面。Controller 管理现有 shard group，不创建新的独立服务仓库。

### 新增或修改模块

```text
src/mini_raft_kv/sharding/controller.py
src/mini_raft_kv/sharding/migration.py
src/mini_raft_kv/sharding/shard_group.py
scripts/inject_fault.py
tests/integration/test_shard_migration.py
```

### Controller 接口

```text
Join(node_id)
Leave(node_id)
Move(shard_id, group_id)
QueryConfig()
```

配置格式：

```json
{
  "config_id": 3,
  "shards": {
    "0": "group1",
    "1": "group2",
    "2": "group2",
    "3": "group3"
  }
}
```

### shard 状态

```text
Serving
Pulling
Pushing
Migrating
```

### 迁移流程

```text
1. controller 生成迁移计划
2. 新 owner 进入 Pulling
3. 旧 owner 进入 Pushing
4. 新 owner 拉取 shard 数据和 client_table 子集
5. 新 owner 完成导入
6. controller 发布新 config
7. 新 owner 进入 Serving
8. 旧 owner 冻结或删除旧 shard
```

核心约束：

```text
迁移期间不能让两个 owner 同时接受同一个 shard 的写入。
```

### 故障测试

```text
1. 新 group 加入
2. 将 shard 3 从 group2 迁移到 group3
3. 迁移过程中持续 Put/Get
4. 迁移过程中杀死新 owner
5. 迁移过程中丢失确认消息
6. 验证数据不丢、不会双写
```

### 周末系统状态

```text
client -> router -> shard controller
                 -> multiple shard groups
                 -> PrimaryBackupEngine
```

### 验收标准

```text
1. config_id 单调递增
2. client 能感知配置变化
3. 迁移后数据仍可读
4. 不出现同一 shard 双 owner 写入
5. 旧 config 请求被拒绝或重定向
6. 迁移代码不依赖具体复制引擎
```

---

## 第 9 周：在 ReplicationEngine 下实现 Raft Leader Election

### 本周在大项目中的位置

新增 `RaftEngine`，先实现选主。现有 Router、ShardGroup、RPC、Network Simulator 和配置系统直接复用。

### 新增或修改模块

```text
src/mini_raft_kv/raft/node.py
src/mini_raft_kv/raft/role.py
src/mini_raft_kv/raft/message.py
src/mini_raft_kv/raft/election.py
src/mini_raft_kv/replication/base.py
config/raft-3.yaml
config/raft-5.yaml
```

### 节点状态

```python
class Role:
    FOLLOWER = "Follower"
    CANDIDATE = "Candidate"
    LEADER = "Leader"
```

核心字段：

```text
current_term
voted_for
role
```

### RPC

```text
RequestVote
RequestVoteResponse
AppendEntriesHeartbeat
AppendEntriesResponse
```

### 时间参数

```text
heartbeat interval: 100ms
election timeout: 300ms～600ms 随机
rpc timeout: 200ms～500ms
```

### 规则

```text
1. election timeout 必须随机
2. election timeout 必须大于 heartbeat interval
3. 收到合法 heartbeat 后重置 election timeout
4. 收到更高 term 的消息立即退回 follower
5. 每个 term 最多投一票
6. 定时器只向 inbox 投递事件
7. handle_event 是唯一状态修改入口
```

### 故障测试

```text
1. 3 节点最终只有一个 leader
2. kill leader 后剩余节点重新选主
3. 5 节点 kill leader 后重新选主
4. 分区 [node1] | [node2, node3]
5. 分区恢复后最终只有一个 leader
6. 延迟 RequestVote 和 heartbeat
```

### 周末系统状态

```text
RaftEngine 可以启动 3/5 节点并稳定选主，
但暂时不能提交 KV 命令。
```

### 验收标准

```text
1. 3 节点 5 秒内选出 leader
2. kill leader 后 5 秒内重新选主
3. 同一 term 不出现两个 leader
4. 少数派不能形成可提交请求的 leader
5. 日志能看到 term、role 和投票变化
6. 所有测试使用第 2 周 Network Simulator
```

---

## 第 10 周：完成 Raft 日志复制、提交和 Apply

### 本周在大项目中的位置

让 `RaftEngine` 真正实现 `submit(command)`。提交后的 command 进入第 3 周写出的同一个 `KVStateMachine`。

### 新增或修改模块

```text
src/mini_raft_kv/raft/log.py
src/mini_raft_kv/raft/replication.py
src/mini_raft_kv/raft/node.py
src/mini_raft_kv/replication/base.py
tests/integration/test_raft_cluster.py
```

### 日志结构

```python
{
  "term": 3,
  "index": 10,
  "command": {
    "op": "Put",
    "key": "x",
    "value": "1",
    "client_id": "c1",
    "seq": 12
  }
}
```

### 必须实现

```text
1. client 请求只能由 leader 接受
2. follower 返回 NotLeader 和 leader_hint
3. leader append log
4. leader 发送 AppendEntries
5. follower 校验 prev_log_index / prev_log_term
6. follower 删除冲突日志
7. 多数复制后 leader 推进 commit_index
8. committed entry 按 index 顺序 apply
```

### 必须维护

```text
commit_index
last_applied
next_index[follower]
match_index[follower]
```

### 必须打印的事件

```text
leader append entry
send AppendEntries
follower reject conflict
follower truncate log
leader advance commit_index
node apply entry
```

### 故障测试

```text
1. follower 掉线后重新上线
2. follower 日志落后后追赶
3. leader 宕机后新 leader 接管
4. follower 冲突日志被修正
5. 丢包后最终日志收敛
6. 少数派不能 commit
```

### 周末系统状态

```text
client -> RPC -> RaftEngine -> committed log -> KVStateMachine
```

暂时可以不保证完整磁盘恢复，但内存中的复制、commit 和 apply 必须正确。

### 验收标准

```text
1. 多数派复制后才能 commit
2. follower 日志最终与 leader 收敛
3. committed entry 不会被覆盖
4. apply 顺序严格等于 log index 顺序
5. commit_index 和 last_applied 正确分离
6. KVStateMachine 没有为 Raft 重新实现一份
```

---

## 第 11 周：Raft 持久化、Snapshot 和线性一致 Raft KV

### 本周在大项目中的位置

将第 3～4 周的 WAL、snapshot、client_table 与第 9～10 周的 RaftEngine 合并，形成真正可恢复的 replicated KV。

### 新增或修改模块

```text
src/mini_raft_kv/raft/storage.py
src/mini_raft_kv/raft/node.py
src/mini_raft_kv/storage/wal.py
src/mini_raft_kv/storage/snapshot.py
src/mini_raft_kv/kv/client_table.py
src/mini_raft_kv/sharding/shard_group.py
config/sharded-raft.yaml
```

### Raft 必须持久化

```text
current_term
voted_for
log_entries
```

`sync=always` 下：

```text
返回成功前，相关 Raft 日志必须 fsync。
```

### 支持命令

```text
Put
Get
Delete
CAS
```

命令格式：

```json
{
  "op": "Put",
  "key": "x",
  "value": "1",
  "client_id": "c1",
  "seq": 12
}
```

### Get 策略

本阶段 `Get` 也走 Raft 日志：

```text
1. 保证线性一致读
2. 暂不引入 lease read 或 read index
3. 降低额外协议复杂度
```

### client_table 规则

```text
1. apply 前检查 client_id + seq
2. 已处理请求直接返回 last_result
3. 新 seq 执行命令并更新 client_table
4. client_table 进入 snapshot
5. snapshot 安装后恢复 client_table
```

### Raft snapshot 内容

```json
{
  "last_included_index": 1024,
  "last_included_term": 8,
  "last_applied": 1024,
  "kv": {
    "x": {
      "data": "1",
      "version": 3
    }
  },
  "client_table": {
    "c1": {
      "last_seq": 12,
      "last_result": "OK"
    }
  }
}
```

### 集成要求

```text
1. ShardGroup 的复制引擎从 primary_backup 切换为 raft
2. Router 和 Controller API 不改变
3. 单 shard Raft 和多 shard Raft 都能通过配置启动
4. follower 可以通过日志或 snapshot 追赶
```

### 故障测试

```text
1. kill leader 后 client 重试同一个请求
2. follower 重启后追赶日志
3. 所有节点 kill -9 后重启
4. 重启后 committed 数据不丢
5. snapshot 后重启仍能去重
6. 落后过多的 follower 安装 snapshot
```

### 周末系统状态

```text
client -> router -> shard group -> RaftEngine
                                  -> replicated KV
                                  -> WAL + snapshot
```

### 验收标准

```text
1. committed log 不丢
2. 请求不会重复 apply
3. leader 切换后数据一致
4. 多数派存活时继续工作
5. Get 能看到最新 committed 写入
6. snapshot 恢复后 client_table 仍有效
7. 分片层已经使用 RaftEngine，而不是另建最终项目
```

---

## 第 12 周：全系统故障注入、Docker 化和发布整理

### 本周在大项目中的位置

不再开发新的实验项目，而是对同一个 `mini-raft-kv` 做系统级测试、配置收敛、文档整理和发布验收。

### Network Simulator 必须支持

```text
delay
drop
reorder
partition
heal
crash
restart
```

### 必测分区

3 节点：

```text
[node1] | [node2, node3]
```

5 节点：

```text
[node1, node2] | [node3, node4, node5]
```

### 系统级测试

```text
1. 少数派不能 commit
2. 多数派可以 commit
3. 分区恢复后日志收敛
4. kill leader 后重新选主
5. 所有节点重启后数据不丢
6. client 重试不会重复写
7. follower 通过 snapshot 追赶
8. shard 迁移期间不双写
9. 多 shard 路由在配置变更后收敛
10. 测试可重复运行且结果稳定
```

### Docker

`docker-compose.yml` 至少支持：

```text
node1
node2
node3
client
```

可选扩展：

```text
node4
node5
controller
```

Linux 环境可选测试：

```text
1. iptables 阻断节点通信
2. tc netem 注入 200ms 延迟
3. tc netem 注入 10% 丢包
4. docker kill leader
5. docker restart follower
```

### 最终文档

`docs/design.md`：

```text
1. 总体架构
2. 单项目演进路线
3. RPC 协议
4. ReplicationEngine 抽象
5. Raft 状态机
6. WAL 和 snapshot
7. client 去重
8. 分片和迁移
9. 故障恢复流程
```

`docs/failure-cases.md`：

```text
1. leader 宕机
2. follower 宕机
3. 网络分区
4. AppendEntries 丢包
5. RequestVote 延迟
6. WAL 尾部损坏
7. snapshot 写一半崩溃
8. client 请求重复
9. shard 迁移中断
10. 旧 config 请求
```

`docs/project-mapping.md`：

```text
BlockServe：
- worker heartbeat
- task lease
- retry
- idempotent task result
- scheduler recovery

ClusterPilot：
- desired state
- actual state
- controller reconcile
- leader election
- config version

IM 系统：
- message_id
- conversation sequence
- duplicate delivery removal
- offline replay
- fanout
- read receipt consistency
```

### 最终验收标准

```text
1. 从同一仓库启动 3 节点 Raft KV
2. 从同一仓库启动 5 节点 Raft KV
3. kill leader 后系统继续服务
4. 网络分区时少数派不能写成功
5. 多数派可以继续提交
6. 分区恢复后节点状态一致
7. client 重试不会重复写
8. 重启后 committed 数据不丢
9. snapshot 后 client_table 仍有效
10. 固定分片和路由可以运行
11. shard 迁移测试可以运行
12. 所有测试可以从仓库根目录统一执行
```

---

# 8. 每周固定工作流程

每周围绕同一个仓库执行：

```text
周一：阅读资料，更新 docs/milestones/week-XX.md 和设计约束
周二：在现有主干上写最小增量
周三：打通端到端链路
周四：补单元、集成和故障测试
周五：修复问题并保持主干可运行
周六：重构重复代码，更新 design.md
周日：完成里程碑报告并打 Git 标签
```

每周必须更新：

```text
README.md
docs/design.md
docs/milestones/week-XX.md
```

`README.md` 始终描述当前整个项目：

```text
1. 怎么安装
2. 怎么启动当前模式
3. 怎么运行全部测试
4. 当前支持哪些配置
5. 当前不支持什么
```

`docs/design.md` 是累积文档，不按周复制：

```text
1. 总体架构
2. 核心数据结构
3. 请求流程
4. 故障处理
5. 一致性语义
6. 当前设计决策
```

`docs/milestones/week-XX.md` 只记录本周增量：

```text
1. 本周增加了什么
2. 改动了哪些现有模块
3. 端到端链路如何变化
4. 测试结果
5. 发现的 bug
6. 尚未解决的问题
7. 下一周如何在此基础上继续
```

---

# 9. 代码提交标准

每周至少 5 次有效提交，但提交对象始终是同一个项目：

```text
commit 1：接口或项目骨架调整
commit 2：核心数据结构
commit 3：端到端流程跑通
commit 4：故障测试
commit 5：修复、重构和文档
```

提交信息示例：

```text
storage: add wal append and checksum
storage: recover state machine from wal
replication: add primary-backup engine
raft: implement request-vote handling
raft: advance commit index after quorum
sharding: reject stale config requests
tests: add leader partition recovery case
```

不再使用：

```text
week03: ...
week04: ...
```

因为代码属于长期模块，不属于一次性周实验。

每周结束可以打标签：

```text
week-03-local-kv
week-05-primary-backup
week-08-sharding
week-11-raft-kv
week-12-release
```

---

# 10. 单项目执行红线

必须遵守：

```text
1. 不创建 12 个独立代码项目
2. 不复制 RPC、WAL、状态机或测试框架
3. 不为 primary-backup 和 Raft 分别实现两套 KV
4. 所有复制模型实现同一个 ReplicationEngine 接口
5. 每周结束时主干必须可以运行
6. 不使用 threading 写 Raft
7. 不在第 9 周前引入 C++
8. 不在 Raft 跑通前引入 gRPC
9. 不使用固定 election timeout
10. 不允许多个协程同时修改 Raft 状态
11. 不在事件循环中直接调用 os.fsync()
12. length-prefix 读取必须使用 readexactly
13. WAL 成功返回前必须明确 fsync 语义
14. 所有 client 写请求必须有 client_id + seq
15. client_table 必须进入 snapshot
16. 所有测试必须可以从仓库根目录运行
17. 每个故障场景必须有日志证明
18. 不允许为了下一周推倒重写上一周代码
19. 临时实现必须通过接口隔离并注明替换计划
20. 每周必须新增至少一个端到端回归测试
```

---

# 11. 不要提前做的事

前 12 周不要做：

```text
1. 不读 Kubernetes 全源码
2. 不读 Kafka 全源码
3. 不读 TiKV 全源码
4. 不做复杂服务发现
5. 不做 gRPC 框架封装
6. 不做监控大盘
7. 不做 Web UI
8. 不做分布式事务
9. 不做 Paxos
10. 不做多数据中心
11. 不做 lease read 优化
12. 不做 follower read 优化
13. 不把项目拆成多个微服务仓库
14. 不为了“看起来完整”提前增加无关基础设施
```

---

# 12. 时间不足时的单项目压缩路线

如果时间不足，不要改成 6 个小项目，而是在同一个仓库内只完成以下主链路：

```text
里程碑 1：length-prefix JSON RPC + timeout + retry
里程碑 2：KVStateMachine + WAL + fsync + snapshot
里程碑 3：PrimaryBackupEngine
里程碑 4：Raft leader election
里程碑 5：Raft log replication
里程碑 6：Raft KV + client_table + fault injection
```

可以暂缓：

```text
1. shard controller
2. 在线数据迁移
3. 多 Raft Group
4. Docker 真实网络故障
```

但仍保持：

```text
同一仓库
同一 RPC
同一状态机
同一存储层
同一故障测试框架
```

---

# 13. 完成后的能力边界

完成后应能理解并设计：

```text
1. 为什么 Kafka 使用 partition 和 offset
2. 为什么 TiKV 使用 Region 和 Raft Group
3. 为什么 etcd 可以作为强一致元数据存储
4. 为什么 IM 系统需要 message_id、seq、去重和重放
5. 为什么 BlockServe 需要 heartbeat、lease 和任务幂等
6. 为什么 ClusterPilot 需要 desired state、actual state 和 reconcile
7. 为什么 WAL 需要 fsync
8. 为什么 Raft 需要多数派
9. 为什么少数派分区不能提交
10. 为什么 client retry 必须配合去重表
11. 为什么分片层和复制层应通过接口解耦
12. 为什么一个大项目更需要稳定边界和持续重构
```

暂时不要求掌握：

```text
1. 分布式事务
2. MVCC
3. lease read
4. read index
5. gossip
6. 多数据中心一致性
7. Kubernetes controller-runtime 源码
8. TiKV、etcd、Kafka 全源码
```

---

# 14. 最终项目描述

英文描述：

```text
A single evolving distributed key-value store project that integrates length-prefixed JSON RPC, pluggable replication engines, WAL, snapshots, request deduplication, sharding, Raft consensus, linearizable reads, crash recovery, and fault-injection tests.
```

中文描述：

```text
一个在 12 周内持续演进的分布式 KV 项目。系统从基础 TCP 和单机持久化开始，逐步接入 primary-backup、分片、Raft、WAL、snapshot、请求去重、线性一致读、故障恢复和网络分区测试；所有阶段共享同一仓库、同一状态机、同一存储层和同一测试基础设施。
```

最终目标不是完成 12 个玩具 demo，而是完成一个架构边界清晰、可以持续重构、可以端到端运行、可以注入故障并验证语义的最小分布式数据库。
