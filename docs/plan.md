# 分布式系统 0 基础 12 周执行计划：最终版

## 1. 总目标

本计划的目标不是泛泛学习分布式系统理论，而是用 12 周写出一个可运行、可测试、可故障恢复的分布式 KV 系统：

```text
client -> Raft 集群 -> replicated KV store
```

最终项目名建议：

```text
mini-raft-kv
```

最终能力目标：

```text
1. 理解网络不可靠、超时、重试、幂等、去重
2. 理解 WAL、fsync、snapshot、crash recovery
3. 理解 primary-backup 复制
4. 理解 stale read、read-your-writes、linearizability
5. 理解分片、路由表、shard controller、数据迁移
6. 理解 Raft leader election、log replication、commit、apply
7. 能写故障注入测试：延迟、丢包、宕机、重启、网络分区
8. 能把这些能力迁移到 IM、BlockServe、ClusterPilot
```

---

## 2. 核心执行原则

### 2.1 少看视频，多写代码

资料使用比例：

```text
文档阅读：30%
代码实现：60%
测试验证：10%
```

视频只作为卡住时的辅助材料，不作为主线。

---

### 2.2 前 12 周统一使用 Python asyncio

不要一开始用 C++ 写 Raft。

原因：

```text
1. 当前阶段目标是理解分布式系统，不是挑战 C++ 异步网络
2. Raft 本身已经足够复杂，不应该同时引入锁、线程、对象生命周期问题
3. Python asyncio 适合写定时器、心跳、超时、故障注入、多节点模拟
4. 等 Python 版本跑通后，再用 C++ 复刻核心模块，收益更高
```

语言路线：

```text
第 1 阶段：Python asyncio 跑通所有分布式逻辑
第 2 阶段：C++ 复刻 RPC、WAL、Raft 状态机
第 3 阶段：迁移到 BlockServe / ClusterPilot / IM 系统
```

---

### 2.3 并发模型固定为 Actor 模型

所有 Raft 节点采用：

```text
单线程事件循环 + Actor 模型
```

每个节点一个 `asyncio.Queue`，所有事件都进入队列。

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
2. 不允许多个协程同时修改 current_term、log、commit_index
3. 不使用 threading.Lock
4. 不用多线程模拟 Raft 节点
5. 定时器只负责投递事件，不直接修改状态
```

所有事件统一抽象成消息：

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

### 2.4 RPC 使用 length-prefix JSON

从第 2 周开始，统一使用：

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

读取时必须使用：

```python
header = await reader.readexactly(4)
size = int.from_bytes(header, "big")
body = await reader.readexactly(size)
```

禁止这样写：

```python
header = await reader.read(4)
body = await reader.read(size)
```

原因：

```text
TCP 是字节流，没有消息边界。
reader.read(n) 不保证读满 n 个字节。
readexactly(n) 才能正确处理半包问题。
```

---

### 2.5 WAL 必须明确 fsync 语义

WAL 支持两种模式：

```text
sync=always
sync=batch
```

#### sync=always

每条 WAL 写入后：

```text
write -> flush -> fsync
```

语义：

```text
只要返回成功，重启后就不应该丢失该记录。
```

#### sync=batch

每 N 条或每 T 毫秒 fsync 一次。

语义：

```text
性能更高，但崩溃时可能丢失最近一批未 fsync 的记录。
```

重要规则：

```text
不要在 asyncio 事件循环里直接调用 os.fsync()
```

错误写法：

```python
os.fsync(fd)
```

正确写法：

```python
loop = asyncio.get_running_loop()
await loop.run_in_executor(None, os.fsync, fd)
```

原因：

```text
os.fsync() 是阻塞系统调用。
如果直接在事件循环中调用，心跳和选举定时器会被卡住。
这可能导致 leader 误超时，引发频繁重新选举。
```

---

### 2.6 client_table 必须进入 snapshot

Raft KV 需要用 `client_id + seq` 去重。

```text
client_table:
  client_id -> {
    last_seq,
    last_result
  }
```

这张表不能只存在内存里。

snapshot 必须同时保存：

```text
1. KV 数据
2. client_table
3. last_applied
```

原因：

```text
如果 client_table 不进 snapshot，节点重启后会忘记已经处理过的请求。
client 重试旧请求时，系统可能重复 apply，导致错误结果。
```

---

### 2.7 网络分区测试分两层

第 2 周开始做应用层 Network Simulator。

第 12 周再考虑真实网络测试。

应用层模拟支持：

```text
delay
drop
partition
heal
crash
restart
```

真实网络测试建议：

```text
1. macOS 本机不要强依赖 tc / iptables
2. Docker for Mac 网络经过虚拟化，tc / iptables 行为可能不稳定
3. 如果要做真实网络测试，优先使用 Linux 虚拟机或 Linux 云服务器
4. 本机开发时，Network Simulator 已足够支撑主要 Raft 测试
```

---

## 3. 推荐仓库结构

```text
distributed-labs/
  README.md

  common/
    rpc/
      codec.py
      client.py
      server.py
    network/
      simulator.py
    storage/
      wal.py
      snapshot.py
    utils/
      clock.py
      log.py

  week01_tcp/
  week02_rpc/
  week03_kv_wal/
  week04_snapshot_cas/
  week05_primary_backup/
  week06_consistency_lab/
  week07_sharded_kv/
  week08_shard_controller/
  week09_raft_election/
  week10_raft_log_replication/
  week11_raft_kv/
  week12_fault_injection/

  docs/
    reading-notes.md
    failure-cases.md
    project-mapping.md
```

最终项目可以单独整理成：

```text
mini-raft-kv/
  raft/
    node.py
    log.py
    message.py
    storage.py
  kv/
    state_machine.py
    client_table.py
  rpc/
    codec.py
    client.py
    server.py
  network/
    simulator.py
  tests/
  docs/
```

---

## 4. 推荐资料

优先使用中文资料或浏览器翻译后的英文资料。

### 4.1 主线资料

```text
1. DDIA 中文版
   重点读第 5、6、7、8、9 章

2. Raft 中文译文
   重点读 leader election、log replication、safety

3. MIT 6.5840 Labs
   不强制看视频，重点参考 Lab 设计

4. TiDB / TiKV 中文文档
   理解 Region、Raft Group、PD、调度、复制

5. Jepsen Consistency Models
   只查概念：linearizability、sequential consistency、eventual consistency
```

### 4.2 不建议一开始看

```text
1. Kubernetes 源码
2. Kafka 源码
3. TiKV 源码
4. etcd 全源码
5. Paxos 原论文
6. Spanner 论文
7. Dynamo 论文
8. Chubby 论文
```

这些以后再看。当前阶段先把系统写出来。

---

# 5. 12 周执行计划

---

## 第 1 周：asyncio TCP Echo Server

### 目标

理解 TCP 连接、断开、超时、并发 client。

### 必读内容

```text
1. asyncio.start_server 基础用法
2. TCP 是字节流，不是消息流
3. read timeout / connection reset / client disconnect
```

### 代码任务

实现 TCP Echo Server。

要求：

```text
1. 使用 asyncio.start_server
2. 支持多个 client
3. 一个 client 卡住不能阻塞其他 client
4. 每个请求带 request_id
5. server 打印 remote_addr、request_id、body
6. client 支持 timeout
```

第 1 周协议可以先用换行分割：

```text
request_id=1 body=hello\n
```

返回：

```text
request_id=1 ok=true body=hello\n
```

### 故障测试

```text
1. client 连接后立刻断开
2. client 发一半数据后断开
3. server sleep 5 秒再返回
4. client timeout 后退出
5. 多个 client 同时请求
```

### 验收标准

```text
1. server 不因异常连接崩溃
2. client timeout 能正确报错
3. 多 client 并发正常
4. 日志能看到 request_id
```

### 本周不做

```text
1. 不做 Raft
2. 不做 WAL
3. 不做复杂 RPC
4. 不做 protobuf / gRPC
```

---

## 第 2 周：length-prefix JSON RPC + 故障注入雏形

### 目标

实现一个可控的 RPC 层，并开始做应用层网络故障模拟。

### 代码任务

实现：

```text
1. length-prefix JSON 编解码
2. RpcClient
3. RpcServer
4. timeout
5. retry
6. client_id + seq 去重
7. Network Simulator 雏形
```

目录：

```text
week02_rpc/
  rpc/
    codec.py
    client.py
    server.py
  network/
    simulator.py
  tests/
    test_codec.py
    test_timeout.py
    test_retry.py
    test_duplicate_request.py
```

### 编码格式

```text
[4 bytes big-endian length][json bytes]
```

读取必须使用：

```python
header = await reader.readexactly(4)
size = int.from_bytes(header, "big")
body = await reader.readexactly(size)
```

### RPC 请求格式

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

### RPC 返回格式

```json
{
  "request_id": 1,
  "ok": true,
  "result": "OK",
  "error": ""
}
```

### Network Simulator

先实现：

```python
class Network:
    async def send(self, src, dst, message):
        pass

    def drop(self, src, dst):
        pass

    def delay(self, src, dst, ms):
        pass

    def recover(self, src, dst):
        pass

    def partition(self, group_a, group_b):
        pass

    def heal(self):
        pass
```

### 故障测试

```text
1. 模拟消息丢失
2. 模拟 500ms 延迟
3. client timeout 后 retry
4. server 执行成功但响应丢失
5. 重复请求不重复执行
```

### 验收标准

```text
1. 能处理半包
2. 能处理粘包
3. client timeout 后能 retry
4. server 能用 client_id + seq 去重
5. Network 能模拟 delay 和 drop
```

---

## 第 3 周：单机 KV + WAL + fsync

### 目标

理解状态机、日志、持久化语义。

后续 Raft 的本质就是：

```text
复制日志 -> 所有节点按同样顺序 apply -> 得到相同状态
```

### 代码任务

实现单机 KV Store。

接口：

```text
Put(key, value)
Get(key)
Delete(key)
```

WAL 路径：

```text
data/
  wal.log
```

WAL 记录：

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
4. WAL 最后一条损坏时自动截断
5. sync=always
6. sync=batch
7. client_id + seq 去重
```

### fsync 要求

不要直接：

```python
os.fsync(fd)
```

必须：

```python
loop = asyncio.get_running_loop()
await loop.run_in_executor(None, os.fsync, fd)
```

### 启动恢复流程

```text
1. 初始化空 HashMap
2. 读取 wal.log
3. 校验 crc32
4. 顺序 replay
5. 遇到最后一条损坏记录时截断
6. 恢复内存状态
```

### 故障测试

```text
1. Put 100 个 key
2. kill server
3. 重启 server
4. Get 这 100 个 key
5. 手动破坏 WAL 最后一行
6. 重启后恢复前面的完整记录
```

### 验收标准

```text
1. sync=always 下，返回成功的数据重启不丢
2. sync=batch 下，文档说明可能丢失哪些数据
3. WAL 最后一行损坏时，前面的完整记录能恢复
4. 重复请求不会重复 apply
```

---

## 第 4 周：Snapshot + Version + CAS

### 目标

理解日志压缩、版本号、条件更新。

### 代码任务

给单机 KV 增加：

```text
1. snapshot
2. version
3. CAS
```

Value 结构：

```python
{
  "data": "value",
  "version": 3
}
```

接口：

```text
Put(key, value)
Get(key)
Delete(key)
CAS(key, expected_version, new_value)
Snapshot()
LoadSnapshot()
```

目录：

```text
data/
  wal.log
  snapshot.dat
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

注意：即使当前还没有 Raft，也要提前把 `client_table` 放进 snapshot。

### 正确 snapshot 流程

完整流程：

```text
1. write snapshot.tmp
2. fsync snapshot.tmp
3. rename snapshot.tmp -> snapshot.dat
4. fsync directory
5. truncate wal.log
```

学习阶段可以简化，但 `design.md` 里必须写清楚完整流程。

### 故障测试

```text
1. 写入 3000 次
2. 生成 snapshot
3. kill server
4. 重启后恢复
5. snapshot 写到一半 kill
6. 确认旧 snapshot 不被破坏
```

### 验收标准

```text
1. version 单调递增
2. CAS 版本不匹配时失败
3. snapshot + WAL 能恢复完整状态
4. snapshot 包含 client_table
5. snapshot 过程中 kill，不破坏旧 snapshot
```

---

## 第 5 周：Primary-Backup KV

### 目标

理解最简单的主备复制。

结构：

```text
client -> primary -> backup
```

### 代码任务

节点：

```text
primary: 127.0.0.1:8001
backup:  127.0.0.1:8002
```

写入流程：

```text
1. client 发 Put 给 primary
2. primary 写 WAL
3. primary 把操作转发给 backup
4. backup 写 WAL
5. backup fsync
6. backup ack
7. primary 返回 client 成功
```

成功语义：

```text
只有 primary 和 backup 都写入 WAL，并且 backup ack 后，client 才能收到成功。
```

### 必须实现

```text
1. primary 到 backup 的 RPC
2. backup ack
3. primary 等 backup ack 后再返回
4. client_id + seq 去重
5. primary 和 backup 都使用 WAL
6. 使用 Network Simulator 注入 delay/drop
```

### 故障测试

```text
1. backup 正常，写入成功
2. backup 挂掉，primary 返回错误
3. backup 写 WAL 前崩溃，primary 不能返回成功
4. backup ack 丢失，client 重试后不能重复写
5. backup 重启后从 WAL 恢复
```

### 验收标准

```text
1. primary 和 backup 数据一致
2. backup 挂掉时不假装写入成功
3. client 重试不会导致重复写
4. backup 重启后能恢复 WAL
```

---

## 第 6 周：一致性实验

### 目标

亲自观察：

```text
stale read
read-your-writes
eventual consistency
linearizable read
```

### 代码任务

在 primary-backup 基础上支持三种读模式：

```text
1. read_from_primary
2. read_from_backup
3. read_from_random
```

增加复制延迟：

```text
primary -> backup delay = 0ms / 100ms / 1s / 5s
```

写测试脚本：

```text
1. Put x=i
2. 立刻 Get x
3. 重复 1000 次
4. 统计读到旧值的次数
```

### 测试矩阵

```text
读 primary，复制延迟 0ms / 100ms / 1s / 5s
读 backup，复制延迟 0ms / 100ms / 1s / 5s
随机读，复制延迟 0ms / 100ms / 1s / 5s
```

### 输出报告

写：

```text
week06_report.md
```

内容：

```text
1. 哪种模式能读到最新值
2. 哪种模式可能读到旧值
3. 延迟越大，旧读比例如何变化
4. 为什么 backup read 不一定线性一致
5. 为什么强一致读通常更贵
```

### 验收标准

```text
1. 能复现 stale read
2. 能解释 read-your-writes
3. 能解释 eventual consistency
4. 能解释为什么强一致读通常读 leader 或 quorum
```

---

## 第 7 周：Sharded KV + 路由表

### 目标

理解水平扩展、分片、路由、WrongShard。

### 代码任务

实现固定 4 个 shard：

```text
shard_id = hash(key) % 4
```

节点：

```text
node1: shard 0, 1
node2: shard 2, 3
```

路由表：

```json
{
  "config_id": 1,
  "shards": {
    "0": "node1",
    "1": "node1",
    "2": "node2",
    "3": "node2"
  }
}
```

### 请求必须带 config_id

```json
{
  "config_id": 1,
  "op": "Put",
  "key": "x",
  "value": "1"
}
```

server 收到不属于自己的 key 时返回：

```json
{
  "ok": false,
  "error": "WrongShard"
}
```

client 遇到 `WrongShard` 后刷新路由表。

### 必须实现

```text
1. client 缓存 config
2. WrongShard 后刷新 config
3. server 检查 config_id
4. server 拒绝旧 config 请求
5. Put/Get 根据 hash(key) 路由
```

### 验收标准

```text
1. Put/Get 能路由到正确节点
2. 错误节点返回 WrongShard
3. client 使用旧路由时能自动刷新
4. config_id 单调递增
```

---

## 第 8 周：Shard Controller + 数据迁移

### 目标

理解元数据服务、配置版本、数据迁移。

### 代码任务

实现 Shard Controller。

接口：

```text
Join(node_id)
Leave(node_id)
Move(shard_id, node_id)
QueryConfig()
```

配置格式：

```json
{
  "config_id": 3,
  "shards": {
    "0": "node1",
    "1": "node2",
    "2": "node2",
    "3": "node3"
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
1. controller 生成新 config
2. 新 owner 进入 Pulling
3. 旧 owner 进入 Pushing
4. 新 owner 拉取 shard 数据
5. 新 owner 完成导入
6. controller 发布新 config
7. 新 owner 进入 Serving
8. 旧 owner 删除或冻结旧 shard
```

### 重要规则

```text
迁移期间不能让两个节点同时接受同一个 shard 的写入。
```

### 故障测试

```text
1. node3 加入
2. 把 shard 3 从 node2 迁移到 node3
3. 迁移过程中 client 持续 Put/Get
4. 检查数据不丢
5. 检查没有双 owner 写入
```

### 验收标准

```text
1. config_id 单调递增
2. client 能感知配置变化
3. shard 迁移后数据仍然可读
4. 迁移期间不能出现两个 owner 同时接受同一个 key 的写入
5. 旧 config 请求被正确拒绝或重定向
```

---

## 第 9 周：Raft Leader Election

### 目标

实现 Raft 选主，不做日志复制。

### 节点状态

```python
class Role:
    FOLLOWER = "Follower"
    CANDIDATE = "Candidate"
    LEADER = "Leader"
```

核心字段：

```python
current_term: int
voted_for: Optional[str]
role: str
```

### RPC

```text
RequestVote
RequestVoteResponse
AppendEntriesHeartbeat
AppendEntriesResponse
```

### 时间参数

推荐：

```text
heartbeat interval: 100ms
election timeout: 300ms ~ 600ms 随机
rpc timeout: 200ms ~ 500ms
```

规则：

```text
1. election timeout 必须随机
2. election timeout 必须大于 heartbeat interval
3. 收到合法 leader heartbeat 后重置 election timeout
4. 收到更高 term 的任何消息都退回 follower
5. 每个 term 最多投一票
```

### Actor 规则

```text
1. 每个 RaftNode 是一个 Actor
2. 所有 RPC 消息进入 inbox
3. election timer 只投递 ElectionTimeout
4. heartbeat timer 只投递 HeartbeatTick
5. handle_event 是唯一状态修改入口
```

### 故障测试

```text
1. 启动 3 节点，最终只能有 1 个 leader
2. kill leader，剩余 2 个节点重新选主
3. 启动 5 节点，kill leader 后重新选主
4. 网络分区：[node1] | [node2, node3]
5. 网络分区恢复后最终只有 1 个 leader
```

### 验收标准

```text
1. 3 节点 5 秒内选出 leader
2. kill leader 后 5 秒内重新选主
3. 同一 term 不出现两个 leader
4. 网络分区时少数派不能形成可用 leader
5. 日志中能看到每次 term 变化
```

### 本周不做

```text
1. 不复制日志
2. 不接 KV
3. 不做 snapshot
4. 不做持久化
```

---

## 第 10 周：Raft Log Replication

### 目标

实现 Raft 的核心：复制日志。

### 日志结构

```python
{
  "term": 3,
  "index": 10,
  "command": {
    "op": "Put",
    "key": "x",
    "value": "1"
  }
}
```

### 必须实现

```text
1. client 请求只能发给 leader
2. leader append log
3. leader 发送 AppendEntries
4. follower 检查 prev_log_index / prev_log_term
5. follower 冲突日志必须删除
6. 多数节点复制成功后 leader commit
7. committed entry apply 到状态机
```

### 必须维护

```text
commit_index
last_applied
next_index[follower]
match_index[follower]
```

### AppendEntries 重点

```text
1. prev_log_index 不存在，拒绝
2. prev_log_term 不匹配，拒绝
3. follower 删除冲突日志
4. follower 追加 leader 新日志
5. leader 根据 match_index 推进 commit_index
```

### 必须打印日志

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
4. follower 有冲突日志时被 leader 修正
5. 网络丢包后最终日志收敛
```

### 验收标准

```text
1. 多数派复制后才能 commit
2. follower 日志最终与 leader 收敛
3. committed entry 不会被覆盖
4. apply 顺序严格等于 log index 顺序
5. commit_index 和 last_applied 正确分离
```

---

## 第 11 周：Raft 持久化 + Raft KV

### 目标

把 Raft 变成真正可恢复的 replicated KV。

### Raft 必须持久化

```text
current_term
voted_for
log_entries
```

持久化时需要明确 fsync 语义。

`sync=always` 下：

```text
返回成功前，相关 Raft 日志必须 fsync。
```

### Raft KV 命令格式

```json
{
  "op": "Put",
  "key": "x",
  "value": "1",
  "client_id": "c1",
  "seq": 12
}
```

支持：

```text
Put
Get
Delete
CAS
```

### Get 策略

本阶段为了简单，`Get` 也走 Raft。

原因：

```text
1. 保证线性一致读
2. 避免先实现 lease read / read index
3. 降低理解成本
```

后续优化阶段再做：

```text
lease read
read index
follower read
```

### client_table

```text
client_table:
  client_id -> {
    last_seq,
    last_result
  }
```

规则：

```text
1. apply 前检查 client_id + seq
2. 如果 seq 已处理，直接返回 last_result
3. 如果 seq 更新，执行命令并更新 client_table
4. client_table 必须进入 snapshot
```

### snapshot 内容

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

### 故障测试

```text
1. kill leader
2. client 重试同一个请求
3. follower 重启后追赶日志
4. 所有节点 kill -9 后重启
5. 重启后 committed 数据不丢
6. snapshot 后重启，client_table 仍然有效
```

### 验收标准

```text
1. committed log 不丢
2. 请求不会重复 apply
3. leader 切换后数据一致
4. 多数派存活时系统继续工作
5. Get 能看到最新 committed 写入
6. client_table 从 snapshot 恢复后仍能去重
```

---

## 第 12 周：故障注入、Docker 测试、项目总结

### 目标

把前 11 周的系统整理成作品。

### 应用层 Network Simulator 必须支持

```text
delay
drop
reorder
partition
heal
crash
restart
```

### 必须测试的分区场景

3 节点：

```text
[node1] | [node2, node3]
```

5 节点：

```text
[node1, node2] | [node3, node4, node5]
```

### 必须测试

```text
1. 少数派不能 commit
2. 多数派可以 commit
3. 分区恢复后日志收敛
4. kill leader 后可以重新选主
5. 所有节点重启后数据不丢
6. client 重试不会重复写
```

### Docker 测试

可以写：

```text
docker-compose.yml
node1
node2
node3
client
```

但注意：

```text
1. macOS 本机不强制做 tc / iptables
2. Docker for Mac 网络行为和 Linux 不完全一致
3. 如果要测真实 tc / iptables，建议用 Linux VM 或 Linux 云服务器
4. 本阶段主验收以 Network Simulator 为准
```

### 可选真实网络测试

在 Linux 环境中测试：

```text
1. iptables 阻断 node1 <-> node2
2. tc netem 注入 200ms delay
3. tc netem 注入 10% packet loss
4. docker kill leader
5. docker restart follower
```

也可以使用 `nicolaka/netshoot` 作为网络调试辅助容器。

### 输出文档

最终写三份文档：

```text
1. mini-raft-kv-design.md
2. failure-cases.md
3. project-mapping.md
```

`mini-raft-kv-design.md` 写：

```text
1. 系统架构
2. RPC 协议
3. Raft 状态机
4. WAL 和 snapshot
5. client 去重
6. 故障恢复流程
```

`failure-cases.md` 写：

```text
1. leader 宕机
2. follower 宕机
3. 网络分区
4. AppendEntries 丢包
5. RequestVote 延迟
6. WAL 最后一条损坏
7. snapshot 写一半崩溃
8. client 请求重复
```

`project-mapping.md` 写：

```text
BlockServe 中哪些地方需要：
- worker heartbeat
- task lease
- retry
- idempotent task result
- scheduler recovery

ClusterPilot 中哪些地方需要：
- desired state
- actual state
- controller reconcile
- leader election
- config version

IM 系统中哪些地方需要：
- message_id
- conversation sequence
- duplicate delivery removal
- offline replay
- fanout
- read receipt consistency
```

### 最终验收标准

```text
1. 能启动 3 节点 Raft KV
2. 能启动 5 节点 Raft KV
3. kill leader 后系统继续服务
4. 网络分区时少数派不能写成功
5. 多数派可以继续提交
6. 分区恢复后所有节点状态一致
7. client 重试不会导致重复写
8. 重启后 committed 数据不丢
9. snapshot 后 client_table 仍然有效
10. 所有测试可以重复运行
```

---

# 6. 每周固定工作流程

每周按这个节奏执行：

```text
周一：读文档，写本周目标
周二：写最小可运行代码
周三：补核心功能
周四：写故障测试
周五：修 bug
周六：重构和补日志
周日：写总结文档
```

每周必须有：

```text
README.md
design.md
test_report.md
```

`README.md` 写：

```text
1. 怎么启动
2. 怎么测试
3. 当前支持什么
4. 当前不支持什么
```

`design.md` 写：

```text
1. 数据结构
2. 请求流程
3. 故障处理
4. 为什么这样设计
```

`test_report.md` 写：

```text
1. 测了什么
2. 怎么测的
3. 结果是什么
4. 发现了什么 bug
5. 还有什么没解决
```

---

# 7. 每周代码提交标准

每周至少提交 5 次：

```text
commit 1：项目骨架
commit 2：核心数据结构
commit 3：核心流程跑通
commit 4：故障测试
commit 5：修复和总结
```

提交信息格式：

```text
week03: add wal append
week03: recover kv from wal
week03: handle corrupted wal tail
week03: add crash recovery test
week03: document fsync semantics
```

---

# 8. 执行红线

必须遵守：

```text
1. 不使用 threading 写 Raft
2. 不在第 9 周前引入 C++
3. 不在 Raft 跑通前引入 gRPC
4. 不用固定 election timeout
5. 不让多个协程同时修改 Raft 状态
6. 不在事件循环里直接调用 os.fsync()
7. length-prefix 读取必须使用 readexactly
8. WAL 成功返回前必须明确是否 fsync
9. 所有 client 写请求必须有 client_id + seq
10. client_table 必须进入 snapshot
11. 所有测试必须能重复运行
12. 每周都要写 test_report.md
13. 每个故障场景都必须有日志证明
```

---

# 9. 不要提前做的事

前 12 周不要做：

```text
1. 不读 Kubernetes 源码
2. 不读 Kafka 源码
3. 不读 TiKV 源码
4. 不做复杂服务发现
5. 不做 gRPC 框架封装
6. 不做监控大盘
7. 不做 Web UI
8. 不做分布式事务
9. 不做 Paxos
10. 不做多数据中心
11. 不做 lease read 优化
12. 不做 follower read 优化
```

这些不是没用，而是现在会分散主线。

---

# 10. 最小压缩路线

如果时间不够，只做这 6 个：

```text
1. length-prefix JSON RPC + timeout + retry
2. 单机 KV + WAL + fsync
3. primary-backup KV
4. Raft leader election
5. Raft log replication
6. Raft KV + client_table + fault injection
```

这 6 个完成后，分布式系统基础就不是空的。

---

# 11. 完成后的能力边界

完成 12 周后，你应该能理解并设计：

```text
1. 为什么 Kafka 用 partition 和 offset
2. 为什么 TiKV 用 Region 和 Raft Group
3. 为什么 etcd 可以作为强一致元数据存储
4. 为什么 IM 系统需要 message_id、seq、去重和重放
5. 为什么 BlockServe 需要 heartbeat、lease 和任务幂等
6. 为什么 ClusterPilot 需要 desired state、actual state 和 reconcile
7. 为什么 WAL 需要 fsync
8. 为什么 Raft 需要多数派
9. 为什么少数派分区不能提交
10. 为什么 client retry 必须配合去重表
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
8. TiKV / etcd / Kafka 全源码
```

---

# 12. 最终项目描述

英文描述：

```text
A minimal fault-tolerant distributed key-value store with Raft, WAL, snapshot, request deduplication, linearizable reads, and fault injection tests.
```

中文描述：

```text
一个用于学习分布式系统的最小 Raft KV，实现 length-prefix JSON RPC、网络超时、请求重试、幂等去重、WAL、fsync、snapshot、Raft 选主、日志复制、故障恢复和网络分区测试。
```

最终目标不是写一个玩具 demo，而是写一个结构清晰、故障可测、语义明确的最小分布式数据库。
