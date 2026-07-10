# 分布式系统 0 基础 12 周执行计划

## 目标

用 12 周从 0 开始补齐分布式系统基础，最终写出一个可以运行的 `mini-raft-kv`：

```text
client -> 多节点 Raft 集群 -> replicated KV store
```

完成后应具备这些能力：

```text
1. 理解网络不可靠、超时、重试、幂等、去重
2. 理解 WAL、snapshot、crash recovery
3. 理解 primary-backup 复制
4. 理解 stale read、线性一致性、最终一致性
5. 理解分片、路由、shard controller
6. 理解 Raft 的选主、日志复制、commit、apply
7. 能写故障注入测试：宕机、重启、网络分区、延迟、丢包
8. 能把这些知识迁移到 IM、BlockServe、ClusterPilot
```

本计划默认每周投入：

```text
工作日：每天 1 小时
周末：一天 3~5 小时
每周总计：8~12 小时
```

不要求大量看视频。主线是：

```text
文档 30%
代码 60%
测试 10%
```

---

# 资料选择原则

## 优先级

```text
第一优先级：中文文档
第二优先级：英文官方文档 + 浏览器翻译
第三优先级：论文中文译文
第四优先级：视频
```

视频不是主线，只在完全卡住时看 10~20 分钟片段。

## 推荐资料

### 1. DDIA 中文版

用于补数据库、复制、分区、一致性、事务、流处理的整体视角。中文译本适合作为长期参考，不要求一口气读完。DDIA 中文第二版翻译站点介绍了数据系统的模型、存储、分区、复制等主题。

本计划只要求读这些章节：

```text
第 5 章：复制
第 6 章：分区
第 7 章：事务
第 8 章：分布式系统的麻烦
第 9 章：一致性与共识
```

### 2. Raft 中文译文

Raft 是本计划的核心算法。中文译文可以看 GitHub 上的《寻找一种易于理解的一致性算法》。译文说明 Raft 把一致性算法拆成领导人选举、日志复制、安全性等模块，适合初学者逐块实现。

同时保留 Raft 官方网站作为权威参考。Raft 官网说明 Raft 的目标是更容易理解，并且在容错性和性能上与 Paxos 等价。

### 3. MIT 6.5840 Labs

不要求完整上课，也不要求跟着视频。重点参考 Lab 的任务设计。

MIT 6.5840 是分布式系统课程，主题包括容错、复制、一致性和真实系统案例。

重点参考：

```text
Lab 1：MapReduce
Lab 3：Raft
Lab 4：Fault-tolerant KV
Lab 5：Sharded KV
```

MIT Lab 1 要实现 coordinator 和 worker，并处理 worker 失败，适合你理解调度和容错。

MIT Lab 3 要实现 Raft；Lab 4 要在 Raft 之上实现容错 KV，并要求多数节点存活且能通信时继续服务。

### 4. TiDB / TiKV 中文文档

用来理解工业级分布式 KV 怎么做。TiKV 中文文档说明 TiKV 通过 Raft 保证多副本一致性和高可用，并采用 multi-raft-group，把数据按 key range 划分为 Region，由 PD 进行调度。

重点读：

```text
TiDB 数据库的存储
TiKV 简介
TiDB 数据库的调度
术语表：PD、Region、Raft Group、Leader、Follower
```

TiDB 存储文档还说明 TiKV 通过 RocksDB 做本地存储，通过 Raft 把数据复制到多台机器，并且写入通常只需要同步到多数节点即可认为成功。

### 5. Jepsen 一致性模型

不用全读。只查概念：

```text
linearizability
sequential consistency
serializability
eventual consistency
stale read
```

Jepsen 的一致性模型文档用层级关系解释一致性强弱，例如 linearizability 蕴含 sequential consistency。

### 6. Kafka 官方文档

第 11、12 周再看。Kafka 官方文档的 Design 部分解释 Kafka 的核心概念，官方文档也列出 Kafka 的核心 API。

不建议一开始看 Kafka 源码。

---

# 代码语言选择

建议分两层：

```text
实验代码：Python
核心项目：C++
```

原因：

```text
1. Python 适合快速验证网络故障、超时、重试、状态机
2. C++ 适合你后续的 BlockServe、ClusterPilot、IM 系统
3. 不要一开始就用 C++ 写 Raft，否则并发、内存、网络、算法会同时增加难度
```

推荐仓库结构：

```text
distributed-labs/
  week01_tcp/
  week02_rpc/
  week03_kv_wal/
  week04_primary_backup/
  week05_consistency_lab/
  week06_sharding/
  week07_raft_election/
  week08_raft_log/
  week09_raft_persist/
  week10_raft_kv/
  week11_fault_injection/
  week12_project_integration/
```

---

# 第 1 周：网络、超时、连接生命周期

## 目标

理解分布式系统最底层的问题：网络不可靠。

## 必读文档

```text
1. Distributed Systems for Fun and Profit：前两章
2. DDIA 第 8 章：分布式系统的麻烦，只读网络和时钟相关部分
```

## 代码任务

写一个 TCP Echo Server。

功能：

```text
1. server 支持多个 client
2. client 发送字符串，server 原样返回
3. 每个请求带 request_id
4. server 打印 remote_addr、request_id、body
5. client 设置 read timeout
6. client 支持连接失败重试
```

协议先用文本：

```text
request_id=1 body=hello
```

返回：

```text
request_id=1 ok=true body=hello
```

## 故障测试

手动测试：

```text
1. client 连接后立刻断开
2. client 发一半数据后断开
3. server 处理请求前 sleep 5 秒
4. client timeout 后重试
```

## 验收标准

```text
1. server 不因为 client 异常断开崩溃
2. client timeout 后能报错
3. client 能重试
4. 日志里能看到 request_id
```

## 本周不做

```text
1. 不做复杂 RPC
2. 不做 protobuf
3. 不做 Raft
4. 不做分片
```

---

# 第 2 周：RPC、重试、幂等

## 目标

理解“请求可能执行了，但响应丢了”这个核心问题。

## 必读文档

```text
1. DDIA 第 8 章：故障、不可靠网络
2. Jepsen linearizability 页面，只看定义
```

## 代码任务

在第 1 周基础上写一个简单 RPC 框架。

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

server 支持：

```text
Ping()
Echo(text)
Put(key, value)
Get(key)
```

## 必须实现

```text
1. client timeout
2. client retry
3. client_id + seq
4. server 记录每个 client 最新 seq
5. 重复请求直接返回上次结果
```

## 故障测试

```text
1. server 执行 Put 后故意不返回
2. client timeout 后重试
3. 确认 Put 不会被执行两次
```

## 验收标准

```text
1. 同一个 client_id + seq 的 Put 只执行一次
2. client 重试不会产生重复写
3. server 日志能区分首次请求和重复请求
```

---

# 第 3 周：单机 KV、WAL、恢复

## 目标

理解状态机和日志。

Raft 后面要做的事情，本质就是：

```text
复制日志 -> 所有节点按相同顺序 apply -> 得到相同状态
```

## 必读文档

```text
1. DDIA 第 3 章：存储与检索，只看日志结构存储部分
2. TiDB 数据库的存储：只看 RocksDB + Raft 的关系
```

## 代码任务

写一个单机 KV Store。

接口：

```text
Put(key, value)
Get(key)
Delete(key)
```

必须支持 WAL：

```text
data/
  wal.log
```

WAL 格式：

```json
{"op":"Put","key":"x","value":"1","client_id":"c1","seq":1}
{"op":"Delete","key":"x","client_id":"c1","seq":2}
```

启动流程：

```text
1. 初始化空 HashMap
2. 顺序读取 wal.log
3. 重放所有操作
4. 恢复内存状态
```

## 故障测试

```text
1. Put 100 个 key
2. kill server
3. 重启 server
4. Get 这 100 个 key
```

## 验收标准

```text
1. 重启后数据不丢
2. Delete 重启后仍然生效
3. WAL 损坏最后一行时，前面的完整记录能恢复
```

---

# 第 4 周：Snapshot、版本号、CAS

## 目标

理解日志不能无限增长，以及并发修改时需要版本控制。

## 必读文档

```text
1. DDIA 第 7 章：事务，只看并发控制相关部分
2. TiKV 简介：只看 Region 和 Raft Group 概念
```

## 代码任务

给单机 KV 增加：

```text
1. snapshot
2. version
3. CAS
```

Value 结构：

```cpp
struct Value {
    std::string data;
    uint64_t version;
};
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

Snapshot 规则：

```text
1. 每 1000 条 WAL 生成一次 snapshot
2. snapshot 成功后截断旧 WAL
3. 重启时先加载 snapshot，再重放 WAL
```

## 故障测试

```text
1. 写入 3000 次
2. 确认生成 snapshot
3. kill server
4. 重启后数据正确
```

## 验收标准

```text
1. version 单调递增
2. CAS 版本不匹配时失败
3. snapshot + WAL 能恢复完整状态
```

---

# 第 5 周：Primary-Backup 复制

## 目标

理解最简单的主备复制。

## 必读文档

```text
1. DDIA 第 5 章：复制
2. TiDB 存储文档中 Raft 复制部分
```

## 代码任务

实现：

```text
client -> primary -> backup
```

写入流程：

```text
1. client 发 Put 给 primary
2. primary 写 WAL
3. primary 把操作转发给 backup
4. backup 写 WAL
5. backup ack
6. primary 返回 client
```

节点：

```text
primary: 127.0.0.1:8001
backup:  127.0.0.1:8002
```

## 必须实现

```text
1. primary 到 backup 的 RPC
2. backup ack
3. primary 等 backup ack 后再返回
4. client_id + seq 去重
5. backup 重启后从 WAL 恢复
```

## 故障测试

```text
1. backup 正常，写入成功
2. backup 挂掉，primary 返回错误
3. backup 重启，重新接受复制
4. client 重试，不能重复写
```

## 验收标准

```text
1. primary 和 backup 数据一致
2. backup 挂掉时不假装写入成功
3. client 重试不会导致重复写
```

---

# 第 6 周：一致性实验

## 目标

亲自观察 stale read、read-your-writes、eventual consistency。

## 必读文档

```text
1. Jepsen consistency models
2. Jepsen linearizability
3. DDIA 第 5 章：复制延迟与一致性
```

## 代码任务

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

写一个测试脚本：

```text
1. Put x=1
2. 立刻 Get x
3. 重复 1000 次
4. 统计读到旧值的次数
```

## 输出报告

写一个 `week06_report.md`：

```text
1. 哪种模式能读到最新值
2. 哪种模式可能读到旧值
3. 延迟越大，旧读比例如何变化
4. 为什么 backup read 不一定线性一致
```

## 验收标准

```text
1. 能复现 stale read
2. 能解释 read-your-writes
3. 能解释为什么强一致读更贵
```

---

# 第 7 周：分片 KV 和路由表

## 目标

理解水平扩展不是“复制更多机器”，而是“把不同 key 分到不同节点”。

## 必读文档

```text
1. DDIA 第 6 章：分区
2. TiKV 简介：Region
3. TiDB 数据库的调度：PD
```

## 代码任务

实现 Sharded KV。

固定 4 个 shard：

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

server 收到不属于自己的 key 时返回：

```json
{
  "ok": false,
  "error": "WrongShard"
}
```

client 遇到 WrongShard 后重新查询路由表。

## 验收标准

```text
1. Put/Get 能路由到正确节点
2. 错误节点返回 WrongShard
3. 修改路由表后 client 能重新路由
```

---

# 第 8 周：Shard Controller 和数据迁移

## 目标

理解元数据服务和 resharding。

## 必读文档

```text
1. MIT Lab 5 Sharded KV 说明
2. TiDB 调度文档
```

MIT Lab 5 要做 sharded key/value service，把 key 分片到多个 Raft 复制组上，分片的目的主要是提升性能。

## 代码任务

实现一个 Shard Controller。

接口：

```text
Join(node_id)
Leave(node_id)
Move(shard_id, node_id)
QueryConfig()
```

支持配置版本：

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

实现简单迁移：

```text
1. controller 生成新 config
2. 旧 owner 导出 shard 数据
3. 新 owner 导入 shard 数据
4. 新 owner 接管 shard
```

## 故障测试

```text
1. node3 加入
2. 把 shard 3 从 node2 迁移到 node3
3. 迁移过程中 client 持续 Put/Get
4. 检查数据不丢
```

## 验收标准

```text
1. config_id 单调递增
2. client 能感知配置变化
3. shard 迁移后数据仍然可读
```

---

# 第 9 周：Raft Leader Election

## 目标

实现 Raft 选主，不做日志复制。

## 必读文档

```text
1. Raft 中文译文：选举部分
2. Raft 官方网站：动画和论文结构
3. MIT Lab 3 Raft 说明
```

## 代码任务

实现 3 节点 Raft election。

节点状态：

```cpp
enum class Role {
    Follower,
    Candidate,
    Leader
};
```

核心字段：

```cpp
uint64_t current_term;
std::optional<NodeId> voted_for;
Role role;
```

RPC：

```text
RequestVote
RequestVoteResponse
AppendEntriesHeartbeat
AppendEntriesResponse
```

必须实现：

```text
1. election timeout 随机化
2. candidate 自增 term
3. 每个 term 最多投一票
4. 收到更大 term 自动退回 follower
5. leader 定时发 heartbeat
```

## 故障测试

```text
1. 启动 3 节点，最终只能有 1 个 leader
2. kill leader，剩余 2 个节点重新选主
3. 网络延迟下不能出现长期双 leader
```

## 验收标准

```text
1. 3 节点 5 秒内选出 leader
2. kill leader 后 5 秒内重新选主
3. 同一 term 不出现两个 leader
```

---

# 第 10 周：Raft Log Replication

## 目标

实现 Raft 的核心：复制日志。

## 必读文档

```text
1. Raft 中文译文：日志复制部分
2. MIT Lab 3 Raft 说明
3. etcd raft README
```

etcd 的 raft 库说明 Raft 用 replicated log 让一组节点维护同一个 replicated state machine。

## 代码任务

日志结构：

```cpp
struct LogEntry {
    uint64_t term;
    uint64_t index;
    std::string command;
};
```

实现：

```text
1. client 请求只能发给 leader
2. leader append log
3. leader 发送 AppendEntries
4. follower 检查 prev_log_index / prev_log_term
5. 多数节点复制成功后 leader commit
6. committed entry apply 到状态机
```

## 必须维护

```text
commit_index
last_applied
next_index[follower]
match_index[follower]
```

## 故障测试

```text
1. follower 掉线后重新上线
2. follower 日志落后后追赶
3. leader 宕机后新 leader 接管
4. 日志冲突时 follower 回滚
```

## 验收标准

```text
1. 多数派复制后才能 commit
2. follower 日志最终与 leader 收敛
3. apply 顺序严格等于 log index 顺序
```

---

# 第 11 周：Raft 持久化和 Raft KV

## 目标

把 Raft 变成真正可恢复的 replicated KV。

## 必读文档

```text
1. MIT Lab 4 Fault-tolerant KV
2. Raft 中文译文：安全性与持久化相关部分
3. TiDB 存储文档：Raft 日志和多数派写入
```

## 代码任务

Raft 必须持久化：

```text
current_term
voted_for
log_entries
```

Raft KV 命令：

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

要求：

```text
1. Put/Delete/CAS 走 Raft
2. Get 也走 Raft，先保证线性一致
3. client_id + seq 去重
4. apply 后才能返回 client
```

## 故障测试

```text
1. kill leader
2. 重启 follower
3. 所有节点 kill -9 后重启
4. 重试同一个 client 请求
```

## 验收标准

```text
1. committed log 不丢
2. 请求不会重复 apply
3. leader 切换后数据一致
4. 多数派存活时系统继续工作
```

---

# 第 12 周：故障注入、总结、迁移到你的项目

## 目标

把前 11 周的东西整理成作品，并明确迁移到 BlockServe、ClusterPilot、IM 的方式。

## 代码任务

写 Network Simulator。

支持：

```text
1. delay
2. drop
3. reorder
4. partition
5. crash
6. restart
```

测试场景：

```text
partition: [node1, node2] | [node3, node4, node5]
```

必须测试：

```text
1. 少数派不能 commit
2. 多数派可以 commit
3. 分区恢复后日志收敛
4. kill leader 后可以重新选主
5. 所有节点重启后数据不丢
```

## 输出文档

写三份总结：

```text
1. mini-raft-kv-design.md
2. failure-cases.md
3. project-mapping.md
```

`project-mapping.md` 写清楚：

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

## 最终验收标准

```text
1. 能启动 3 节点 Raft KV
2. 能启动 5 节点 Raft KV
3. kill leader 后系统继续服务
4. 网络分区时少数派不能写成功
5. 分区恢复后所有节点状态一致
6. client 重试不会导致重复写
7. 重启后 committed 数据不丢
```

---

# 每周固定工作流程

每周都按这个流程执行：

```text
周一：读文档，写本周目标
周二：写最小可运行代码
周三：补核心功能
周四：写失败测试
周五：修 bug
周六：重构和补日志
周日：写总结文档
```

每周必须有这三个文件：

```text
README.md
design.md
test_report.md
```

`README.md` 写：

```text
怎么启动
怎么测试
当前支持什么
当前不支持什么
```

`design.md` 写：

```text
数据结构
请求流程
故障处理
为什么这样设计
```

`test_report.md` 写：

```text
测了什么
怎么测的
结果是什么
发现了什么 bug
```

---

# 每周代码提交标准

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
```

---

# 不要提前做的事

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
```

这些都会分散精力。

---

# 最小路线压缩版

如果时间不够，只做这 6 个：

```text
1. RPC + timeout + retry + request_id
2. 单机 KV + WAL
3. primary-backup KV
4. Raft leader election
5. Raft log replication
6. Raft KV + fault injection
```

这 6 个做完，分布式系统基础就不是空的了。

---

# 最终项目名称

建议主项目叫：

```text
mini-raft-kv
```

仓库描述：

```text
A minimal fault-tolerant distributed key-value store with Raft, WAL, snapshot, retry deduplication, and fault injection tests.
```

中文描述：

```text
一个用于学习分布式系统的最小 Raft KV，实现网络超时、请求重试、幂等去重、WAL、snapshot、Raft 选主、日志复制、故障恢复和网络分区测试。
```

---

# 完成后的能力边界

完成这 12 周后，你应该能看懂并设计：

```text
1. 为什么 Kafka 用 partition 和 offset
2. 为什么 TiKV 用 Region 和 Raft Group
3. 为什么 etcd 可以作为强一致元数据存储
4. 为什么 IM 消息系统需要 message_id、seq、去重和重放
5. 为什么 BlockServe 需要 heartbeat、lease 和任务幂等
6. 为什么 ClusterPilot 需要 desired state、actual state 和 reconcile
```

但此时还不需要掌握：

```text
1. 分布式事务
2. 多副本读优化
3. lease read
4. MVCC
5. gossip
6. 多数据中心一致性
7. Kubernetes controller-runtime 源码
```

这些放到下一阶段。
