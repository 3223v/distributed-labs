# mini-raft-kv 客户端协议规范（protocol.md）

> 本文档是 client ↔ server 消息格式的**唯一权威定义**，覆盖第 3～12 周全部需求，一次定死。
> 修改本协议必须遵守第 8 节的兼容性规则。内部节点间消息（primary→backup、Raft RPC）见第 7 节，是另一个信封，不在客户端协议内。

---

## 0. 设计原则（为什么这样定）

1. **`result` 永远是 object 或 null，绝不是裸字符串。** 这样第 4 周加 `version`、以后加任何字段都只是在 object 里加 key，客户端零破坏。
2. **`error` 永远是带 `code` 的 object，绝不是裸字符串。** WrongShard / NotLeader / VersionMismatch 都是程序要分支处理的，字符串匹配是灾难；`leader_hint` 这类附加信息放进 `error.data`。
3. **收到不认识的字段必须忽略**（双向都是）。新增字段永远安全。
4. **幂等靠 `seq`，寻址靠 `request_id`，两者职责绝不混用**（见 3.1）。
5. 与 plan.md 原文示例的偏差及理由记录在第 9 节。

---

## 1. 传输层

不变，沿用第 2 周：

```
[4 字节 big-endian 长度][UTF-8 JSON]
```

- 读取必须 `await reader.readexactly(n)`。
- 一条 TCP 连接上可以连续发多个请求（当前 client 实现是一请求一连接，协议不限制）。

## 2. 请求信封

```json
{
  "v": 1,
  "request_id": 42,
  "client_id": "c1",
  "seq": 7,
  "config_id": 3,
  "method": "Put",
  "params": {"key": "x", "value": "1"}
}
```

| 字段 | 类型 | 必需性 | 说明 |
|---|---|---|---|
| `v` | int | 可选，默认 1 | 协议版本，终极逃生舱。server 遇到不认识的 v 返回 `UnsupportedVersion` |
| `request_id` | int | **必需** | 本条消息的标识，用于响应匹配和日志追踪。**每次发送（含重试）都不同** |
| `client_id` | string | 写必需，读可选 | 客户端身份，去重表的 key |
| `seq` | int | **写方法必需**，读方法省略 | 幂等序号。**重试时不变**。只有写方法消耗 seq |
| `config_id` | int | 第 7 周起必需，之前省略 | 客户端持有的路由表版本。单 shard 模式 server 忽略它 |
| `method` | string | 必需 | 见第 5 节方法表，大小写不敏感，规范形式为表中写法 |
| `params` | object | 必需，可为 `{}` | 方法参数 |

### 2.1 request_id 与 seq 的分工（重要）

```
request_id：每条消息唯一，重试 +1 —— 回答「这个响应是回给哪次发送的」
seq       ：每个写操作唯一，重试不变 —— 回答「这个操作是否已经执行过」
```

推论（这条规则修复了现有的去重缓存 bug）：
- server 去重命中时，缓存的是**业务结果**，响应信封里的 `request_id` 必须填**当前这次请求的** request_id，不能回放旧的。
- client 只用 request_id 匹配响应，用 seq 保证不重复执行。

### 2.2 seq 的推进规则

- 只有**写方法**（Put / Delete / CAS）递增并携带 seq；读方法（Get / Ping / Echo / QueryConfig）不携带。
- 同一 client_id 的 seq 单调递增，server 端规则：
  - `seq == last_seq` → 重复请求，返回缓存结果，不重复 apply
  - `seq <  last_seq` → 返回错误 `StaleSeq`
  - `seq >  last_seq` → 新请求，执行并记录

## 3. 响应信封

```json
{
  "v": 1,
  "request_id": 42,
  "ok": true,
  "result": {"version": 3},
  "error": null
}
```

失败时：

```json
{
  "v": 1,
  "request_id": 42,
  "ok": false,
  "result": null,
  "error": {
    "code": "NotLeader",
    "message": "node2 is not leader",
    "data": {"leader_hint": "node1"}
  }
}
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `request_id` | int | 原样回显**当前请求**的 request_id |
| `ok` | bool | 业务是否成功 |
| `result` | object \| null | 成功时的结果，**永远是 object**；失败时为 null |
| `error` | object \| null | 失败时非空：`{code, message, data}`；成功时为 null |

`error.data` 按 code 不同携带机器可读的附加信息，`error.message` 只给人看，程序**只允许**依赖 `code` 和 `data`。

## 4. 错误码表（全 12 周汇总）

| code | 引入周 | data | 含义 |
|---|---|---|---|
| `BadRequest` | 3 | — | 缺字段、类型错、params 不合法 |
| `UnknownMethod` | 3 | — | method 不认识（**未实现的方法也返回这个**，如第 3 周收到 CAS） |
| `UnsupportedVersion` | 3 | `{"supported": [1]}` | 协议版本不支持 |
| `KeyNotFound` | 3 | — | Get/Delete 的 key 不存在（Delete 是否算错误见 5.3） |
| `StaleSeq` | 3 | `{"last_seq": 7}` | seq 回退 |
| `VersionMismatch` | 4 | `{"current_version": 5}` | CAS 版本不匹配 |
| `Unavailable` | 5 | — | 依赖的副本不可用（如 backup 宕机），**不伪造成功** |
| `WrongShard` | 7 | `{"config_id": 4}` | key 不归本节点管；data 带 server 当前 config_id，提示 client 刷新路由 |
| `StaleConfig` | 7 | `{"config_id": 4}` | 请求的 config_id 过旧 |
| `NotLeader` | 9 | `{"leader_hint": "node1"}` | 本节点不是 leader；leader 未知时 hint 为 null |
| `Internal` | 3 | — | 服务端内部错误兜底 |

客户端处理约定：`WrongShard`/`StaleConfig` → 刷新路由表后重试（seq 不变）；`NotLeader` → 换 hint 节点重试（seq 不变）；`Unavailable`/超时 → 退避重试（seq 不变）。

## 5. 方法表

### 5.1 KV 数据方法

| method | 引入周 | params | result（成功时） |
|---|---|---|---|
| `Put` | 3 | `{"key": s, "value": s}` | `{}`；第 4 周起 `{"version": int}` |
| `Get` | 3 | `{"key": s}` | `{"value": s}`；第 4 周起 `{"value": s, "version": int}` |
| `Delete` | 3 | `{"key": s}` | `{"existed": bool}` |
| `CAS` | 4 | `{"key": s, "expected_version": int, "value": s}` | `{"version": int}`（新版本号） |

### 5.2 版本语义（第 4 周生效，现在定死约定）

- 首次 Put 的 version 为 **1**，之后每次成功写 +1。
- **version 0 表示「key 不存在」**。推论：`CAS(key, expected_version=0, value)` 即「不存在才创建」，一个约定同时解决 create-if-absent，不需要新增方法。
- Get 不存在的 key → `ok: false, code: KeyNotFound`（而非 ok:true + null value，保持与现有实现一致）。

### 5.3 Delete 语义

Delete 不存在的 key：`ok: true, result: {"existed": false}`——删除是幂等操作，"本来就没有"不算失败。（这也让重试天然安全。）

### 5.4 运维/控制方法

| method | 引入周 | params | result |
|---|---|---|---|
| `Ping` | 3 | `{}` | `{"msg": "pong"}` |
| `Echo` | 3 | `{"value": s}` | `{"value": s}` |
| `QueryConfig` | 7 | `{}` | `{"config_id": int, "shards": {"0": "group1", ...}}` |
| `Join` | 8 | `{"node_id": s}` | `{"config_id": int}` |
| `Leave` | 8 | `{"node_id": s}` | `{"config_id": int}` |
| `Move` | 8 | `{"shard_id": int, "group_id": s}` | `{"config_id": int}` |

Join/Leave/Move 是发给 shard controller 的管理命令，复用同一信封。

## 6. 去重表缓存什么

去重表的 value 缓存**业务结果三元组**，不含信封字段：

```json
{"client_id": {"last_seq": 7, "last_ok": true, "last_result": {...}, "last_error": null}}
```

- 必须包含 `ok` 和 `error`：**确定性失败也要原样回放**。例如 CAS 因 VersionMismatch 失败后响应丢失，client 重试同一 seq，必须再次收到 VersionMismatch，而不是被重新执行。
- 回放时信封的 `request_id` 用当前请求的（见 2.1）。
- 本结构就是进 WAL replay 重建、第 4 周进 snapshot 的结构。

## 7. 内部消息信封（区别于客户端协议）

节点间消息（第 5 周 primary→backup，第 9 周起 Raft）用 plan.md 3.4 的信封，以 `type` 字段区分于客户端请求的 `method` 字段：

```json
{"type": "RequestVote", "request_id": 1001, "from": "node1", "to": "node2", "term": 3, "payload": {...}}
```

同一 codec、同一传输层。server 收到消息后：有 `method` → 客户端请求；有 `type` → 内部消息。两个命名空间永不混用。（内部 payload 的详细定义等第 5/9 周写进本文件新章节，不影响客户端协议。）

## 8. 兼容性规则（修改本协议前必读）

1. 只允许**新增可选字段**，不允许改名、改类型、改语义。
2. 新能力优先加**新 method**，而不是给旧 method 的 params 加分支语义。
3. 新错误优先加**新 code**；老 client 遇到不认识的 code 按通用失败处理即可。
4. 收到不认识的字段/方法/错误码：字段忽略，方法回 `UnknownMethod`，错误码按失败处理。
5. 万不得已才动 `v`，且 server 必须同时支持新旧两个版本一整周。

## 9. 与 plan.md 原文示例的偏差

| plan 原文 | 本规范 | 理由 |
|---|---|---|
| `"result": "OK"`（字符串） | `result` 是 object | 第 4 周 Get 要带 version，第 7 周 QueryConfig 要返回路由表，裸字符串必然破坏 |
| `"error": "WrongShard"`（字符串） | `error` 是 `{code, message, data}` | NotLeader 要携带 leader_hint，WrongShard 要携带最新 config_id，字符串塞不下 |
| 所有请求都带 seq | 只有写方法带 seq | Get 幂等无需去重；读也占 seq 会让去重表白白膨胀 |

语义全部一致（去重规则、错误场景、方法集合都来自 plan 原文），只是**载体**从字符串升级为对象。

## 10. 现有代码改造清单（第 3 周动手时一起做）

1. `rpc/server.py`：所有 `"result": "OK"` / `"result": "pong"` / 裸字符串 error 改为本规范格式；去重命中时用当前 request_id 重新包壳（修复缓存旧 request_id 的 bug）。
2. `client/client.py`：`call()` 返回 `{ok, result, error}` 中 result/error 已是 object，无需扁平化；只有写方法递增 `self.seq`（读方法不带 seq）。
3. `kv/` 各模块：去重表按第 6 节结构缓存 `{last_seq, last_ok, last_result, last_error}`。
4. WAL 记录里的 command 不受影响（`{op,key,value,client_id,seq}` 保持 plan 原文格式，那是内部持久化格式，不是客户端协议）。
