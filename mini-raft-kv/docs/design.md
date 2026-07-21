# mini-raft-kv 设计文档

> 累积文档，每周更新。记录架构决策、数据格式、请求流程、已知问题。

## 1. 架构分层

```
Client -> RpcClient -> RpcServer -> ReplicationEngine -> KVStateMachine -> WAL
                                              |
                                         LocalEngine (当前)
                                         未来: PrimaryBackupEngine -> RaftEngine
```

**依赖规则**：
- Server 只认识 Engine 接口，不碰状态机/WAL
- Engine 知道 WAL 和状态机，不知道网络协议
- 状态机内部持有 ClientTable，apply() 是唯一改状态入口
- 组装只发生在 app.py

**为什么 apply 是唯一状态入口**：重启 replay 和正常写入走同一个 apply()，client_table 从 WAL 自动重建，保证"重启后仍能识别已处理请求"（红线 4）。

## 2. 数据格式

### 客户端协议

```
请求: {v, request_id, client_id, seq, config_id, method, params}
响应: {v, request_id, ok, result, error}
      error = {code, message, data}
```

详见 `docs/protocol.md`。当前实现有偏差（version 字段未按协议），第 4 周 CAS 时收敛。

### 内部 Command

```python
{op, key, value, client_id, seq, version, request_id}
```

### WAL 记录

```json
{"key":"x","op":"put","value":"1","client_id":"c1","seq":1,"version":null,"request_id":1,"crc32":123456}
```

每条一行 JSON，含 CRC32。replay 时校验 checksum，首条损坏即截断。

## 3. 请求流程（Put 为例）

```
1. Server 收字节 → codec.decode → 校验信封
2. Server 把 RPC 请求翻译成 Command
3. Server → engine.submit(cmd)
4. LocalEngine.submit:
   a. islegal() 校验
   b. wal.append(cmd)   ← 先落盘
   c. sm.apply(cmd)     ← 再执行
5. StateMachine.apply:
   a. ct.check(client_id, seq) → new/duplicate/stale
   b. 执行 op（改 self.dt）
   c. ct.record(...)
   d. 返回 {ok, result, error}
6. Server 把三元组包成响应信封 → 发送
```

**读请求不经过 submit**：Get → engine.query(qry) → sm.read(qry)，不写 WAL，不进去重表。

## 4. 去重设计

ClientTable 按 client_id 存储：`{last_seq, last_ok, last_result, last_error}`。

- **new**：执行命令 → record
- **duplicate**：返回缓存的三元组（含 ok/error，确定性失败也原样回放）
- **stale**：拒绝，返回错误

**去重只在 apply() 里生效**，不在 engine 或 server 层。这样 replay 时自动重建 client_table。

**已知行为**：engine.submit 不做预查，重复请求仍会写一条 WAL。这不影响正确性——replay 时第二条在 apply 层命中去重、不覆盖数据。预查是优化，不是正确性要求。

## 5. WAL 与持久化

### sync=always（已实现）
每条 append 后 `flush + fsync`。崩溃后最多丢 0 条。

### sync=batch（未实现）
每 N 条或每 T 毫秒 fsync 一次。崩溃后最多丢最近一批未 fsync 的记录。

### 恢复流程
```
1. WAL.replay() → 校验 crc32 → 遇损坏行截断 → 返回 Command 列表
2. 逐条 sm.apply(cmd) → 重建 data + client_table
3. 启动 RPC 服务
```

## 6. 当前状态

### 已实现
- Length-prefix JSON RPC + timeout/retry
- LocalEngine + KVStateMachine + ClientTable
- WAL (append/crc32/replay/tail truncation)
- sync=always 持久化
- 崩溃恢复（kill -9 重启数据不丢）
- 请求去重（同一 client_id+seq 不重复执行）
- 基础测试（unit: WAL, integration: CRUD/恢复/去重）

### 未实现
- WAL sync=batch
- Snapshot
- Version（value 带版本号）
- CAS
- Network Simulator
- PrimaryBackupEngine
- Raft

### 已知问题
- state_machine: del 不存在的 key 时 tmp 未定义（NameError）
- state_machine: CAS 对不存在的 key 会 KeyError
- Put 的 version 目前存 -1（占位，第 4 周改为服务端自增）
- 客户端协议与 protocol.md 有偏差（error code 为空字符串，result 格式不完全一致）
