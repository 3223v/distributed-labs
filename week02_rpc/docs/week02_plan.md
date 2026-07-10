# Week 2：RPC、重试、幂等

## 本周目标

理解分布式系统中一个核心问题：**"请求可能已经执行了，但响应丢了"**。

第 1 周你理解了"网络不可靠会导致连接断开和超时"。本周往前推一步：

```
Client                        Server
   │                             │
   │──── Put(x=1) ────────────> │  ← 请求到达，Server 执行成功
   │                             │
   │      (响应在网络中丢失)      │  ← Server 返回了，但 Client 没收到
   │                             │
   │──── Put(x=1) 重试 ───────> │  ← Client 超时后重试
   │                             │
   │  问题：这次 Put 应该再执行一次吗？   │
```

答案是：**不应该**。这就是幂等性的核心——同一个操作执行多次，效果和执行一次相同。

本周你要实现一个简单的 RPC 框架，用 `client_id + seq` 实现请求去重。

---

## 阅读清单

### 必读（2~3 小时）

| # | 资料 | 链接 | 读什么 |
|---|------|------|--------|
| 1 | **DDIA 第 8 章**：故障与部分失效、不可靠网络 | 中文翻译：<https://github.com/awdoiudh/Designing-Data-Intensive-Applications-2nd-Edition/blob/main/ch8.md> | 重读"Faults and Partial Failures"和"Unreliable Networks"两个小节。重点关注：为什么分布式系统中故障不是"全有或全无"的。上周你读了概念，这周要带着"超时后重试到底安不安全"这个问题重读。 |
| 2 | **Jepsen Consistency Models**：Linearizability 定义 | <https://jepsen.io/consistency/models> | **只看 linearizability 的定义**，不用看其他模型。理解一句话："一旦一个写操作完成，所有之后的读操作必须能看到这个值。"这个定义会直接影响本周你如何处理 Put 的顺序和去重。 |
| 3 | **RPC 的概念**：什么是 RPC、和纯 TCP 消息的区别 | <https://en.wikipedia.org/wiki/Remote_procedure_call> | 读前两段即可。理解 RPC 的本质：让你像调用本地函数一样调用远程函数，但底层必须处理网络错误。 |

### 选读（卡住时再看）

| # | 资料 | 链接 |
|---|------|------|
| 4 | Python `json` 模块文档 | <https://docs.python.org/3/library/json.html> |
| 5 | 上周的 Real Python Socket Guide | <https://realpython.com/python-sockets/> |

---

## 本周要理解的核心概念

写代码前确保能回答：

1. **"超时后重试"和"请求去重"是什么关系？** 为什么说没有去重的重试是危险的？
2. **`client_id` 和 `seq` 各解决什么问题？** 为什么单独一个 `request_id`（像第 1 周那样）不够？
3. **Server 收到重复请求时应该返回什么？** 是重新执行操作，还是返回上次的结果？
4. **什么是线性一致性（linearizability）？** 它和"读最新数据"是什么关系？
5. **如果 Server 重启了，去重信息还在吗？** 如果不在，会发生什么？（这个问题留到第 3 周解决）

---

## 代码任务：简易 RPC 框架

### 整体架构

```
┌──────────┐                  ┌──────────────────┐
│  Client  │                  │     Server       │
│          │                  │                  │
│  ┌────┐  │   TCP (JSON)     │  ┌────────────┐  │
│  │RPC │  │ ◄──────────────► │  │Dispatcher  │  │
│  │Stub│  │   单连接复用       │  │            │  │
│  └────┘  │                  │  │ Ping()     │  │
│          │                  │  │ Echo()     │  │
│  timeout │                  │  │ Put(k,v)   │  │
│  retry   │                  │  │ Get(k)     │  │
│  client  │                  │  │            │  │
│  _id+seq │                  │  │ 去重表     │  │
│          │                  │  │ KV存储     │  │
└──────────┘                  │  └────────────┘  │
                              └──────────────────┘
```

### 协议定义（JSON 格式）

从第 1 周的文本协议升级到 JSON，原因：
- 本周需要传结构化数据（method、params、client_id、seq）
- 文本协议的解析规则（`request_id=1 body=hello`）不够用了
- 你能对比两种协议方式：文本适合简单场景，JSON 适合结构化场景

**请求格式：**

```json
{
    "request_id": 1,
    "client_id": "c1",
    "seq": 3,
    "method": "Put",
    "params": {
        "key": "x",
        "value": "1"
    }
}
```

**响应格式：**

```json
{
    "request_id": 1,
    "ok": true,
    "result": "OK",
    "error": ""
}
```

**字段说明：**

| 字段 | 谁生成 | 含义 |
|------|--------|------|
| `request_id` | Client | 单次请求的 ID，用于日志关联。不同 client 之间的 request_id 不冲突。 |
| `client_id` | Client | 客户端的唯一标识。Server 使用它隔离不同 client 的去重状态。 |
| `seq` | Client | 客户端自增的序列号。同一个 client 内，每次**新请求** seq+1。重试时 seq 不变。这是去重的关键。 |
| `method` | Client | 要调用的方法名 |
| `params` | Client | 方法参数 |
| `ok` | Server | 操作是否成功 |
| `result` | Server | 操作返回结果 |
| `error` | Server | 错误信息（ok=false 时） |

### ❗关键概念区分

```

很关键，想清楚这个：
```text
request_id ≠ 去重依据

request_id：用来关联日志，排查问题。
            不同 client 可以有相同的 request_id，不冲突。
            request_id 只是单次请求的身份证号。

去重依靠：client_id + seq
            同一个 client 内的同一个 seq 号，只能执行一次。
            不同 client 的相同 seq 号互不影响。
```

---

### Server 要实现什么

#### 第一步：协议框架

在第 1 周的 TCP echo server 基础上改造：

1. **TCP 层不变**：多线程 accept、recv、sendall 结构保留
2. **消息边界处理**：使用 `\n` 作为消息分隔符。因为 JSON 不像文本协议天然有边界，你需要显式处理。
3. **JSON 解析**：`json.loads()` 解析请求，`json.dumps()` 构造响应
4. **异常处理**：JSON 解析失败、字段缺失等情况都要处理，返回 `ok:false` 而不是崩溃

消息边界处理思路（重要）：

```text
TCP 是字节流，不是消息流。你 send 三次可能 recv 收到一坨。

解决方案：用 \n 做分隔符。

发送端：sendall(json_str + "\n")
接收端：维护一个 buffer，不断 recv 追加到 buffer，
        每次从 buffer 中按 \n 切分完整消息处理。

示例：
  recv 收到 "{"request_id":1,"method":"Ping"}\n{"request_id":2,"m"
  第一段是完整 JSON → 解析处理
  第二段 "{"request_id":2,"m" 不完整 → 留在 buffer 等下次 recv
```

#### 第二步：方法分发

支持四个方法：

| 方法 | 参数 | 返回 | 说明 |
|------|------|------|------|
| `Ping` | 无 | `"pong"` | 健康检查，不涉及去重 |
| `Echo` | `text` | 原样返回 `text` | 回响，不涉及去重 |
| `Put` | `key`, `value` | `"OK"` | 写操作，**需要去重** |
| `Get` | `key` | 存储的 `value`，不存在返回空 | 读操作，暂不需要去重 |

Server 内部维护：
- 一个 `dict` 作为 KV 存储：`data = {}`
- 一个去重表：记录每个 client 最后一次成功的请求

#### 第三步：去重逻辑（本周核心）

去重表结构：

```python
# key: client_id
# value: { "last_seq": int, "last_result": dict }
dedup_table = {}
```

处理流程：

```text
收到请求（method=Put, client_id="c1", seq=3）

1. 查去重表：client_id="c1" 是否存在？
   
   情况 A：不存在（第一次见这个 client）
     → 执行 Put
     → 记录：dedup_table["c1"] = {"last_seq": 3, "last_result": {"ok":true,"result":"OK"}}
     → 返回执行结果
     → 日志："[首次] client=c1 seq=3 Put x=1"
   
   情况 B：存在，且 seq > last_seq（新请求）
     → 执行 Put
     → 更新 last_seq 和 last_result
     → 返回执行结果
     → 日志："[首次] client=c1 seq=3 Put x=1"
   
   情况 C：存在，且 seq == last_seq（重复请求）
     → 不执行 Put！
     → 直接返回上次的 last_result
     → 日志："[重复] client=c1 seq=3 → 返回缓存结果"
   
   情况 D：存在，且 seq < last_seq（过期的请求，可能是网络延迟导致）
     → 不执行
     → 返回错误：ok=false, error="stale request"
     → 日志："[过期] client=c1 seq=3 < last_seq=5"

2. 去重只对 Put 生效。Ping、Echo、Get 不需要去重（它们没有副作用）。
```

**为什么不给 Ping/Echo/Get 去重？**

```text
去重的目的是防止"有副作用的操作被执行多次"。

Ping 和 Echo 没有副作用，多执行几次没关系。
Get 是只读操作，也没有副作用。（后面几周会讨论 Get 的线性一致性问题）

Put 不同：Put("x", "1") 执行两次，x 还是 "1"，数据上虽然没变，
但这里你想理解的是——server 不应该假装"第二次执行成功"。
实际分布式系统中，如果 Put 后面有其他操作（如 increment），
重复执行就会导致数据错误。
```

---

### Client 要实现什么

#### 第一步：RPC Stub

封装一个 RPC 调用函数，而不是每次手动构造消息：

```text
思路：
  定义一个函数 rpc_call(method, params) -> 响应结果
  
  这个函数内部做：
  1. 构造 JSON 请求
  2. 发送
  3. 等待响应
  4. 超时处理
  5. 重试处理
  6. 返回解析后的结果
```

Client 需要维护状态：
- `client_id`：固定值，如 `"c1"`，启动时生成（可用主机名+进程ID）
- `seq`：每次新请求 +1，重试时不变
- `request_id`：每次新请求 +1

#### 第二步：超时 + 重试

和第 1 周一样，但要增加对去重的理解：

```text
场景：Client 发送 Put(x=1)，client_id="c1"，seq=3
      5 秒超时，没有收到响应

Client 不知道发生了什么：
  - 可能请求根本没到 Server → 应该重试
  - 可能请求到了，Server 执行了，但响应丢了 → 应该重试，但 seq 不变！

重试时：
  - seq 保持不变！这是整个去重机制的核心
  - 如果用新 seq（seq=4），Server 会认为是新请求，可能重复执行
```

#### 第三步：交互式命令行

```text
支持命令：
  ping          → 调用 Ping()
  echo <text>   → 调用 Echo(text)
  put <key> <value> → 调用 Put(key, value)
  get <key>     → 调用 Get(key)
  quit          → 退出

每次命令 = 一次 RPC 调用 = seq+1
```

---

### 目录结构

```
week02_rpc/
├── README.md         # 启动说明 + 验收标准
├── design.md         # 设计文档
├── test_report.md    # 测试报告
├── docs/
│   └── week02_plan.md  # 本文件
├── server.py         # RPC Server
├── client.py         # RPC Client
├── protocol.py       # (可选) 共享的协议定义：请求/响应构造、解析
└── common.py         # (可选) 消息边界处理、buffer 逻辑
```

如果你想把客户端和服务端拆得更清楚，建议把消息编解码抽到 `protocol.py`，这样 server 和 client 都能复用：

```text
protocol.py：
  - build_request(request_id, client_id, seq, method, params) -> JSON 字符串 + \n
  - build_response(request_id, ok, result, error) -> JSON 字符串 + \n
  - parse_message(json_str) -> dict
  - MessageBuffer 类：处理 TCP 粘包，按 \n 切分完整消息

server.py：
  - from protocol import MessageBuffer, parse_message, build_response
  - 处理连接、分发方法、去重、KV 存储

client.py：
  - from protocol import build_request, parse_message
  - 交互式命令、超时、重试、seq 管理
```

---

## 故障测试

### 测试 1：基本 RPC 调用

```bash
# 终端 1
python3 server.py

# 终端 2
python3 client.py
> ping        # 预期：pong
> echo hello  # 预期：hello
> put x 1     # 预期：OK
> get x       # 预期：1
> get y       # 预期：(空或 not found)
```

### 测试 2：去重 — 重试不产生重复写

**这是本周最重要的测试。**

思路（写在测试脚本里）：不用交互式 client，写一个专用测试脚本：

```text
1. 启动 server
2. 发送 Put(x=1)，client_id="t1"，seq=1
3. 正常收到响应，确认 OK
4. 再次发送 Put(x=1)，client_id="t1"，seq=1（模拟重试：同一个 seq）
5. 收到响应，确认 OK
6. 发送 Put(x=1)，client_id="t1"，seq=2，不同 value
7. 正常收到响应
8. 发送 Get(x)
9. 检查 Get 的返回值：

   如果去重正确工作：
   - 第 2 次的 seq=1 请求直接返回缓存结果，不会覆盖 x 的值
   - 第 3 次的 seq=2 请求是新请求，会覆盖
   - 最终 Get(x) 应该返回 seq=2 的 value

   验证方法：在 server 日志中确认：
   - 看到 "[首次] client=t1 seq=1"
   - 看到 "[重复] client=t1 seq=1"
   - 看到 "[首次] client=t1 seq=2"
```

### 测试 3：Server 执行后不返回 → Client 重试

```text
1. 在 server 代码中加一个特殊处理：
   如果 Put 的 key 是 "slow"，就 time.sleep(10) 但不影响其他逻辑
   （这模拟了"server 执行了但响应慢"的场景）

2. client 超时设为 3 秒

3. 发送 Put(slow, 1)，client_id="t1"，seq=1
4. client 超时后重试，seq 仍然是 1
5. 第一次请求 server 还在 sleep 中...

问题：会出现两次 Put(slow, 1) 同时执行吗？还是一个先执行一个后执行？

答案取决于你的实现：
  - 如果去重检查在 sleep 之前：第二次请求查去重表时，第一次已经注册了 seq=1，
    第二次就会命中去重缓存，不重复执行。
  - 如果去重检查在 sleep 之后：第二次会尝试执行两次。

正确的做法：**先检查去重、注册 seq，再执行业务逻辑。**
```

### 测试 4：多 Client 独立 seq

```text
1. 启动 server
2. Client "c1" 发送 Put(x, "c1_value")，seq=1
3. Client "c2" 发送 Put(x, "c2_value")，seq=1
4. Client "c2" 发送 Get(x)
5. 预期：Get 返回 "c2_value"
6. Client "c1" 重新用 seq=1 发送 → 应该命中 c1 的去重缓存

关键验证：不同 client 的 seq 是独立计数的，c1 的 seq=1 和 c2 的 seq=1 互不影响。
```

### 测试 5：过期 seq 拒绝

```text
1. 发送 Put(x, 1)，client_id="t1"，seq=5，正常成功
2. 发送 Put(x, 2)，client_id="t1"，seq=3（模拟网络延迟导致旧请求后到达）
3. 预期：返回 ok=false，error="stale request"
4. 验证：Get(x) 应该还是 1（seq=3 的请求没有执行）
```

---

## 验收标准

完成以下所有项才算本周过关：

- [ ] Server 支持 Ping、Echo、Put、Get 四个方法
- [ ] 同一个 `client_id + seq` 的 Put **只执行一次**，重复请求返回缓存结果
- [ ] Client 超时后重试使用**相同 seq**，不会导致重复执行
- [ ] 不同 client 的 seq 互相独立，互不影响
- [ ] Server 日志能区分 `[首次]`、`[重复]`、`[过期]` 请求
- [ ] 过期 seq（seq < last_seq）被拒绝，返回错误
- [ ] 所有 5 个故障测试场景手动执行通过
- [ ] Server 不因为 JSON 解析失败、字段缺失等异常崩溃

---

## 本周不做

1. **不做 WAL 持久化** → 那是第 3 周的事。本周去重表在内存里，Server 重启后去重信息丢失。
2. **不做 protobuf / gRPC** → JSON 就够。
3. **不做连接池** → 每次发请求建新连接即可（和上周一样）。
4. **不做多 server 复制** → 这是单节点 RPC，复制是第 5 周的事。
5. **不做 Raft** → 那是第 9 周的事。
6. **不做并发去重的线程安全** → 本周先做简单的，不要求一把大锁保护去重表做原子操作。
7. **不做消息 body 太大分片** → 消息都是小 JSON，`recv(4096)` 够用。

---

## 设计文档大纲（design.md）

```markdown
# Week 2 设计文档

## 协议设计
- 为什么从文本协议升级到 JSON
- 为什么用 client_id + seq 而不是单独 request_id 去重
- 消息边界方案（为什么用 \n，有没有其他选择）

## Server 设计
- 方法分发机制
- 去重表结构和去重流程（含时序图）
- 为什么去重检查要在业务逻辑之前

## Client 设计
- RPC stub 的实现思路
- 超时 + 重试策略
- seq 管理规则

## 遇到的问题和解决思路
```

## 测试报告大纲（test_report.md）

```markdown
# Week 2 测试报告

## 测试环境

## 测试场景和结果

### 测试 1：基本 RPC 调用
### 测试 2：去重 — 重试不产生重复写
### 测试 3：Server 执行后不返回 → Client 重试
### 测试 4：多 Client 独立 seq
### 测试 5：过期 seq 拒绝

每个测试：操作步骤、预期结果、实际结果、是否通过

## 发现的 Bug

## 未解决的问题
```

---

## 时间建议

| 时间段 | 做什么 | 预计耗时 |
|--------|--------|---------|
| 周一 | 重读 DDIA 第 8 章 + 看 Jepsen linearizability 定义 | 1 小时 |
| 周二 | 写 protocol.py（消息编解码 + buffer）+ server 骨架 | 1.5 小时 |
| 周三 | 实现 server 去重逻辑 + 方法分发 | 1 小时 |
| 周四 | 写 client RPC stub + 超时重试 + seq 管理 | 1.5 小时 |
| 周五 | 手动执行全部 5 个故障测试 | 1 小时 |
| 周六 | 修 bug + 写 design.md + test_report.md | 2~3 小时 |
| 周日 | 整体回顾，对比第 1 周：从"网络不可靠"到"请求可能执行了但响应丢了" | 2 小时 |
