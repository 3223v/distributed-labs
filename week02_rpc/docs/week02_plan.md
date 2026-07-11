# Week 2：length-prefix JSON RPC + 故障注入雏形

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

本周你要实现一个 RPC 层（length-prefix JSON 编解码 + 超时 + 重试 + client_id/seq 去重），并开始做一个应用层 Network Simulator，让故障注入不再是"手动改代码"。

---

## 阅读清单

### 必读（2~3 小时）

| # | 资料 | 链接 | 读什么 |
|---|------|------|--------|
| 1 | **DDIA 第 8 章**：故障与部分失效、不可靠网络 | 中文翻译：<https://github.com/awdoiudh/Designing-Data-Intensive-Applications-2nd-Edition/blob/main/ch8.md> | 重读"Faults and Partial Failures"和"Unreliable Networks"。带着"超时后重试到底安不安全"这个问题重读。 |
| 2 | **Jepsen Consistency Models**：Linearizability 定义 | <https://jepsen.io/consistency/models> | **只看 linearizability 的定义**。理解一句话："一旦一个写操作完成，所有之后的读操作必须能看到这个值。" |
| 3 | Python `asyncio` 文档 — Streams | <https://docs.python.org/3/library/asyncio-stream.html> | 重点看 `asyncio.start_server`、`asyncio.connect`、`StreamReader.read()` vs `StreamReader.readexactly()` 的区别。 |

### 选读（卡住时再看）

| # | 资料 | 链接 |
|---|------|------|
| 4 | Python `struct` 模块 | <https://docs.python.org/3/library/struct.html>（用于 4 字节 big-endian 编解码）|
| 5 | Python `json` 模块文档 | <https://docs.python.org/3/library/json.html> |

---

## 本周要理解的核心概念

1. **"超时后重试"和"请求去重"是什么关系？** 为什么说没有去重的重试是危险的？
2. **`client_id` 和 `seq` 各解决什么问题？** 为什么单独一个 `request_id`（像第 1 周那样）不够？
3. **Server 收到重复请求时应该返回什么？** 是重新执行操作，还是返回上次的结果？
4. **TCP 是字节流，为什么 `read(n)` 不够？** 什么场景下会出问题？`readexactly(n)` 做了什么保证？
5. **什么是线性一致性（linearizability）？** 它和"读最新数据"是什么关系？

---

## 代码任务

### 整体架构

```
┌──────────────────────┐        length-prefix JSON         ┌──────────────────────┐
│       Client          │                                   │       Server          │
│                       │       [4字节长度][JSON]            │                       │
│  ┌─────────────────┐  │ ◄──────────────────────────────► │  ┌─────────────────┐  │
│  │ RpcClient       │  │       单连接复用                   │  │ RpcServer       │  │
│  │  - timeout       │  │                                   │  │  - 方法分发      │  │
│  │  - retry         │  │                                   │  │  - 去重表        │  │
│  │  - client_id+seq │  │                                   │  │  - KV 存储       │  │
│  └─────────────────┘  │                                   │  └─────────────────┘  │
│                       │                                   │                       │
│  ┌─────────────────┐  │                                   │                       │
│  │ Network(可选)   │  │  应用层网络模拟                      │                       │
│  │  - delay         │  │  （本周只做雏形）                     │                       │
│  │  - drop          │  │                                   │                       │
│  └─────────────────┘  │                                   │                       │
└──────────────────────┘                                   └──────────────────────┘
```

### 编码格式：length-prefix JSON

从第 1 周的文本协议升级到 **length-prefix JSON**：

```
[4 bytes big-endian length][json bytes]
```

示例（发送 `{"method":"Ping"}`）：

```
\x00\x00\x00\x14{"method":"Ping"}
  │              │
  └─ 长度=20 ────└─ 20 字节的 JSON
```

#### 为什么是 length-prefix 而不是 `\n` 分隔？

| 方案 | 优点 | 缺点 |
|------|------|------|
| `\n` 分隔 | 实现简单，人类可读 | JSON body 里的字符串可能包含 `\n`，需要转义 |
| length-prefix | 不需要转义，二进制安全，解析高效 | 需要处理半包（收到了 2 字节的 header） |

后面的周会做二进制数据（snapshot、WAL），length-prefix 是更好的选择。

#### 写入（发送端）

```python
import struct, json

def encode_message(obj: dict) -> bytes:
    body = json.dumps(obj).encode("utf-8")
    header = struct.pack(">I", len(body))  # big-endian unsigned int
    return header + body

# 发送
writer.write(encode_message(request))
await writer.drain()
```

#### 读取（接收端）— 关键

```python
async def decode_message(reader: asyncio.StreamReader) -> dict:
    # 必须用 readexactly，不能用 read！
    header = await reader.readexactly(4)
    size = struct.unpack(">I", header)[0]
    body = await reader.readexactly(size)
    return json.loads(body.decode("utf-8"))
```

**为什么必须 `readexactly`：**

```text
TCP 是字节流，没有消息边界。

假设你发送了一个 100 字节的消息：
  reader.read(4)  可能返回 2 字节（TCP 分包了） → 解析 size 错误
  reader.readexactly(4) 一定返回 4 字节 → 正确

reader.read(n) 的语义：最多读 n 字节，可能读不够
reader.readexactly(n) 的语义：必须读满 n 字节，否则抛 IncompleteReadError
```

---

### 目录结构

```
week02_rpc/
├── README.md
├── design.md
├── test_report.md
├── docs/
│   └── week02_plan.md      # 本文件
├── rpc/
│   ├── __init__.py
│   ├── codec.py            # encode_message / decode_message
│   ├── client.py           # RpcClient: timeout, retry, seq 管理
│   └── server.py           # RpcServer: 方法分发, 去重
├── network/
│   ├── __init__.py
│   └── simulator.py        # Network: delay, drop, partition (雏形)
├── tests/
│   ├── test_codec.py        # 编码解码正确性
│   ├── test_timeout.py      # 超时行为
│   ├── test_retry.py        # 重试不重复执行
│   └── test_duplicate_request.py  # 去重正确性
├── server_main.py           # 启动 server 的入口
└── client_main.py           # 交互式 client 入口
```

---

### RPC 请求格式

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

### RPC 响应格式

```json
{
    "request_id": 1,
    "ok": true,
    "result": "OK",
    "error": ""
}
```

### 字段说明

| 字段 | 谁生成 | 含义 |
|------|--------|------|
| `request_id` | Client | 单次请求的 ID，日志关联用。不同 client 之间不冲突。 |
| `client_id` | Client | 客户端唯一标识。Server 用它隔离不同 client 的去重状态。 |
| `seq` | Client | **去重的关键**。同一个 client 内，每次新请求 seq+1，重试时 seq 不变。 |
| `method` | Client | 要调用的方法名 |
| `params` | Client | 方法参数 |
| `ok` | Server | 操作是否成功 |
| `result` | Server | 操作返回结果 |
| `error` | Server | 错误信息（ok=false 时） |

### ❗ 关键概念区分

```text
request_id ≠ 去重依据

request_id：用来关联日志，排查问题。
            不同 client 可以有相同的 request_id，不冲突。

去重依靠：client_id + seq
            同一个 client 内的同一个 seq 号，只能执行一次。
            不同 client 的相同 seq 号互不影响。
```

---

## 第一部分：rpc/codec.py — 编解码

这是最基础的模块，server 和 client 都依赖它。

```python
# rpc/codec.py

import struct
import json

def encode_message(obj: dict) -> bytes:
    """将 dict 编码为 length-prefix JSON bytes"""
    body = json.dumps(obj).encode("utf-8")
    header = struct.pack(">I", len(body))
    return header + body

async def decode_message(reader: asyncio.StreamReader) -> dict:
    """从 StreamReader 读取一个 length-prefix JSON 消息"""
    header = await reader.readexactly(4)
    size = struct.unpack(">I", header)[0]
    body = await reader.readexactly(size)
    return json.loads(body.decode("utf-8"))
```

测试要点：
- 正常编解码往返
- 大消息（>64KB 的 value）
- 空 body 的场景

---

## 第二部分：rpc/server.py — RPC Server

### 基于 asyncio

```python
# 使用 asyncio.start_server，每个连接一个协程（不是线程）
async def handle_client(reader, writer):
    while True:
        try:
            req = await decode_message(reader)
        except asyncio.IncompleteReadError:
            break  # 客户端断开
        resp = dispatch(req)
        writer.write(encode_message(resp))
        await writer.drain()

async def main():
    server = await asyncio.start_server(handle_client, "0.0.0.0", 8000)
    async with server:
        await server.serve_forever()
```

注意和 week01 的区别：**不是 threading，是 asyncio 协程**。一个协程卡住不会阻塞其他协程，但如果在协程里调用同步阻塞函数（如 `time.sleep(10)`），就会阻塞整个事件循环。用 `await asyncio.sleep(10)` 代替。

### 方法分发

支持四个方法：

| 方法 | 参数 | 返回 | 说明 |
|------|------|------|------|
| `Ping` | 无 | `"pong"` | 健康检查，不涉及去重 |
| `Echo` | `text` | 原样返回 `text` | 回响，不涉及去重 |
| `Put` | `key`, `value` | `"OK"` | 写操作，**需要去重** |
| `Get` | `key` | 存储的 `value` | 读操作，暂不需要去重 |

### 去重逻辑（本周核心）

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
     → 记录：dedup_table["c1"] = {"last_seq": 3, "last_result": {...}}
     → 日志："[首次] client=c1 seq=3 Put x=1"

   情况 B：存在，且 seq > last_seq（新请求）
     → 执行 Put
     → 更新 last_seq 和 last_result
     → 日志："[首次] client=c1 seq=3 Put x=1"

   情况 C：存在，且 seq == last_seq（重复请求）
     → 不执行 Put！
     → 直接返回上次的 last_result
     → 日志："[重复] client=c1 seq=3 → 返回缓存结果"

   情况 D：存在，且 seq < last_seq（过期请求）
     → 不执行
     → 返回 ok=false, error="stale request"
     → 日志："[过期] client=c1 seq=3 < last_seq=5"

2. 去重只看 Put（有副作用的操作）。Ping、Echo、Get 不需要去重。
```

**关键：去重检查必须在业务逻辑之前。** 如果先执行业务逻辑再检查去重，慢请求就会绕过去重。

---

## 第三部分：rpc/client.py — RPC Client

### RpcClient 类

封装超时、重试、seq 管理：

```python
class RpcClient:
    def __init__(self, server_host, server_port, client_id, timeout=5.0):
        self.client_id = client_id
        self.seq = 0          # 每次新请求 +1，重试不变
        self.request_id = 0   # 每次新请求 +1，重试也 +1
        self.timeout = timeout
        self.max_retries = 3

    async def call(self, method: str, params: dict = None) -> dict:
        """发送一次 RPC 调用，自动处理超时和重试"""
        self.seq += 1                     # 新请求，seq+1
        self.request_id += 1
        return await self._call_with_retry(method, params, self.seq)

    async def _call_with_retry(self, method, params, seq):
        """内部重试循环，seq 不变"""
        for attempt in range(self.max_retries):
            try:
                # 建立连接、发送、等待响应
                ...
            except asyncio.TimeoutError:
                print(f"[TIMEOUT] request_id={self.request_id} attempt={attempt+1}")
            except ConnectionRefusedError:
                print("服务端未启动")
                break
        return {"ok": False, "error": "max retries exceeded"}
```

**重试时的关键规则：**
- `seq` **保持不变**（这是去重机制的核心）
- 每次重试使用**新连接**（旧连接可能已半死不活）
- `request_id` 可以递增（仅用于日志关联）

### 交互式命令行

```
> ping
< pong

> echo hello world
< hello world

> put x 1
< OK

> get x
< 1

> quit
```

---

## 第四部分：network/simulator.py — Network Simulator 雏形

目标：不让故障测试依赖"手动改代码、加 sleep、手动 kill 进程"。

本周只做最简单的版本：

```python
class Network:
    """管理所有节点之间的消息传递，支持注入 delay 和 drop"""

    def __init__(self):
        self._rules = {}  # (src, dst) -> {"delay_ms": int, "drop_rate": float}

    def add_rule(self, src: str, dst: str, delay_ms: int = 0, drop_rate: float = 0.0):
        """设置 src → dst 的链路属性"""
        self._rules[(src, dst)] = {"delay_ms": delay_ms, "drop_rate": drop_rate}

    def reset(self):
        """清除所有规则"""
        self._rules.clear()

    async def send(self, src: str, dst: str, message: bytes) -> Optional[bytes]:
        """模拟发送消息：可能延迟、可能丢弃"""
        rule = self._rules.get((src, dst), {})
        delay_ms = rule.get("delay_ms", 0)
        drop_rate = rule.get("drop_rate", 0.0)

        # 丢包
        if random.random() < drop_rate:
            return None

        # 延迟
        if delay_ms > 0:
            await asyncio.sleep(delay_ms / 1000.0)

        return message  # 到达
```

本周 Network Simulator 是可选的——你可以直接在 client 和 server 之间用 TCP 通信，在测试中手动注入延迟。但从第 5 周开始，Network Simulator 会成为主要的测试手段。

---

## 故障测试

### 测试 1：codec 正确性

```text
1. encode → decode 往返一致性
2. 大消息（64KB value）不丢数据
3. 故意发半包 → readexactly 抛 IncompleteReadError
```

### 测试 2：基本 RPC 调用

```bash
python3 server_main.py
python3 client_main.py
> ping        # pong
> echo hello  # hello
> put x 1     # OK
> get x       # 1
> get y       # (空)
```

### 测试 3：去重 — 重试不产生重复写（本周最重要）

```text
1. Client(c1) 发送 Put(x=1), seq=1 —— 正常成功
2. Client(c1) 再次发送 Put(x=1), seq=1 —— 应返回缓存结果, x 还是 1
3. Client(c1) 发送 Put(x=2), seq=2 —— 新请求, x 变成 2
4. Get(x) → "2"

在 server 日志中验证：
  "[首次] client=c1 seq=1"
  "[重复] client=c1 seq=1"
  "[首次] client=c1 seq=2"
```

### 测试 4：慢响应 → 超时 → 重试 → 去重

```text
1. Server 对 key="slow" 的 Put 做 10 秒 sleep
2. Client 超时设为 3 秒
3. Client 发送 Put(slow, 1), seq=1
4. Client 3 秒后超时, 用相同 seq=1 重试
5. 第二次请求命中去重表 → 不重复执行 → 返回结果
6. 验证：server 日志中第二次请求显示 "[重复]"

关键验证点:
  - 去重检查在 sleep 之前 → 第二次请求不会同时执行
  - 如果去重检查在 sleep 之后 → 会有竞态问题（错误）
```

### 测试 5：多 Client 独立 seq

```text
1. Client(c1) Put(x, "c1_value") seq=1
2. Client(c2) Put(x, "c2_value") seq=1
3. Client(c2) Get(x) → "c2_value"
4. Client(c1) 重发 seq=1 → 命中 c1 的去重缓存（不是 c2 的）
```

### 测试 6：过期 seq 拒绝

```text
1. Put(x, 1), seq=5, 成功
2. Put(x, 2), seq=3, 应返回 ok=false, error="stale request"
3. Get(x) → "1"（seq=3 没被执行）
```

---

## 验收标准

- [ ] length-prefix 编解码正确（codec 测试通过）
- [ ] Server 支持 Ping、Echo、Put、Get 四个方法
- [ ] 同一个 `client_id + seq` 的 Put **只执行一次**，重复请求返回缓存结果
- [ ] Client 超时后重试使用**相同 seq**，不会导致重复执行
- [ ] 不同 client 的 seq 互相独立
- [ ] Server 日志能区分 `[首次]`、`[重复]`、`[过期]`
- [ ] 过期 seq 被拒绝
- [ ] 所有 6 个故障测试场景手动执行通过
- [ ] Server 不因 JSON 解析失败、字段缺失而崩溃

---

## 本周不做

1. **不做 WAL 持久化** → 第 3 周。去重表在内存里，重启丢失。
2. **不做 protobuf / gRPC** → length-prefix JSON 就够。
3. **不做多 server 复制** → 单节点 RPC，复制是第 5 周。
4. **不做 Raft** → 第 9 周。
5. **不做复杂 partition / heal** → Network Simulator 本周只做 delay + drop。
6. **不做消息过大分片** → 本周消息都是小 JSON。

---

## 设计文档大纲（design.md）

```markdown
# Week 2 设计文档

## 协议设计
- 为什么从文本协议升级到 length-prefix JSON
- 为什么 length-prefix 优于 \n 分隔
- 为什么用 client_id + seq 而不是单独 request_id 去重
- readexactly vs read

## Server 设计
- asyncio 协程模型 vs week01 threading 的对比
- 方法分发机制
- 去重表结构和去重流程（含流程图）
- 为什么去重检查要在业务逻辑之前

## Client 设计
- RpcClient 的接口设计
- 超时 + 重试策略
- seq 管理规则（新请求 +1，重试不变）

## Network Simulator 设计
- 架构：在应用层模拟网络行为
- 支持 delay / drop
- 为什么从第 2 周就开始做而不是第 12 周

## 遇到的问题和解决思路
```

## 测试报告大纲（test_report.md）

```markdown
# Week 2 测试报告

## 测试环境

## 测试场景和结果
### 测试 1：codec 正确性
### 测试 2：基本 RPC 调用
### 测试 3：去重 — 重试不产生重复写
### 测试 4：慢响应 → 超时 → 重试 → 去重
### 测试 5：多 Client 独立 seq
### 测试 6：过期 seq 拒绝

每个测试：操作步骤、预期结果、实际结果、是否通过、日志关键行

## 发现的 Bug

## 未解决的问题
```

---

## 时间建议

| 时间段 | 做什么 | 预计耗时 |
|--------|--------|---------|
| 周一 | 读 asyncio Streams 文档 + 回顾 DDIA 第 8 章 | 1 小时 |
| 周二 | 写 `rpc/codec.py` + 测试，确认编解码正确 | 1 小时 |
| 周三 | 写 `rpc/server.py`：方法分发 + 去重逻辑 | 1.5 小时 |
| 周四 | 写 `rpc/client.py`：超时 + 重试 + seq 管理 | 1.5 小时 |
| 周五 | 手动执行全部 6 个故障测试 | 1 小时 |
| 周六 | 修 bug + 写 design.md + test_report.md | 2~3 小时 |
| 周日 | 整体回顾 + Network Simulator 雏形（可选） | 2 小时 |
