# Week 2 设计文档

## 1. 协议设计

### 为什么从文本协议升级到 length-prefix JSON

Week 1 使用换行分隔的文本协议：

```text
request_id=1 body=hello\n
```

优点：实现简单，肉眼可读。
问题：body 中不能包含 `\n`（需要转义），不适合后续的二进制数据（WAL、snapshot）。

Week 2 升级为 **length-prefix JSON**：

```
[4 bytes big-endian length][json bytes]
```

- **不需要转义**：4 字节长度头明确标定消息边界，body 可以是任意字节。
- **二进制安全**：后续 WAL 记录、snapshot 可以内嵌二进制数据。
- **解析高效**：先读 4 字节知道长度，再精确读取 body，不会出现半包解析。

### 为什么用 `readexactly` 而不是 `read`

TCP 是字节流，没有消息边界。`reader.read(n)` 最多读 n 字节，可能只返回 2 字节（TCP 分包）。`reader.readexactly(n)` 保证读满 n 字节，否则抛 `IncompleteReadError`。

```python
# 正确
header = await reader.readexactly(4)

# 错误
header = await reader.read(4)  # 可能只返回 2 字节
```

### 为什么用 client_id + seq 去重

| 字段 | 用途 |
|------|------|
| `request_id` | 日志关联、问题排查。不同 client 可相同，不用于去重。 |
| `client_id` | 标识哪个客户端，隔离不同 client 的去重空间。 |
| `seq` | 同一 client 内单调递增。新请求 +1，重试不变。去重的核心依据。 |

只用 `request_id` 不够——不同 client 可以有相同的 `request_id`，且重试时 `request_id` 可能不同但语义相同。

### 请求/响应格式

请求：
```json
{
    "request_id": 1,
    "client_id": "c1",
    "seq": 3,
    "method": "Put",
    "params": {"key": "x", "value": "1"}
}
```

响应：
```json
{
    "request_id": 1,
    "ok": true,
    "result": "OK",
    "error": ""
}
```

---

## 2. Server 设计

### 并发模型

Week 1 使用 `threading`（每个连接一个线程）。Week 2 起全面切换到 `asyncio`：

- `asyncio.start_server` 自动为每个连接创建协程
- `asyncio.Lock` 保护 `dedup_table` 和 `hash_map`（单线程事件循环 + 协程间互斥）
- 不使用 `threading.Lock`、不引入多线程

### 方法分发

| 方法 | 参数 | 返回 | 去重 |
|------|------|------|------|
| `Ping` | 无 | `"pong"` | 否 |
| `Echo` | `value` | 原样返回 | 否 |
| `Put` | `key`, `value` | `"OK"` | 是 |
| `Get` | `key` | value 或错误 | 否 |

### 去重流程

```
收到 Put(client_id=c1, seq=3)
  │
  ├─ dedup_table[c1] 不存在？ → 执行 Put，记录 last_seq=3
  │
  ├─ seq > last_seq？         → 执行 Put，更新 last_seq=3
  │
  ├─ seq == last_seq？        → 返回缓存 last_result（不执行）
  │
  └─ seq < last_seq？         → 返回错误 "seq 回退，拒绝处理"
```

**关键设计决策：去重检查在业务逻辑之前。** 如果先执行业务再检查去重，慢请求（如 sleep 10 秒）会绕过防重机制。

### 异常处理

| 异常 | 处理方式 |
|------|---------|
| `asyncio.IncompleteReadError` | 客户端断开，关闭连接 |
| `json.JSONDecodeError` | 非法 JSON，打印日志，关闭连接 |
| 缺少必填字段 | 返回 `ok: false` + 错误信息，保持连接 |
| 未知方法 | 返回 `ok: false` |

---

## 3. Client 设计

### 超时 + 重试策略

```
call(method, params)
  │
  ├─ seq += 1, request_id += 1
  ├─ current_seq = self.seq  ← 重试时不变
  │
  └─ 重试循环 (max 3 次)
       │
       ├─ 建立连接 → 发送请求 → 等待响应（timeout 秒）
       │
       ├─ 成功 → 返回结果
       │
       ├─ TimeoutError → current_req_id += 1 → 关闭旧连接 → 重试
       │
       ├─ ConnectionRefusedError → 退出（服务不可用）
       │
       └─ 其他异常 → 关闭连接 → 重试
```

**关键规则：**
- `seq` 重试时不变（去重核心）
- `request_id` 重试时递增（日志关联）
- 每次重试使用新连接（旧连接可能半死不活）

---

## 4. Network Simulator 设计

### 架构

Network Simulator 在应用层模拟网络行为：

```python
class Network:
    def add_rule(src, dst, delay_ms, drop_rate)  # 设置链路属性
    def reset()                                    # 清除所有规则
    async def send(src, dst, message) -> bytes     # 可能延迟/丢包
```

### 为什么从 Week 2 开始做

而非等到 Week 12：
- 故障注入不应依赖"手动改代码加 sleep 然后重启"
- 从早期就建立可控的故障环境
- 后续周的测试（primary-backup、Raft）都依赖它

### 当前状态

`Network` 类已实现（delay + drop），但尚未集成到 RPC 通信链路中。当前 client 和 server 直接通过 TCP 通信，不经过 Network 层。集成工作放在后续周进行。

---

## 5. 目录结构

```
week02_rpc/
├── rpc/
│   ├── codec.py             # encode_message / decode_message
│   ├── client.py            # Client: 超时、重试、seq 管理
│   └── server.py            # Server: 方法分发、去重、KV 存储
├── network/
│   └── simulator.py         # Network Simulator（delay, drop）
├── tests/
│   ├── test_codec.py        # 编解码（5 场景）
│   ├── test_timeout.py      # 超时行为（2 场景）
│   ├── test_retry.py        # 重试 + 去重（1 场景）
│   └── test_duplicate_request.py  # 去重完整测试（4 场景）
├── server_main.py           # 启动入口
├── client_main.py           # 交互式命令行
├── README.md                # 使用说明
├── design.md                # 本文档
└── test_report.md           # 测试报告
```
