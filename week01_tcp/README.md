# Week 1：TCP Echo Server — 网络、超时、连接生命周期

## 本周目标

理解分布式系统最底层的问题：**网络不可靠**。

具体来说，你要亲身体验：

1. TCP 连接可能在任何时刻断开
2. 你发送的数据对方不一定收到
3. 对方收到了但响应可能永远回不来
4. 没有超时机制的代码在网络异常时会永久卡住
5. 重试是必要的，但不是没有代价的

本周代码量很小（200~300 行 Python），重点是**理解行为**，不是做功能。

---

## 阅读清单

按优先级排列，不是全部必读。

### 必读（2~3 小时）

| # | 资料 | 链接 | 读什么 |
|---|------|------|--------|
| 1 | **Distributed Systems for Fun and Profit** 第 1、2 章 | 英文原版：<http://book.mixu.net/distsys/single-page.html><br>中文翻译（推荐）：<https://github.com/wilsonwen/distsysbook_ch> | 第 1 章理解"分布式系统为什么难"；第 2 章理解系统模型和抽象层级。不要跳读，这两章是所有后续内容的基础。 |
| 2 | **DDIA 第 8 章**（网络和时钟部分） | 英文原版（付费）：<https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/ch08.html><br>中文翻译（免费）：<https://github.com/awdoiudh/Designing-Data-Intensive-Applications-2nd-Edition/blob/main/ch8.md> | 只读以下小节：**Faults and Partial Failures**、**Unreliable Networks**、**Unreliable Clocks**。重点理解"部分失败"这个概念——单机代码要么成功要么失败，分布式代码可能部分成功部分失败。 |

### 选读（卡住时再看）

| # | 资料 | 链接 | 读什么 |
|---|------|------|--------|
| 3 | Real Python — Socket Programming Guide | <https://realpython.com/python-sockets/> | Python socket API 参考。不需要通读，写代码时当手册查。重点关注 `socket()`、`bind()`、`listen()`、`accept()`、`send()`、`recv()`、`settimeout()` 这几个函数。 |
| 4 | Python 官方文档 — socket 模块 | <https://docs.python.org/3/library/socket.html> | 当参考手册查，不需要通读。 |

---

## 本周要理解的核心概念

在写代码之前，确保你能回答这些问题（不用写下来，脑子里过一遍）：

1. **为什么单机程序的错误处理模型在分布式系统中失效？**（提示：单机要么全成功要么全失败，分布式可能出现部分失败）
2. **TCP 是可靠传输协议，为什么我们还说"网络不可靠"？**（提示：TCP 保证数据不丢不重不乱序，但它不能保证连接不断开、对端不崩溃、响应不超时）
3. **timeout 的本质是什么？**（提示：不是"对方太慢"，而是"我不想无限等下去"）
4. **重试有什么风险？**（提示：如果请求已经执行了但响应丢了，重试会导致重复执行）
5. **什么是 request_id？为什么要它？**（提示：日志关联、问题排查）

---

## 代码任务：TCP Echo Server

### 整体架构

```
┌──────────┐     TCP      ┌──────────┐
│  Client  │ ──────────── │  Server  │
│          │   request    │          │
│          │ ───────────> │          │
│          │              │          │
│          │   response   │          │
│          │ <─────────── │          │
└──────────┘              └──────────┘
                             │
                             │ 同时服务多个 client
                             │ 每个 client 一个线程
                             ▼
                    ┌─────────────────┐
                    │ ClientHandler 1 │
                    │ ClientHandler 2 │
                    │ ClientHandler 3 │
                    │      ...        │
                    └─────────────────┘
```

### 通信协议（文本格式，不用 JSON）

请求格式：

```text
request_id=1 body=hello
```

响应格式：

```text
request_id=1 ok=true body=hello
```

协议要点：
- 一行一条消息，用 `\n` 分隔
- `request_id` 是整数，由 client 生成，单调递增
- `body` 是任意字符串（不含 `\n`）
- `ok=true` 表示成功，`ok=false` 表示失败

选择文本协议而不是 JSON 的原因：
- 第一周不要引入序列化复杂度
- 手动解析字符串让你更清楚"协议"是什么
- 后面几周会演进到 JSON，到时候你能对比两种方式的差异

### Server 要做什么

**第一步：最简 Echo Server（单 client）**

实现思路：
1. 创建 TCP socket，绑定 `127.0.0.1:8000`
2. `listen()` 开始监听
3. `accept()` 等待连接
4. 收到连接后，循环 `recv()` 读取数据
5. 解析 `request_id` 和 `body`
6. 打印日志：`[INFO] client=127.0.0.1:54321 request_id=1 body=hello`
7. 原样构造响应返回
8. client 断开后回到步骤 3

这一步验证：你能正确创建 TCP 连接、发送数据、接收响应。

**第二步：支持多 client 并发**

单 client 的问题：第一个 client 连着的时候，第二个 client 无法连接。

解决方案（选一种）：
- **方案 A（推荐）**：每 accept 一个连接，创建一个 `threading.Thread` 处理
- **方案 B**：使用 `socketserver.ThreadingTCPServer`
- **方案 C**：使用 `asyncio`（如果你已经熟悉协程）

推荐方案 A，因为：
- 代码最直观，你完全掌控每个线程的生命周期
- 能直接看到"并发"的本质
- 不引入额外框架概念

注意点：
- 线程函数里要 `try/except` 包裹所有逻辑，一个 client 崩了不能影响其他 client
- 处理好线程退出：client 断开时线程要及时结束
- 不需要做线程池，第一周 client 数量很少

**第三步：增加健壮性**

重点放在这些异常处理上：

| 场景 | 会发生什么 | 你要怎么处理 |
|------|-----------|-------------|
| client 发一半断开 | `recv()` 返回空字节 `b''` 或抛出 `ConnectionResetError` | 捕获异常，关闭 socket，线程退出 |
| client 发送空行 | 解析失败 | 忽略空行，继续等待下一个请求 |
| client 发送格式错误的数据 | 解析不到 `request_id` 或 `body` | 返回 `request_id=0 ok=false body=parse error` |
| server 处理很慢 | client 等不及 | 这是 client 侧的事情，server 不需要特殊处理 |

### Client 要做什么

**第一步：最简 Client**

实现思路：
1. 创建 TCP socket，连接 `127.0.0.1:8000`
2. 构造请求字符串，`send()` 发送
3. `recv()` 接收响应
4. 打印响应
5. 关闭连接，退出

这一步验证：你能从 client 发请求到 server 并收到响应。

**第二步：增加 send 循环**

不要让 client 只发一条就退出。改成：
1. 从命令行读取用户输入（或从预定义列表读取）
2. 每条输入构造一个请求，`request_id` 递增
3. 发送、接收、打印
4. 输入 `quit` 时退出

**第三步：增加超时**

这是本周最关键的概念之一。

实现思路：
1. 在 `connect()` 之前设置 socket 超时：`sock.settimeout(5.0)`（5 秒）
2. 如果 `connect()`、`send()`、`recv()` 超过 5 秒没完成，Python 会抛出 `socket.timeout`
3. 捕获 `socket.timeout`，打印 `[TIMEOUT] request_id=3`，而不是让程序卡死

测试方法：
- 在 server 处理函数里手动 `time.sleep(10)` 模拟慢处理
- 观察 client 是否真的在 5 秒后超时报错

**第四步：增加重试**

超时后怎么办？重试。

实现思路：
1. 定义最大重试次数（如 3 次）
2. 如果 `connect()` 或 `send()` 或 `recv()` 超时：
   - 打印重试日志
   - 关闭当前 socket
   - 创建新 socket
   - 重新连接
   - 重新发送**同一个** `request_id`
3. 超过最大重试次数后放弃，打印 `[FAILED] request_id=3 after 3 retries`

重试策略要点：
- 每次重试之间加一个短暂间隔（如 0.5 秒），不要疯狂重试
- 重试时使用**新连接**，旧连接可能已经半死不活
- 保留原始 `request_id`，不要递增

---

## 故障测试（手动执行）

这是本周最重要的部分。代码写得再漂亮，不测这些场景等于白写。

### 测试 1：正常收发

```bash
# 终端 1
python3 server.py

# 终端 2
python3 client.py
# 输入 hello、world、foo 等
```

预期：
- server 打印每个请求的 `client_addr`、`request_id`、`body`
- client 打印每个响应
- 多次收发都正常

### 测试 2：Client 连接后立刻断开（不发送数据）

操作：
1. 启动 server
2. client `connect()` 成功后立刻 `close()`
3. 观察 server 行为

预期：server 不崩溃，对应 client 线程正常退出。

### 测试 3：Client 发送一半后断开

操作：
1. 使用 `nc`（netcat）或自己写一个脚本
2. 连接到 server
3. 发送 `"request_id=1 bo"`（不完整的消息，没有 `\n`）
4. 直接断开（Ctrl+C 或 kill 进程）

预期：server 不崩溃。

### 测试 4：Server 处理慢 → Client 超时

操作：
1. 在 server 代码里，处理请求前 `time.sleep(10)`
2. client 设置 5 秒超时
3. client 发送请求

预期：
- client 5 秒后报 timeout
- server 10 秒后尝试返回响应时发现 client 已断开
- server 不崩溃（`send()` 会抛出 `BrokenPipeError` 或 `ConnectionResetError`）

### 测试 5：Client 超时后重试

操作：
1. 保持 server `time.sleep(10)`
2. client 超时后自动重试
3. 在 client 第三次重试前，恢复 server（去掉 sleep，重启 server）

预期：
- 前几次超时报错
- 恢复后有一次重试成功
- 同一个 `request_id` 最终成功

### 测试 6：多 Client 同时连接

操作：
1. 启动 server
2. 同时启动 3 个 client
3. 3 个 client 各自发送多条消息

预期：
- 每个 client 都能正常收发
- server 日志能区分不同 client 的请求
- 一个 client 断开不影响其他 client

---

## 验收标准

完成以下所有项才算本周过关：

- [ ] server 不因为 client 异常断开（不发数据就断、发一半断）而崩溃
- [ ] client 超时后能打印超时错误（不是卡死）
- [ ] client 超时后能自动重试，且重试使用原 `request_id`
- [ ] server 日志能看到每条请求的 `remote_addr`、`request_id`、`body`
- [ ] 多个 client 可以同时连接并正常通信
- [ ] 所有故障测试场景手动执行通过

---

## 本周不做

明确列出，防止跑偏：

1. **不引入任何第三方库**——只用 Python 标准库（`socket`、`threading`）
2. **不用 JSON / Protobuf**——用纯文本协议
3. **不做 RPC 框架**——那是第 2 周的事
4. **不做 Raft**——那是第 9 周的事
5. **不做持久化 / WAL**——那是第 3 周的事
6. **不做优雅关闭 / 信号处理**——直接 Ctrl+C 就行
7. **不做 client_id + seq 去重**——那是第 2 周的事
8. **不做连接池**——每次重试建新连接就够了

---

## 每周文件要求

本周目录下需要产出：

```
week01_tcp/
├── README.md        # 你正在看的这个文件（启动说明 + 验收标准）
├── design.md        # 设计文档（见下方大纲）
├── test_report.md   # 测试报告（见下方大纲）
├── server.py        # 代码
└── client.py        # 代码
```

### design.md 大纲

```markdown
# Week 1 设计文档

## 协议设计
- 为什么选文本协议而不是 JSON
- 请求/响应格式定义
- 为什么用 request_id

## Server 设计
- 单线程 vs 多线程的选择理由
- 一个 client 线程的生命周期
- 异常处理策略（哪些异常捕获、哪些不捕获）

## Client 设计
- 超时设置的选择（为什么是这个值）
- 重试策略（次数、间隔、是否用新连接）
- 重试时 request_id 的处理

## 遇到的问题和解决思路
- （记录实际的坑）
```

### test_report.md 大纲

```markdown
# Week 1 测试报告

## 测试环境
- OS / Python 版本

## 测试场景和结果

### 测试 1：正常收发
- 操作：...
- 结果：通过 / 失败
- 日志截图（关键行）

### 测试 2：Client 连接后立刻断开
- 操作：...
- 结果：...
- server 行为：...

...（每个测试场景一节）

## 发现的 Bug
- Bug 描述
- 如何复现
- 如何修复

## 未解决的问题
- （如果有的话，诚实记录）
```

---

## 时间建议

| 时间段 | 做什么 | 预计耗时 |
|--------|--------|---------|
| 周一 | 读 DS for Fun and Profit 第 1、2 章 | 1 小时 |
| 周二 | 读 DDIA 第 8 章网络和时钟部分 + 写 server.py | 1.5 小时 |
| 周三 | 写 client.py（含超时和重试） | 1 小时 |
| 周四 | 手动执行全部 6 个故障测试 | 1 小时 |
| 周五 | 修测试发现的 bug | 1 小时 |
| 周六 | 写 design.md + test_report.md | 2~3 小时 |
| 周日 | 整体回顾 + 补充 edge case | 2 小时 |
