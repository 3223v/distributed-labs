# Week 2 测试报告

## 测试环境

- OS: macOS Darwin 24.6.0
- Python: 3.9
- 日期: 2026-07-14

## 测试场景和结果

### 1. test_codec.py — 编解码正确性

| # | 场景 | 结果 | 说明 |
|---|------|------|------|
| 1.1 | 编码 → 解码往返 | ✅ 通过 | Python dict 经 encode → decode 后完全一致 |
| 1.2 | 大消息（64KB） | ✅ 通过 | header 长度正确，往返数据完整 |
| 1.3 | 空 params | ✅ 通过 | `{}` 正常编解码 |
| 1.4 | 半包 header | ✅ 通过 | `readexactly` 正确抛出 `IncompleteReadError` |
| 1.5 | 半包 body | ✅ 通过 | `readexactly` 正确抛出 `IncompleteReadError` |

### 2. test_timeout.py — 超时行为

| # | 场景 | 结果 | 说明 |
|---|------|------|------|
| 2.1 | 连接不可达端口 | ✅ 通过 | Client 超时后返回 `ok: false`，不卡死 |
| 2.2 | 超时后恢复正常 | ✅ 通过 | 先连不可达（超时失败），再连正常 server（成功） |

### 3. test_duplicate_request.py — 去重

| # | 场景 | 结果 | 说明 |
|---|------|------|------|
| 3.1 | 重复请求不重复执行 | ✅ 通过 | 相同 `client_id + seq` 返回缓存结果，未覆盖原值 |
| 3.2 | 过期 seq 拒绝 | ✅ 通过 | `seq < last_seq` 返回错误，值未被修改 |
| 3.3 | 多 client 独立 seq | ✅ 通过 | c1 和 c2 各自的 seq=1 互不影响 |
| 3.4 | 缺少必填字段 | ✅ 通过 | 返回 `ok: false` + 错误信息，server 继续服务 |

### 4. test_retry.py — 重试 + 去重

| # | 场景 | 结果 | 说明 |
|---|------|------|------|
| 4.1 | 慢处理 → 超时 → 重试 | ✅ 通过 | Server 第一次 3 秒延迟处理，Client 端 1 秒超时后重试，第二次命中去重缓存，值保持为初始值 |

### 5. 手动交互测试

| 命令 | 结果 |
|------|------|
| `ping` | ✅ 返回 `pong` |
| `echo hello world` | ✅ 回显 `hello world` |
| `put x 1` | ✅ 返回 `OK`，Get 确认值为 `1` |
| `get x` | ✅ 返回 `1` |
| `get nonexist` | ✅ 返回 `key 不存在` |

### 6. Server 去重日志验证

| 日志标记 | 触发条件 | 已验证 |
|----------|---------|--------|
| `[首次]` | seq > last_seq | ✅ |
| `[重复]` | seq == last_seq | ✅ |
| `[过期]` | seq < last_seq | ✅ |

## 发现的 Bug

| # | Bug | 状态 |
|---|-----|------|
| 1 | `import asyncio` 缺失（`server_main.py`、`client_main.py`） | 已修复 |
| 2 | `client_main.py` 中 `\|\|` 不是 Python 语法（应为 `or`） | 已修复 |
| 3 | `get` / `put` 命令错误调用了 `ping` 方法 | 已修复 |
| 4 | 参数校验 `arr[1] is None` 永远为 False | 已修复 |
| 5 | `except json.JSONDecodeError` 被 `except Exception` 拦截 | 已修复 |
| 6 | `dispatch` 字段校验在取值之前，导致 NameError | 已修复 |
| 7 | 错误返回中引用未赋值的 `request_id` 导致 NameError | 已修复 |

## 未解决的问题

1. **去重缓存 `request_id` 陈旧**：`server.py` 中 `return record["last_result"]` 返回缓存的原始 `request_id`，当重试请求使用不同 `request_id` 时会触发 client 的 `[WARN]` 防御检查。建议 server 在返回缓存时更新 `request_id`。
2. **`codec.encode_message` 是 `async def` 但无 `await`**：不需要异步，但不影响功能。
3. **Network Simulator 未集成**：`Network` 类已实现，但 client 和 server 仍直连 TCP，未经过 Network 层。
4. **去重表纯内存**：server 重启后所有去重状态丢失（Week 3 将引入 WAL）。
