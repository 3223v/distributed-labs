# CRC32 校验 — WAL 完整性保护指南

## 为什么需要 CRC32

当前你的 WAL 记录是这样的：

```json
{"op": "Put", "key": "x", "value": "hello", "client_id": "c1", "seq": 1}
```

问题：如果写这条记录时进程被 `kill -9`，文件里可能只有半行：

```
{"op": "Put", "key": "x", "va
```

重启 replay 时，`json.loads` 解析这行会抛异常——你已经处理了。但更坏的情况是：

```
{"op": "Put", "key": "x", "value": "old_data"}\n{"op": "Put", "ke
```

第二行写了一半，磁盘上可能是乱码，`json.loads` 解析失败。但如果有脏数据碰巧解析成功（极低概率），就会 corrupt 你的 KV 状态。

**CRC32 做的事**：给每行记录附加一个校验码。replay 时重新计算校验码，对不上就说明这行损坏了 → 截断丢弃。

---

## CRC32 是什么

一个 **4 字节的整数**，由数据内容计算得出。特点：

- 相同数据 → 相同 CRC32（100% 确定）
- 数据改变一个字节 → CRC32 完全不同
- 无法从 CRC32 反推原始数据（单向）

Python 内置支持，无需安装任何东西：

```python
import binascii

data = b'hello world'
crc = binascii.crc32(data)       # 返回整数，如 222957957
crc_unsigned = crc & 0xffffffff  # 转为无符号 32 位
```

---

## 改造 WAL 记录格式

### 现在（无校验）

```json
{"op": "Put", "key": "x", "value": "hello", "client_id": "c1", "seq": 1}
```

### 改后（带 crc32）

```json
{"crc32": 3862052738, "op": "Put", "key": "x", "value": "hello", "client_id": "c1", "seq": 1}
```

`crc32` 是对 **除了 crc32 自身以外** 的 JSON 内容计算的。

---

## 实现步骤

### 1. `append` 改造 —— 计算并写入 crc32

```python
import binascii

async def append(self, record: dict) -> None:
    with open(self.path, "a") as f:
        # 1. 先把核心字段转 JSON
        body = json.dumps(record)           # {"op":"Put","key":"x",...}
        body_bytes = body.encode("utf-8")

        # 2. 计算 CRC32
        crc = binascii.crc32(body_bytes) & 0xffffffff

        # 3. 把 crc32 插入到 record 里
        record["crc32"] = crc

        # 4. 重新序列化（现在包含 crc32 字段）
        full = json.dumps(record)

        # 5. 写入 + fsync
        f.write(full + "\n")
        if self.sync_mode == "always":
            f.flush()
            await asyncio.to_thread(os.fsync, f.fileno())
```

**关键**：先算 crc，再塞回去，再序列化。顺序不能乱。

### 2. `replay` 改造 —— 逐行校验 + 损坏截断

```python
async def replay(self) -> list[dict]:
    result = []
    if not os.path.exists(self.path):
        return result

    with open(self.path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                log.error("WAL 行 JSON 解析失败，截断到此", line=line[:50])
                self._truncate_to_line(f, line)
                break

            # 取出 crc32 字段
            stored_crc = data.pop("crc32", None)
            if stored_crc is None:
                log.error("WAL 行缺少 crc32，截断到此")
                self._truncate_to_line(f, line)
                break

            # 重新计算（用 pop 掉 crc32 后的剩余字段）
            recomputed = json.dumps(data).encode("utf-8")
            recomputed_crc = binascii.crc32(recomputed) & 0xffffffff

            if recomputed_crc != stored_crc:
                log.error("WAL crc32 不匹配，截断到此",
                          stored=stored_crc, computed=recomputed_crc)
                self._truncate_to_line(f, line)
                break

            result.append(data)

    return result
```

### 3. `_truncate_to_line` —— 截断损坏行

这是最关键的操作：发现某行损坏，就把文件从这行开头截断，丢弃它及后面所有内容。

```python
def _truncate_to_line(self, f, bad_line: str):
    """截断 WAL 文件到当前行之前（不含当前行）"""
    # 当前文件指针位置 = 这行末尾
    # 这行长度 = len(bad_line) + 1（换行符）
    current_pos = f.tell()
    bad_line_len = len(bad_line.encode("utf-8")) + 1
    # 截断位置 = 当前行开头
    truncate_pos = current_pos - bad_line_len

    f.close()
    with open(self.path, "ab") as fw:
        fw.truncate(truncate_pos)

    log.warn("WAL 已截断", path=self.path, position=truncate_pos)
```

### 4. 完整流程示例

```
WAL 文件内容：
{"crc32":111,"op":"Put","key":"x","value":"v1"}\n
{"crc32":222,"op":"Put","key":"y","value":"v2"}\n
{"crc32":999,"op":"Put","key":"z","va         ← 半行，损坏

replay 过程：
第 1 行 → 解析成功 → crc32=111 匹配 → 加入结果 ✓
第 2 行 → 解析成功 → crc32=222 匹配 → 加入结果 ✓
第 3 行 → json.loads 失败 → 截断文件到此行开头
         → 文件变成前 2 行
         → return [{op:Put,x:v1}, {op:Put,y:v2}]
```

---

## 用到的 Python 模块

```python
import binascii          # crc32 计算
import json              # 序列化
import os                # fsync, f.truncate
import asyncio           # to_thread
```

---

## 验证你的实现是否正确

写完后来回测这两个场景：

**场景 1：正常往返**
```
append 3 条 → replay → 返回 3 条，每条 crc32 已被 pop 掉
```

**场景 2：手动破坏最后一行**
```
1. 写入 3 条记录
2. 用文本编辑器打开 wal 文件，把最后一行改几个字符
3. replay → 返回前 2 条，WAL 文件被截断为前 2 行
```

---

## 常见错误

| 错误 | 原因 | 解决 |
|------|------|------|
| `crc32` 每次不同 | 你把 crc32 字段也纳入计算了 | 先算 crc，再塞回 record |
| 截断位置不对 | `tell()` 的位置计算错了 | 在 `for line in f` 循环里，`f.tell()` 指向下一行开头，所以要减去当前行长度 |
| `f.truncate()` 报错 | 文件以读模式打开的 | 先 `f.close()`，再用写模式打开执行 `truncate` |
