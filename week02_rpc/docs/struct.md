# Python struct 模块完整讲解

## 一、核心作用

`struct` 专门用来：**Python 数值 ↔ C 语言风格二进制字节串互相转换**。
Python 里的 `int/float` 是高层对象，网络、文件、硬件通信需要底层紧凑二进制字节，struct 就是干这个编解码的。

结合 **length-prefix JSON**：
长度前缀是 4 字节大端 uint32，`struct.pack(">I", n)` 就是把数字 n 转成 4 字节网络二进制头；接收端 `struct.unpack` 把 4 个字节还原成整数长度。

## 二、两个核心函数

### 1. struct.pack(format, v1, v2...)

把 Python 变量打包成 **bytes 二进制**

```
import struct
# >I 大端无符号4字节int
b = struct.pack(">I", 18)
print(b)  # b'\x00\x00\x00\x12'
```

### 2. struct.unpack(format, buffer)

把二进制 bytes 解包成 Python 元组

```
length, = struct.unpack(">I", b'\x00\x00\x00\x12')
print(length)  # 18
```

## 三、格式符三部分：字节序 + 类型

### 1. 字节序标志（放在最前面）

| 符号 | 含义 | 场景 |
| --- | --- | --- |
| `>` | 大端序（网络字节序） | TCP、RPC、length-prefix 标准，跨设备通用 |
| `<` | 小端序 | Windows 本地硬件、x86 机器 |
| `=` | 本机默认序 | 不推荐网络传输 |

网络通信**一律用 `>`**。

### 2. 常用类型标识符（重点）

| 符号 | C 类型 | 占用字节 | Python 类型 |
| --- | --- | --- | --- |
| `I` | unsigned int | 4 | int（0~4294967295） |
| `i` | signed int | 4 | int（正负） |
| `H` | unsigned short | 2 | int |
| `B` | unsigned char | 1 | int 0~255 |
| `f` | float | 4 | float |
| `d` | double | 8 | float |

### 示例对照

- `>I`：大端、4字节无符号整数（长度前缀标准格式）
- `>H`：大端2字节短整数
- `<i`：本机小端4字节有符号int

## 四、结合 length-prefix 完整流程

### 发包编码

1. JSON 转 utf-8 字节：`data = json.dumps(obj).encode()`
2. 计算长度 `L = len(data)`
3. struct 打包 4 字节长度头：`header = struct.pack(">I", L)`
4. 发送 `header + data`

### 收包解码

1. 先收 4 字节头 `buf[:4]`
2. `L, = struct.unpack(">I", buf[:4])` 读出消息长度
3. 再读取后面 L 字节 JSON 数据

## 五、关键特性

1. **定长二进制**
一种格式符固定占用固定字节，不像字符串数字（`"123"` 长度可变），所以能精准切分包，解决 TCP 粘包。
2. 只处理基础数值，不直接处理字符串
字符串要自己 encode/decode，struct 只管数字二进制。
3. unpack 永远返回元组
单个值也要加逗号解包：`val, = struct.unpack(">I", b4)`

## 六、极简小示例

```
import struct

# 打包数字 256 为4字节大端二进制
packed = struct.pack(">I", 256)
print(packed)  # b'\x00\x00\x01\x00'

# 还原数字
num, = struct.unpack(">I", packed)
print(num)  # 256
```

## 一句话总结

`struct` 是 Python 操作底层紧凑二进制的工具，用来把整数/浮点数转成固定长度字节，是 TCP 自定义协议、长度前缀帧、网络通信必不可少的模块。