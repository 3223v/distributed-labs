import json
import struct


# 字典到字节流
async def encode_message(obj: dict) -> bytes:
    body = json.dumps(obj).encode('utf-8')
    length = struct.pack('>I', len(body))
    return length + body


# reader 对象到字典
async def decode_message(reader) -> dict:
    # 必须用 readexactly，不能用 read！
    header = await reader.readexactly(4)
    size = struct.unpack(">I", header)[0]
    body = await reader.readexactly(size)
    return json.loads(body.decode("utf-8"))
