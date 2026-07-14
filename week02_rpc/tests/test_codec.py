"""测试 length-prefix JSON 编解码的正确性"""
import asyncio
import json
import struct
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from rpc import codec


async def test_encode_decode_roundtrip():
    """编码 → 解码 往返一致性"""
    obj = {
        "request_id": 1,
        "client_id": "c1",
        "seq": 3,
        "method": "Put",
        "params": {"key": "x", "value": "hello"}
    }
    encoded = await codec.encode_message(obj)
    # 验证 header
    header = encoded[:4]
    size = struct.unpack(">I", header)[0]
    assert size == len(encoded) - 4, f"header 长度不对: {size} vs {len(encoded) - 4}"
    # 验证往返
    decoded = await codec.decode_message(MockReader(encoded))
    assert decoded == obj, f"往返不一致: {decoded}"


async def test_large_message():
    """64KB 大消息不丢数据"""
    big_value = "x" * 65536
    obj = {"data": big_value}
    encoded = await codec.encode_message(obj)
    # header 应该正确反映长度
    size = struct.unpack(">I", encoded[:4])[0]
    assert size == len(json.dumps(obj).encode("utf-8")), "大消息 size 错误"
    # 往返
    decoded = await codec.decode_message(MockReader(encoded))
    assert decoded == obj, "大消息往返不一致"


async def test_empty_params():
    """空 params 正常编解码"""
    obj = {"request_id": 1, "method": "Ping", "params": {}}
    encoded = await codec.encode_message(obj)
    decoded = await codec.decode_message(MockReader(encoded))
    assert decoded == obj


async def test_partial_header():
    """半包 header: readexactly 应该抛 IncompleteReadError"""
    # 只给 2 字节 header（完整 header 是 4 字节）
    reader = MockReader(b'\x00\x00')
    try:
        await codec.decode_message(reader)
        assert False, "应该抛 IncompleteReadError"
    except asyncio.IncompleteReadError:
        pass  # 期望的行为


async def test_partial_body():
    """半包 body: readexactly 应该抛 IncompleteReadError"""
    body = json.dumps({"key": "value"}).encode("utf-8")
    header = struct.pack(">I", len(body))
    # 只给 header + 半个 body
    half = len(body) // 2
    reader = MockReader(header + body[:half])
    try:
        await codec.decode_message(reader)
        assert False, "应该抛 IncompleteReadError"
    except asyncio.IncompleteReadError:
        pass  # 期望的行为


# ----- 辅助：模拟 StreamReader -----

class MockReader:
    """模拟 asyncio.StreamReader，按 readexactly 语义返回数据"""
    def __init__(self, data: bytes):
        self._data = data
        self._pos = 0

    async def readexactly(self, n: int) -> bytes:
        chunk = self._data[self._pos:self._pos + n]
        if len(chunk) < n:
            raise asyncio.IncompleteReadError(chunk, n)
        self._pos += n
        return chunk


async def run_all():
    tests = [
        test_encode_decode_roundtrip,
        test_large_message,
        test_empty_params,
        test_partial_header,
        test_partial_body,
    ]
    passed = 0
    for t in tests:
        try:
            await t()
            print(f"  ✓ {t.__name__}")
            passed += 1
        except Exception as e:
            print(f"  ✗ {t.__name__}: {e}")
    print(f"\n{passed}/{len(tests)} 通过")
    return passed == len(tests)


if __name__ == "__main__":
    success = asyncio.run(run_all())
    exit(0 if success else 1)
