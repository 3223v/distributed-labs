"""测试超时 → 重试 → 去重不重复执行"""
import asyncio
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from rpc import server, codec


class SlowServer:
    """
    模拟 server 对特定 key 做慢处理。
    第一个请求 slow=1 会延迟 3 秒再返回，后续请求正常。
    """
    def __init__(self):
        self._srv = server.Server()
        self.slow_count = 0

    async def dispatch(self, req):
        key = req.get("params", {}).get("key", "")
        # 只有 Put 且 key="slow" 且第一次执行才延迟
        if req.get("method", "").lower() == "put" and key == "slow":
            self.slow_count += 1
            if self.slow_count == 1:
                await asyncio.sleep(3)
        return await self._srv.dispatch(req)

    async def handle_client(self, reader, writer):
        try:
            while True:
                req = await codec.decode_message(reader)
                resp = await self.dispatch(req)
                writer.write(await codec.encode_message(resp))
                await writer.drain()
        except asyncio.IncompleteReadError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    async def run(self, host, port):
        srv = await asyncio.start_server(self.handle_client, host, port)
        async with srv:
            await srv.serve_forever()


async def test_retry_preserves_seq():
    """
    Client 超时后重试使用相同 seq。
    Server 第一次慢处理（3 秒），Client 1 秒超时 → 重试。
    第二次请求命中去重表 → 不重复执行 → 返回缓存结果。
    """
    srv = SlowServer()
    task = asyncio.create_task(srv.run("127.0.0.1", 18091))
    await asyncio.sleep(0.3)

    # 直接构造请求发送（绕过 Client 的 seq 自增，精确控制）
    async def send_raw(req):
        reader, writer = await asyncio.open_connection("127.0.0.1", 18091)
        writer.write(await codec.encode_message(req))
        await writer.drain()
        resp = await asyncio.wait_for(codec.decode_message(reader), timeout=5)
        writer.close()
        await writer.wait_closed()
        return resp

    # 1. 用短超时的 client 发 Put(slow, 1)
    # 不能用 Client 类因为它的重试逻辑会自己处理
    # 这里我们模拟：发 seq=1，1 秒后超时（在发送方），然后重发 seq=1
    import asyncio as aio_mod
    try:
        reader, writer = await aio_mod.wait_for(
            asyncio.open_connection("127.0.0.1", 18091), timeout=5
        )
        writer.write(await codec.encode_message({
            "request_id": 1, "client_id": "r1", "seq": 1,
            "method": "Put", "params": {"key": "slow", "value": "first"}
        }))
        await writer.drain()
        # 等 1 秒就放弃（server 要 3 秒才返回）
        try:
            resp = await aio_mod.wait_for(codec.decode_message(reader), timeout=1.0)
        except asyncio.TimeoutError:
            resp = None  # 超时，关闭连接
        writer.close()
        await writer.wait_closed()
    except Exception:
        pass

    # 等 server 慢处理完成
    await asyncio.sleep(3.5)

    # 2. 重试：seq=1 不变，request_id 换一个
    r2 = await send_raw({
        "request_id": 2, "client_id": "r1", "seq": 1,
        "method": "Put", "params": {"key": "slow", "value": "retry"}
    })
    # server 应识别为重复，返回缓存结果
    assert r2["ok"] == True, f"重试应成功（去重返回缓存）: {r2}"

    # 3. 验证 slow 的值为 "first"（不是 "retry"）
    r3 = await send_raw({
        "request_id": 3, "client_id": "r1", "seq": 2,
        "method": "Get", "params": {"key": "slow"}
    })
    assert r3["result"] == "first", f"去重失败，值被覆盖为: {r3['result']}"

    task.cancel()
    await asyncio.sleep(0.2)
    print("  ✓ retry 不重复执行")


async def run_all():
    tests = [test_retry_preserves_seq]
    passed = 0
    for t in tests:
        try:
            await t()
            passed += 1
        except Exception as e:
            print(f"  ✗ {t.__name__}: {e}")
            import traceback
            traceback.print_exc()
    print(f"\n{passed}/{len(tests)} 通过")
    return passed == len(tests)


if __name__ == "__main__":
    success = asyncio.run(run_all())
    exit(0 if success else 1)
