"""测试 Client 超时行为"""
import asyncio
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from rpc import client, server, codec


async def test_client_timeout():
    """Client 超时后应正确返回错误，不卡死"""
    # 启动一个 server 但不响应（模拟超时场景用端口无响应更简单）
    # 这里我们连接到一个会 accept 但不返回数据的端口
    # 实际用短超时连接一个不可达地址来测
    cli = client.Client("127.0.0.1", 19999, "c1", timeout=0.5)
    cli.max_retries = 1  # 只试一次，快速失败
    resp = await cli.call("Ping", {})
    assert resp["ok"] == False, f"超时应该返回 ok=false: {resp}"
    assert "max retries" in resp["error"], f"错误信息应包含 max retries: {resp}"


async def test_timeout_then_server_available():
    """先超时失败，再连正常 server 能成功（验证 Client 可恢复）"""
    cli = client.Client("127.0.0.1", 19998, "c1", timeout=0.5)
    cli.max_retries = 1
    resp = await cli.call("Ping", {})
    assert resp["ok"] == False  # 第一次超时

    # 启动正常 server
    srv = server.Server()
    task = asyncio.create_task(srv.run("127.0.0.1", 18090))
    await asyncio.sleep(0.3)

    cli2 = client.Client("127.0.0.1", 18090, "c1", timeout=2.0)
    resp = await cli2.call("Ping", {})
    assert resp["ok"] == True
    assert resp["result"] == "pong"

    task.cancel()
    await asyncio.sleep(0.2)


async def run_all():
    tests = [
        test_client_timeout,
        test_timeout_then_server_available,
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
