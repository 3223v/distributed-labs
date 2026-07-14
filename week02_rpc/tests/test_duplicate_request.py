"""测试去重：过期 seq 拒绝、多 client 独立 seq、重复请求幂等"""
import asyncio
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from rpc import server, codec


async def send_raw(host, port, req):
    reader, writer = await asyncio.open_connection(host, port)
    writer.write(await codec.encode_message(req))
    await writer.drain()
    resp = await asyncio.wait_for(codec.decode_message(reader), timeout=3)
    writer.close()
    await writer.wait_closed()
    return resp


async def test_duplicate_request():
    """重复请求返回缓存结果，不重复执行"""
    srv = server.Server()
    task = asyncio.create_task(srv.run("127.0.0.1", 18092))
    await asyncio.sleep(0.3)

    host, port = "127.0.0.1", 18092

    # 首次写
    r = await send_raw(host, port, {
        "request_id": 1, "client_id": "c1", "seq": 1,
        "method": "Put", "params": {"key": "k", "value": "v1"}
    })
    assert r["ok"] == True, f"首次写失败: {r}"

    # 重复请求（相同 client_id + seq，不同 value）
    r = await send_raw(host, port, {
        "request_id": 2, "client_id": "c1", "seq": 1,
        "method": "Put", "params": {"key": "k", "value": "SHOULD_NOT_WRITE"}
    })
    assert r["ok"] == True, f"重复请求应返回缓存: {r}"

    # 验证值没有被覆盖
    r = await send_raw(host, port, {
        "request_id": 3, "client_id": "c1", "seq": 2,
        "method": "Get", "params": {"key": "k"}
    })
    assert r["result"] == "v1", f"值被错误覆盖: {r['result']}"

    task.cancel()
    await asyncio.sleep(0.2)


async def test_stale_seq_rejected():
    """过期 seq 被拒绝"""
    srv = server.Server()
    task = asyncio.create_task(srv.run("127.0.0.1", 18093))
    await asyncio.sleep(0.3)

    host, port = "127.0.0.1", 18093

    # 推进 seq 到 5
    for i in range(1, 6):
        await send_raw(host, port, {
            "request_id": i, "client_id": "c1", "seq": i,
            "method": "Put", "params": {"key": "k", "value": f"v{i}"}
        })

    # 用 old seq=2 发请求
    r = await send_raw(host, port, {
        "request_id": 99, "client_id": "c1", "seq": 2,
        "method": "Put", "params": {"key": "k", "value": "STALE"}
    })
    assert r["ok"] == False, f"过期 seq 应被拒绝: {r}"
    assert "回退" in r.get("error", ""), f"错误信息应包含'回退': {r}"

    # 验证值仍然是 v5
    r = await send_raw(host, port, {
        "request_id": 100, "client_id": "c1", "seq": 6,
        "method": "Get", "params": {"key": "k"}
    })
    assert r["result"] == "v5", f"值被过期请求修改: {r['result']}"

    task.cancel()
    await asyncio.sleep(0.2)


async def test_independent_client_seq():
    """不同 client 的 seq 互相独立"""
    srv = server.Server()
    task = asyncio.create_task(srv.run("127.0.0.1", 18094))
    await asyncio.sleep(0.3)

    host, port = "127.0.0.1", 18094

    # c1 写
    await send_raw(host, port, {
        "request_id": 1, "client_id": "c1", "seq": 1,
        "method": "Put", "params": {"key": "k", "value": "c1_val"}
    })
    # c2 写（相同 seq=1，独立空间）
    r = await send_raw(host, port, {
        "request_id": 2, "client_id": "c2", "seq": 1,
        "method": "Put", "params": {"key": "k", "value": "c2_val"}
    })
    assert r["ok"] == True, f"c2 独立 seq 应正常执行: {r}"

    # 验证最终值
    r = await send_raw(host, port, {
        "request_id": 3, "client_id": "c1", "seq": 2,
        "method": "Get", "params": {"key": "k"}
    })
    assert r["result"] == "c2_val", f"c2 写入应生效: {r['result']}"

    task.cancel()
    await asyncio.sleep(0.2)


async def test_missing_fields():
    """缺少必要字段时返回错误，不崩溃"""
    srv = server.Server()
    task = asyncio.create_task(srv.run("127.0.0.1", 18095))
    await asyncio.sleep(0.3)

    host, port = "127.0.0.1", 18095

    # 缺少 client_id 和 seq
    r = await send_raw(host, port, {
        "request_id": 1, "method": "Put",
        "params": {"key": "k", "value": "v"}
    })
    assert r["ok"] == False, f"缺少字段应返回错误: {r}"
    assert "不全" in r.get("error", ""), f"应有信息不全提示: {r}"

    # Server 应仍然存活
    r = await send_raw(host, port, {
        "request_id": 2, "client_id": "c1", "seq": 1,
        "method": "Ping", "params": {}
    })
    assert r["ok"] == True, f"Server 应在缺少字段后仍然正常: {r}"

    task.cancel()
    await asyncio.sleep(0.2)


async def run_all():
    tests = [
        test_duplicate_request,
        test_stale_seq_rejected,
        test_independent_client_seq,
        test_missing_fields,
    ]
    passed = 0
    for t in tests:
        try:
            await t()
            print(f"  ✓ {t.__name__}")
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
