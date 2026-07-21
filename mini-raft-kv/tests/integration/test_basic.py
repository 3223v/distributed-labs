# 集成测试：基本 CRUD + Ping/Echo
import asyncio, os, signal, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from mini_raft_kv.client.client import Client

SERVER_SCRIPT = os.path.join(os.path.dirname(__file__), "..", "..", "scripts", "start_local.py")
WAL_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "wal.log")

def _start_server():
    """启动 server 子进程，等待就绪"""
    _stop_server()
    # 清 WAL
    if os.path.exists(WAL_PATH):
        os.remove(WAL_PATH)
    pid = os.fork()
    if pid == 0:
        os.setsid()
        os.chdir(os.path.join(os.path.dirname(__file__), "..", ".."))
        os.execvp("python3", ["python3", SERVER_SCRIPT])
    time.sleep(1.0)
    return pid

def _stop_server():
    os.system("pkill -9 -f start_local 2>/dev/null")
    time.sleep(0.3)


async def _test():
    c = Client("127.0.0.1", 8000, v=1, client_id="test-basic", timeout=3.0)
    ok_count = 0
    total = 6

    # Put
    r = await c.call("put", {"key": "name", "value": "alice"})
    assert r["ok"] == True, f"Put failed: {r}"
    ok_count += 1; print(f"  [{ok_count}/{total}] Put ok")

    # Get
    r = await c.call("get", {"key": "name"})
    assert r["ok"] == True
    raw = r["result"]
    val = raw["value"] if isinstance(raw, dict) else raw
    assert "alice" in str(val), f"Get wrong value: {r}"
    ok_count += 1; print(f"  [{ok_count}/{total}] Get ok")

    # Get miss
    r = await c.call("get", {"key": "nope"})
    assert r["ok"] == False, f"Get miss should fail: {r}"
    ok_count += 1; print(f"  [{ok_count}/{total}] Get miss ok")

    # Delete
    r = await c.call("del", {"key": "name"})
    assert r["ok"] == True, f"Del failed: {r}"
    ok_count += 1; print(f"  [{ok_count}/{total}] Del ok")

    # Ping
    r = await c.call("ping", {})
    assert r["ok"] == True, f"Ping failed: {r}"
    ok_count += 1; print(f"  [{ok_count}/{total}] Ping ok")

    # Echo
    r = await c.call("echo", {"value": "hello"})
    assert r["ok"] == True, f"Echo failed: {r}"
    ok_count += 1; print(f"  [{ok_count}/{total}] Echo ok")

    print(f"\n  ALL {ok_count}/{total} PASSED")


if __name__ == "__main__":
    pid = _start_server()
    try:
        asyncio.run(_test())
    finally:
        _stop_server()
