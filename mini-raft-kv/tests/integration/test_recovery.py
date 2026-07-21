# 集成测试：crash recovery + WAL 尾部损坏恢复
import asyncio, os, signal, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from mini_raft_kv.client.client import Client

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
SERVER_SCRIPT = os.path.join(PROJECT_ROOT, "scripts", "start_local.py")
WAL_PATH = os.path.join(PROJECT_ROOT, "data", "wal.log")

def _start_server():
    _stop_server()
    if os.path.exists(WAL_PATH):
        os.remove(WAL_PATH)
    pid = os.fork()
    if pid == 0:
        os.setsid()
        os.chdir(PROJECT_ROOT)
        os.execvp("python3", ["python3", SERVER_SCRIPT])
    time.sleep(1.0)
    return pid

def _stop_server():
    os.system("pkill -9 -f start_local 2>/dev/null")
    time.sleep(0.3)


async def _test():
    c = Client("127.0.0.1", 8000, v=1, client_id="recovery-test", timeout=3.0)
    ok_count = 0
    total = 3

    # 1. Put → 验证
    await c.call("put", {"key": "persist_key", "value": "persist_val"})
    r = await c.call("get", {"key": "persist_key"})
    raw = r["result"]
    val = raw["value"] if isinstance(raw, dict) else raw
    assert "persist_val" in str(val), f"Pre-crash Get failed: {r}"
    ok_count += 1; print(f"  [{ok_count}/{total}] Pre-crash Put/Get ok")

    # 2. Kill -9 → 重启 → 数据还在
    _stop_server()
    pid = os.fork()
    if pid == 0:
        os.setsid()
        os.chdir(PROJECT_ROOT)
        os.execvp("python3", ["python3", SERVER_SCRIPT])
    time.sleep(1.0)

    c2 = Client("127.0.0.1", 8000, v=1, client_id="recovery-test2", timeout=3.0)
    r = await c2.call("get", {"key": "persist_key"})
    raw = r["result"]
    val = raw["value"] if isinstance(raw, dict) else raw
    assert "persist_val" in str(val), f"Post-crash Get failed: {r}"
    ok_count += 1; print(f"  [{ok_count}/{total}] Crash recovery ok")

    # 3. WAL 尾部损坏 → 重启 → 前面数据完好
    _stop_server()
    with open(WAL_PATH, "a") as f:
        f.write('{"key":"broken","op":"put","crc32":9999}\n')

    pid = os.fork()
    if pid == 0:
        os.setsid()
        os.chdir(PROJECT_ROOT)
        os.execvp("python3", ["python3", SERVER_SCRIPT])
    time.sleep(1.0)

    c3 = Client("127.0.0.1", 8000, v=1, client_id="recovery-test3", timeout=3.0)
    r = await c3.call("get", {"key": "persist_key"})
    raw = r["result"]
    val = raw["value"] if isinstance(raw, dict) else raw
    assert "persist_val" in str(val), f"Corrupted WAL recovery failed: {r}"
    r2 = await c3.call("get", {"key": "broken"})
    assert r2["ok"] == False, f"Corrupted record should not be recovered: {r2}"
    ok_count += 1; print(f"  [{ok_count}/{total}] Corrupt WAL truncation ok")

    print(f"\n  ALL {ok_count}/{total} PASSED")


if __name__ == "__main__":
    pid = _start_server()
    try:
        asyncio.run(_test())
    finally:
        _stop_server()
