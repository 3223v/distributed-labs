# 集成测试：client_id+seq 去重
import asyncio, os, signal, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from mini_raft_kv.common import codec

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
SERVER_SCRIPT = os.path.join(PROJECT_ROOT, "scripts", "start_local.py")
WAL_PATH = os.path.join(PROJECT_ROOT, "data", "wal.log")

def _start_server():
    os.system("pkill -9 -f start_local 2>/dev/null"); time.sleep(0.3)
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
    os.system("pkill -9 -f start_local 2>/dev/null"); time.sleep(0.3)


async def _test():
    ok_count = 0
    total = 3

    # 用裸 codec 模拟：同一 client_id+seq 发两次，第二次应返回缓存结果
    r, w = await asyncio.open_connection("127.0.0.1", 8000)

    req1 = {"v":1,"request_id":100,"client_id":"dd","seq":1,"method":"put","params":{"key":"k","value":"v1"}}
    w.write(await codec.encode_message(req1)); await w.drain()
    resp1 = await codec.decode_message(r)
    assert resp1["ok"] == True, f"First Put failed: {resp1}"
    ok_count += 1; print(f"  [{ok_count}/{total}] First Put ok")

    # 重试：不同 request_id，相同 client_id+seq
    req2 = {"v":1,"request_id":101,"client_id":"dd","seq":1,"method":"put","params":{"key":"k","value":"v999"}}
    w.write(await codec.encode_message(req2)); await w.drain()
    resp2 = await codec.decode_message(r)
    assert resp2["ok"] == True, f"Dedup response failed: {resp2}"
    ok_count += 1; print(f"  [{ok_count}/{total}] Dedup hit ok（未重复执行）")

    # 验证 value 仍是 v1
    req3 = {"v":1,"request_id":102,"client_id":"ck","seq":0,"method":"get","params":{"key":"k"}}
    w.write(await codec.encode_message(req3)); await w.drain()
    resp3 = await codec.decode_message(r)
    raw = resp3["result"]
    val = raw["value"] if isinstance(raw, dict) else raw
    assert "v1" in str(val) and "v999" not in str(val), f"Dedup failed — value was overwritten: {resp3}"
    ok_count += 1; print(f"  [{ok_count}/{total}] Value preserved (v1 not overwritten by v999) ok")

    w.close()

    # 额外：seq 回退应拒绝
    c = Client("127.0.0.1", 8000, v=1, client_id="stale-test", timeout=3.0)
    await c.call("put", {"key": "s", "value": "1"})  # seq=1
    await c.call("put", {"key": "s2", "value": "2"}) # seq=2
    # 手动发 seq=1 的请求
    r2, w2 = await asyncio.open_connection("127.0.0.1", 8000)
    w2.write(await codec.encode_message({"v":1,"request_id":200,"client_id":"stale-test","seq":1,"method":"put","params":{"key":"s","value":"bad"}}))
    await w2.drain()
    resp = await codec.decode_message(r2)
    # stale 应返回错误（当前 ok=False）
    print(f"  Stale seq response: ok={resp['ok']} error={resp['error']} — " + ("rejected ✓" if resp["ok"] == False else "UNEXPECTED"))
    w2.close()

    print(f"\n  ALL {ok_count}/{total} PASSED")

if __name__ == "__main__":
    # Client import needed in stale test at the bottom
    from mini_raft_kv.client.client import Client
    pid = _start_server()
    try:
        asyncio.run(_test())
    finally:
        _stop_server()
