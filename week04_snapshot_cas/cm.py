import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from rpc import client
import asyncio

rpc_client = client.Client("0.0.0.0", "8000", "1")

while True:
    print("> ", end="")
    line = input().strip()
    if not line:
        continue
    parts = line.split()
    cmd = parts[0].lower()

    if cmd == "quit":
        break
    elif cmd == "ping":
        resp = asyncio.run(rpc_client.call("ping", {}))
        print("<", resp.get("result", ""))

    elif cmd == "get":
        if len(parts) < 2:
            print("< 用法: get <key>")
            continue
        resp = asyncio.run(rpc_client.call("get", {"key": parts[1]}))
        print("<", resp.get("result", resp.get("error", "")))

    elif cmd == "put":
        if len(parts) < 3:
            print("< 用法: put <key> <value>")
            continue
        resp = asyncio.run(rpc_client.call("put", {"key": parts[1], "value": parts[2]}))
        if resp.get("ok"):
            print("< OK")
        else:
            print("<", resp.get("error", ""))
    elif cmd == "echo":
        if len(parts) < 2:
            print("< 用法: echo <message>")
            continue
        resp = asyncio.run(rpc_client.call("echo", {"value": " ".join(parts[1:])}))
        print("<", resp.get("result", resp.get("error", "")))
    elif cmd == "del":
        if len(parts) < 2:
            print("< 用法: get <key>")
            continue
        resp = asyncio.run(rpc_client.call("del", {"key": parts[1]}))
        print("<", resp.get("result", resp.get("error", "")))

    else:
        print("< 未知命令:", cmd, "支持: ping / get / put / del / quit ") 
