#!/usr/bin/env python3
"""命令行交互工具,像 Redis 一样操作 mini-raft-kv"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import asyncio, uuid
from mini_raft_kv.client.client import Client


HELP = """
  PUT <key> <value>      写入键值
  GET <key>              读取键
  DEL <key>              删除键
  CAS <key> <ver> <val>  带版本号的比较并交换
  PING                   探活
  ECHO <msg>             回显
  SNAPSHOT               生成快照
  HELP                   显示此帮助
  QUIT / EXIT / Ctrl+C   退出
"""


async def repl(host="127.0.0.1", port=8000):
    c = Client(host, port, v=1, client_id=uuid.uuid4().hex[:8], timeout=5.0)
    print(f"mini-raft-kv CLI  已连接到 {host}:{port}")
    print("输入 HELP 查看命令列表\n")

    while True:
        try:
            line = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nbye")
            break

        if not line:
            continue

        parts = line.split()
        method = parts[0].upper()

        if method in ("QUIT", "EXIT"):
            print("bye")
            break
        elif method == "HELP":
            print(HELP)
            continue

        params = {}
        try:
            if method == "PUT":
                params = {"key": parts[1], "value": parts[2]}
            elif method == "GET":
                params = {"key": parts[1]}
            elif method == "DEL":
                params = {"key": parts[1]}
            elif method == "CAS":
                params = {"key": parts[1], "version": int(parts[2]), "value": parts[3]}
            elif method == "ECHO":
                params = {"value": " ".join(parts[1:]) if len(parts) > 1 else ""}
            elif method in ("PING", "SNAPSHOT"):
                pass
            else:
                print(f"  未知命令: {method}（输入 HELP 查看）")
                continue

            result = await c.call(method, params)
            ok = result.get("ok", False)
            r = result.get("result")
            err = result.get("error")

            if ok:
                if method == "GET":
                    val = r.get("value") if isinstance(r, dict) else r
                    print(f"  \"{val}\"")
                elif method == "PUT":
                    ver = r.get("version") if isinstance(r, dict) else "?"
                    print(f"  OK (version={ver})")
                elif method == "CAS":
                    ver = r.get("version") if isinstance(r, dict) else "?"
                    print(f"  OK (version={ver})")
                elif method == "DEL":
                    existed = r.get("existed") if isinstance(r, dict) else "?"
                    print(f"  OK (existed={existed})")
                elif method == "PING":
                    print(f"  PONG")
                elif method == "SNAPSHOT":
                    idx = r.get("wal_index") if isinstance(r, dict) else "?"
                    print(f"  OK (wal_index={idx})")
                else:
                    print(f"  {r}")
            else:
                msg = err.get("message", str(err)) if isinstance(err, dict) else str(err)
                print(f"  (error) {msg}")

        except (IndexError, ValueError):
            print(f"  参数错误，格式: {method} <...>")
        except ConnectionRefusedError:
            print("  连接失败：服务端未启动")
            break
        except Exception as e:
            print(f"  异常: {e}")


if __name__ == "__main__":
    asyncio.run(repl())
