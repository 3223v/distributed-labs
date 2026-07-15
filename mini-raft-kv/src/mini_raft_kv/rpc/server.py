import asyncio
import json
from mini_raft_kv.common import log
from mini_raft_kv.rpc import codec

class Server:
    def __init__(self):
        self.dedup_table = {}   # client_id -> {"last_seq": int, "last_result": dict}
        self.hash_map = {}      # key -> value
        self.lock = asyncio.Lock()

    async def dispatch(self, req: dict) -> dict:
        request_id = req.get("request_id")
        client_id = req.get("client_id")
        seq = req.get("seq")
        if request_id is None or client_id is None or seq is None:
            return {
                "request_id": request_id if request_id else 0,
                "ok": False,
                "result": "",
                "error": "信息不全"
            }
        method = req.get("method", "").lower()
        params = req.get("params", {})
        key = params.get("key", "")
        value = params.get("value", "")
        if method == "put":
            async with self.lock:
                # 初始化去重记录
                if client_id not in self.dedup_table:
                    self.dedup_table[client_id] = {
                        "last_seq": -1,
                        "last_result": None
                    }
                record = self.dedup_table[client_id]

                # 序列号回退 → 错误
                if seq < record["last_seq"]:
                    log.warn("过期请求，拒绝", client=client_id, seq=seq)
                    return {
                        "request_id": request_id,
                        "ok": False,
                        "result": "",
                        "error": "seq 回退，拒绝处理"
                    }

                # 重复请求 → 返回缓存结果
                if seq == record["last_seq"]:
                    log.info("重复请求", client=client_id, seq=seq)
                    return record["last_result"]

                # 新请求（seq > last_seq）→ 执行 Put
                self.hash_map[key] = value
                resp = {
                    "request_id": request_id,
                    "ok": True,
                    "result": "OK",
                    "error": ""
                }
                # 更新去重表
                record["last_seq"] = seq
                record["last_result"] = resp
                log.info("首次请求", client=client_id, seq=seq)
                return resp

        elif method == "get":
            async with self.lock:
                if key in self.hash_map:
                    return {
                        "request_id": request_id,
                        "ok": True,
                        "result": self.hash_map[key],
                        "error": ""
                    }
                else:
                    return {
                        "request_id": request_id,
                        "ok": False,
                        "result": "",
                        "error": "key 不存在"
                    }

        elif method == "ping":
            return {
                "request_id": request_id,
                "ok": True,
                "result": "pong",
                "error": ""
            }

        elif method == "echo":
            return {
                "request_id": request_id,
                "ok": True,
                "result": value,    # 回显 params 中的 value
                "error": ""
            }

        else:
            return {
                "request_id": request_id,
                "ok": False,
                "result": "",
                "error": "未知方法"
            }

    async def handle_client(self, reader, writer):
        addr = writer.get_extra_info('peername')
        log.info("新客户端连接", addr=addr)
        try:
            while True:
                req = await codec.decode_message(reader)
                if req is None:
                    break
                resp = await self.dispatch(req)
                data = await codec.encode_message(resp)
                writer.write(data)         # 同步写入缓冲区
                await writer.drain()       # 确保数据发送
        except asyncio.IncompleteReadError:
            log.warn("客户端断开", addr=addr)
        except json.JSONDecodeError:
            log.warn("非法json", addr=addr)
        except Exception as e:
            log.error("处理客户端出错", addr=addr, error=str(e))
        finally:
            writer.close()
            await writer.wait_closed()

    async def run(self, host="0.0.0.0", port=8000):
        server = await asyncio.start_server(
            self.handle_client,
            host=host,
            port=port
        )
        log.info("RPC 服务启动", host=host, port=port)
        async with server:
            await server.serve_forever()
