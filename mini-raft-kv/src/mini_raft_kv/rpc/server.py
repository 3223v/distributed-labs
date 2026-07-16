import asyncio
import json
from mini_raft_kv.common import log
from mini_raft_kv.rpc import codec
from mini_raft_kv.kv import ClientTable
from mini_raft_kv.rpc import KvStore

class Server:
    def __init__(self, v):
        self.v = v
        self.kv = KvStore()
        self.ct = ClientTable()
        self.lock = asyncio.Lock()

    async def dispatch(self, req: dict) -> dict:
        request_v = req.get("v")
        request_id = req.get("request_id")
        client_id = req.get("client_id")
        seq = req.get("seq")
        if request_id is None or client_id is None or seq is None:
            return {
                "request_id": request_id if request_id else 0,
                "ok": False,
                "result": None,
                "error": {
                    "code" : "2",
                    "message" :"消息不全",
                    "data" : None
                }
            }
        if request_v is None or request_v != self.v:
            return {
                "request_id": request_id if request_id else 0,
                "ok": False,
                "result": None,
                "error": {
                    "code" :"4",
                    "message" :"v错误",
                    "data" : None
                }
            }
        method = req.get("method", "").lower()
        params = req.get("params", {})
        key = params.get("key", "")
        value = params.get("value", "")
        if method == "put":
            async with self.lock:
                if self.ct.check(client_id,seq) == "new":
                    self.ct.record(client_id,-1,True,None,None)
                
                # 序列号回退 → 错误
                if self.ct.check(client_id,seq) == "stale":
                    log.warn("过期请求，拒绝", client=client_id, seq=seq)
                    return {
                        "request_id": request_id,
                        "ok": False,
                        "result": None,
                        "error": {
                            "code" :"1",
                            "message" :"seq回退，拒绝",
                            "data" :None
                        }
                    }

                # 重复请求 → 返回缓存结果
                if self.ct.check(client_id,seq) == "duplicate":
                    log.info("重复请求", client=client_id, seq=seq)
                    return {
                        "request_id" : request_id,
                        "ok" : self.ct.return_old.get("last_ok",None),
                        "result" : self.ct.return_old.get("last_result",None),
                        "error" :self.ct.return_old.get("last_error",None),
                    }

                # 新请求（seq > last_seq）→ 执行 Put
                kv_resp = self.kv.put(key,value,client_id,seq)
                resp = {
                    "request_id": request_id,
                    "ok": kv_resp.get("ok",None),
                    "result": kv_resp.get("result",None)
                    "error": kv_resp.get("error",None)
                }
                # 更新去重表
                self.ct.record(client_id,seq,resp.get("ok",None),resp.get("result",None),resp.get("error",None))
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
