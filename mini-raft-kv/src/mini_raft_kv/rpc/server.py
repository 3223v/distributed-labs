import asyncio
import json
from mini_raft_kv.common import log
from mini_raft_kv.common import codec
from mini_raft_kv.common.command import Command
from mini_raft_kv.common.config import ServerConfig
from mini_raft_kv.common.query import Query
from mini_raft_kv.replication.base import Engine

class Server:

    def __init__(self, eg:Engine, cg:ServerConfig):
        self.eg = eg
        self.cg = cg

    async def handle_client(self, reader, writer):
        addr = writer.get_extra_info('peername')
        log.info("新客户端连接", addr=addr)
        try:
            while True:
                req = await codec.decode_message(reader)
                if req is None:
                    break
                # 版本对比，不对直接错误
                if req["v"] != self.cg["v"]:
                    resp = {
                        "ok" : False,
                        "result" : None,
                        "error": None
                    }
                # 这里对入参json->command 并且后续流转都用command
                # {
                #     "v": 1,
                #     "request_id": 42,
                #     "client_id": "c1",
                #     "seq": 7,
                #     "config_id": 3,
                #     "method": "Put",
                #     "params": {"key": "x", "value": "1","version": 3}
                # }
                cmd = Command(
                    req.get("method"),
                    req.get("params",{}).get("key"),
                    req.get("params",{}).get("value"),
                    req.get("client_id"),
                    req.get("seq"),
                    req.get("params",{}).get("version"),
                    req.get("request_id")
                )
                qry = Query(
                    req.get("params",{}).get("key"),
                    req.get("params",{}).get("config_id","")
                )
                if req.get("method").lower() == "get":
                    resp = await self.eg.query(qry)
                elif req.get("method").lower() == "put":
                    resp = await self.eg.submit(cmd)
                elif req.get("method").lower() == "cas":
                    resp = await self.eg.submit(cmd)
                elif req.get("method").lower() == "del":
                    resp = await self.eg.submit(cmd)
                elif req.get("method").lower() == "ping":
                    resp = {
                        "ok" : True,
                        "result" : {
                            "key" : None,
                            "value" : "pong",
                            "version" : None
                        },
                        "error":None
                    }
                elif req.get("method").lower() == "echo":
                    resp = {
                        "ok" : True,
                        "result" : {
                            "key" : None,
                            "value" : req.get("params",{}).get("value",""),
                            "version" : None
                        },
                        "error":None
                    }
                else:
                    resp = {
                        "ok" : False,
                        "result" : None,
                        "error": None
                    }
                
                # 这里出参应该是
                # {
                #     "ok": True/False,
                #     "result": None,
                #     "error" : None
                # }
                # result = {
                #     "key":"",
                #     "value":"",
                #     "version":""
                # }
                # error = {
                #     "code":"",
                #     "data":"",
                #     "message":""
                # }
                resp["v"] = self.cg["v"]
                resp["request_id"] = req["request_id"]
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
    
    async def run(self):
        server = await asyncio.start_server(
            self.handle_client,
            host = self.cg.host,
            port = self.cg.port
        )
        log.info("RPC 服务启动", host=self.cg.host, port=self.cg.port)
        async with server:
            await server.serve_forever()