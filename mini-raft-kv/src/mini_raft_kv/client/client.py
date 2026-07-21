import asyncio
from mini_raft_kv.common import log
from mini_raft_kv.common import codec

class Client:
    def __init__(self, server_host, server_port, v, client_id, timeout=5.0):
        self.server_host = server_host
        self.server_port = server_port
        self.client_id = client_id
        self.seq = 0          # 
        self.request_id = 0   # 
        self.timeout = timeout
        self.max_retries = 3
        self.v = v
        self.config_id = 1


        # {
        #     "v": 1,
        #     "request_id": 42,
        #     "client_id": "c1",
        #     "seq": 7,
        #     "config_id": 3,
        #     "method": "Put",
        #     "params": {"key": "x", "value": "1" , "version" : 3}
        # }
    
    async def call(self, method: str, params: dict = None) -> dict:
        #  如果是读写，才自增，而且重试不增加      
        if method.lower() == "put" or method.lower() == "cas" or method.lower() == "del" :
            self.seq += 1

        for attempt in range(self.max_retries):
            #request_id 默认自增，发消息就会自增一次
            self.request_id +=1
            req = {
                "v" : self.v,
                "seq" : self.seq,
                "request_id" : self.request_id,
                "client_id" : self.client_id,
                "config_id" : self.config_id,
                "method" : method,
                "params" : params or {}
            }
            try:
                # 建立连接
                reader, writer = await asyncio.open_connection(
                    self.server_host, self.server_port
                )

                # 发送请求（带超时控制）
                encoded = await codec.encode_message(req)
                writer.write(encoded)
                await writer.drain()

                # 等待响应（带超时）
                response = await asyncio.wait_for(
                    codec.decode_message(reader),
                    timeout=self.timeout
                )

                # 关闭连接
                writer.close()
                await writer.wait_closed()

                # 检查响应是否匹配当前 request_id（防止乱序）
                if response.get("request_id") != self.request_id:
                    # 理论上不会发生，但可作为防御
                    log.warn("响应 request_id 不匹配", response = response)
                    continue

                return {
                    "ok" : response.get("ok"),
                    "result" : response.get("result"),
                    "error" : response.get("error")
                }

            except asyncio.TimeoutError:
                log.warn("超时", request_id=self.request_id, attempt=attempt+1)
                # 超时重试，request_id 递增
                self.request_id += 1
                # 关闭可能残留的连接
                try:
                    writer.close()
                    await writer.wait_closed()
                except:
                    pass
                continue

            except ConnectionRefusedError:
                log.info("服务端未启动")
                break   # 服务不可用，直接退出重试

            except Exception as e:
                log.error("异常", e = str(e) ,attempt = attempt +1)
                # 其他异常也尝试重试（request_id 在循环顶部已自增，这里不再重复加）
                try:
                    writer.close()
                    await writer.wait_closed()
                except:
                    pass
                continue

        # 所有重试失败
        return {
                "ok": False, 
                "result": None,
                "error": {
                    "data" : "",
                    "code" : "",
                    "message" : "重试次数耗尽也连不上"
                }
            }