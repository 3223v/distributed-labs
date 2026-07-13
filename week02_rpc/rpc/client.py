import asyncio
from rpc import codec  

class Client:
    def __init__(self, server_host, server_port, client_id, timeout=5.0):
        self.server_host = server_host
        self.server_port = server_port
        self.client_id = client_id
        self.seq = 0          # 每次新请求 +1，重试不变
        self.request_id = 0   # 每次新请求 +1，重试也 +1（用于日志）
        self.timeout = timeout
        self.max_retries = 3

    async def call(self, method: str, params: dict = None) -> dict:
        """发送一次 RPC 调用，自动处理超时和重试"""
        self.seq += 1                     # 新请求的序列号
        self.request_id += 1              # 新请求的ID
        # 固定本次调用的 seq 和 request_id（重试时不改变）
        current_seq = self.seq
        current_req_id = self.request_id

        for attempt in range(self.max_retries):
            # 构造请求
            request = {
                "request_id": current_req_id,
                "client_id": self.client_id,
                "seq": current_seq,
                "method": method,
                "params": params or {}
            }

            try:
                # 建立连接
                reader, writer = await asyncio.open_connection(
                    self.server_host, self.server_port
                )

                # 发送请求（带超时控制）
                encoded = await codec.encode_message(request)
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
                if response.get("request_id") != current_req_id:
                    # 理论上不会发生，但可作为防御
                    print(f"[WARN] 响应 request_id 不匹配: {response}")
                    continue

                # 返回核心字段
                return {
                    "ok": response.get("ok", False),
                    "result": response.get("result", ""),
                    "error": response.get("error", "")
                }

            except asyncio.TimeoutError:
                print(f"[TIMEOUT] request_id={current_req_id} attempt={attempt+1}")
                # 超时重试，request_id 递增
                current_req_id += 1
                # 关闭可能残留的连接
                try:
                    writer.close()
                    await writer.wait_closed()
                except:
                    pass
                continue

            except ConnectionRefusedError:
                print("服务端未启动")
                break   # 服务不可用，直接退出重试

            except Exception as e:
                print(f"[ERROR] {e} attempt={attempt+1}")
                # 其他异常也尝试重试
                current_req_id += 1
                try:
                    writer.close()
                    await writer.wait_closed()
                except:
                    pass
                continue

        # 所有重试失败
        return {"ok": False, "result": "", "error": "max retries exceeded"}

if __name__ == "__main__":
    client = Client("0.0.0.0","8000",1)
    asyncio.run(client.call("get",{
        "key":"test",
        "value":""
    }))