import socket
import threading
import time
import random

serve = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serve.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
HOST = "0.0.0.0"
PORT = 8000
serve.bind((HOST, PORT))
# listen(n)：将socket转为监听状态，开启TCP握手接收，设置最多n个已完成握手、等待程序取走的连接缓存。
serve.listen(5)
THREADING  = 0
print(f"info:服务启动，监听端口：{PORT}")

def parse_kv_text(text):
    # 只按第一个逗号分割，body 里可以有逗号
    part1, body_part = text.split(",", 1)
    id_val = part1.split(":", 1)[1]        # "1"
    body_val = body_part.split(":", 1)[1]  # "hello, world"
    return {"request_id": int(id_val), "body": body_val}

def deal_recv(addr,conn):
    print(f"info:新建线程处理{addr}的连接")
    num = random.randint(0,5)
    if num==0:
        time.sleep(10)
        print(f"这个请求运气不好，测试超时")
    with conn:
        while True:
            recv_bytes = conn.recv(1024)
            if not recv_bytes:
                print(f"info:客户端{addr}断开")
                break
            recv_txt = recv_bytes.decode("utf-8")
            info = parse_kv_text(recv_txt)
            id = int(info["request_id"])
            content = info["body"]
            print(f"收到{addr}消息:{recv_txt}")

            send_txt = f"request_id:{id},ok:true,body:{content}"

            send_bytes = send_txt.encode("utf-8")

            try:
                conn.sendall(send_bytes)
            except BrokenPipeError:
                # 客户端已断开，停止发送，退出循环
                print(f"客户端 {addr} 连接已断开，无法发送数据")
                break
    print(f"info:处理{addr}连接线程结束")

threads = []
while True:
    conn,addr = serve.accept()
    t = threading.Thread(target = deal_recv,args = (addr,conn))
    threads.append(t)
    t.start()
    THREADING = THREADING + 1
    print(f"共启动线程数量{THREADING}")
