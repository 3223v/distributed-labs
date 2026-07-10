import socket
import json
import random
import threading
import time

HOST = "127.0.0.1"
PORT = 8000
MAX_RETRY = 3
NUM_THREADING = 100

lock = threading.Lock()
counter = 0
ID = 0

def client_send():
    global counter, ID
    with lock:
        counter += 1
        ID += 1
        local_id = ID          # 复制一份避免锁外使用
    msg_in = "压测"
    send_txt = f"request_id:{ID},body:{msg_in}"
    send_bytes = send_txt.encode("utf-8")

    resp = None
    success = False
    retry_times = 0

    while retry_times < MAX_RETRY:
        retry_times += 1
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        client.settimeout(5.0)
        try:
            client.connect((HOST, PORT))
            client.sendall(send_bytes)
            resp = client.recv(1024)
            success = True
            break
        except socket.timeout:
            print(f"线程{local_id} 第{retry_times}次超时，", end="")
        except ConnectionRefusedError:
            print("服务端未启动，无需重试")
            break
        except BrokenPipeError:
            print(f"线程{local_id} 第{retry_times}次连接断裂，", end="")
        except Exception as e:
            print(f"线程{local_id} 第{retry_times}次异常：{e}，", end="")
        finally:
            client.close()

        if retry_times < MAX_RETRY:
            print("准备重试...")

    if not success:
        print(f"线程{local_id} 已重试{MAX_RETRY}次，发送失败")
    else:
        print(f"线程{local_id} 服务端回复：{resp.decode('utf-8')}")

# 启动所有线程
threads = []
for _ in range(NUM_THREADING):
    t = threading.Thread(target=client_send)
    threads.append(t)
    t.start()

# 等待所有线程结束
for t in threads:
    t.join()

print(f"共启动线程数量：{counter}")