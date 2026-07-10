import socket


HOST = "127.0.0.1"
PORT = 8000
ID = 1
MAX_RETRY = 3  # 最大重试次数

while True:
    print("请输入要发送的信息，quit退出，回车发送")
    msg_in = input().strip()
    if msg_in == "quit":
        break

    send_txt = f"request_id:{ID},body:{msg_in}"
    send_bytes = send_txt.encode("utf-8")
    resp = None
    success = False
    retry_times = 0

    # 超时重试循环
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
            break  # 收发成功，跳出重试循环
        except socket.timeout:
            print(f"第{retry_times}次超时，", end="")
        except ConnectionRefusedError:
            print("服务端未启动，无需重试")
            break
        except BrokenPipeError:
            print(f"第{retry_times}次连接断裂，", end="")
        except Exception as e:
            print(f"第{retry_times}次异常：{e}，", end="")
        finally:
            client.close()

        # 未到最大次数提示重试
        if retry_times < MAX_RETRY:
            print("准备重试...")

    # 重试全部失败
    if not success:
        print(f"已重试{MAX_RETRY}次，本次消息发送失败")
    else:
        print("服务端回复：", resp.decode("utf-8"))
        ID += 1
