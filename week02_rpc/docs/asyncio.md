# asyncio 极简完整指南
## 一、底层核心原理
asyncio 是 Python 单线程**异步IO事件循环**，基于操作系统IO多路复用（epoll/kqueue/select）。
- 同步：IO阻塞时卡死整个线程；
- asyncio：IO等待时切走协程，去执行其他就绪任务，**单线程并发**，无线程切换开销。

两个基础概念：
1. **协程 coroutine**：`async def` 定义的函数，调用不执行，只生成协程对象；
2. **事件循环 loop**：调度器，存放所有待运行协程，监听IO就绪、切换任务。

---

# 二、顶层入口 API
## 1. asyncio.run() 程序统一入口
### 用法
```python
async def main():
    pass

if __name__ == "__main__":
    asyncio.run(main())
```
### 内部做了什么
1. 自动创建/获取事件循环；
2. 传入main协程，等待它全部执行完毕；
3. 执行结束自动关闭循环、清理资源；
4. **禁止在协程内部嵌套 run()**。

## 2. 并发执行：asyncio.gather()
批量并发多个协程，等待全部完成，按顺序返回结果列表。
```python
async def a(): return 1
async def b(): return 2

async def main():
    res = await asyncio.gather(a(), b())
    print(res) # [1, 2]
```
内部逻辑：把所有协程打包成Task，丢进事件循环并行调度，全部完成再返回。

## 3. 创建后台任务：asyncio.create_task()
把协程丢进事件循环后台执行，不阻塞当前代码，可稍后await拿结果。
```python
task = asyncio.create_task(do_something())
# 中间可以做其他逻辑
result = await task
```
内部：封装为Task对象，由loop调度；支持取消 `task.cancel()`。

## 4. 睡眠（模拟IO等待）：asyncio.sleep(n)
```python
await asyncio.sleep(1)
```
不会阻塞线程，只是告诉loop：当前协程让出执行权，1秒后再调度。

## 5. 等待IO超时：asyncio.wait_for()
```python
# 3秒内没完成就抛超时异常
res = await asyncio.wait_for(do_work(), timeout=3)
```

## 6. 多任务等待：asyncio.wait()
区分**已完成/未完成**任务，比gather更灵活：
```python
done, pending = await asyncio.wait([task1, task2])
```

---

# 三、TCP网络专用API
## 1. 服务端：asyncio.start_server()
```python
server = await asyncio.start_server(handle_client, "0.0.0.0", 8000)
async with server:
    await server.serve_forever()
```
### 内部完整流程
1. 创建底层TCP socket；
2. 设置端口复用、bind绑定IP端口；
3. listen开启监听；
4. serve_forever() 循环异步accept；
5. 每收到新连接，自动创建Task执行 `handle_client(reader, writer)`；
6. 新连接socket封装成 `StreamReader` / `StreamWriter` 对外暴露。

### StreamReader 常用方法
- `await reader.readexactly(N)`：精准读取N字节，不足则等待，断连抛IncompleteReadError；
- `await reader.read(n)`：最多读n字节；
适配你 length-prefix JSON 分包协议。

### StreamWriter 常用方法
- `writer.write(bytes)`：写入发送缓冲区（非阻塞）；
- `await writer.drain()`：**应用层背压**，缓冲区满时挂起协程，等内核发送完成再继续；
- `writer.close()`：关闭socket；
- `await writer.wait_closed()`：等待连接彻底关闭。

## 2. 客户端：asyncio.open_connection()
```python
reader, writer = await asyncio.open_connection("127.0.0.1", 8000)
```
内部：创建客户端socket，异步三次握手连接服务端，同样包装读写流。

---

# 四、协程语法关键字
## 1. async def 定义协程函数
```python
async def func():
    pass
```
调用 `func()` 不会执行，只返回协程对象，必须用 `await` / create_task 驱动。

## 2. await 交出执行权
只能写在 `async def` 内部；
遇到await时，协程暂停，事件循环调度其他就绪任务，IO完成后切回来继续执行。

## 3. async with 异步上下文管理器
服务、网络流、锁都支持，自动释放资源：
```python
async with server:
    # 退出代码块自动关闭监听socket
```

## 4. async for 异步迭代器（流式数据）
极少用于TCP，一般用于数据库流式查询。

---

# 五、同步/异步互操作API
1. `asyncio.to_thread(func, *args)`
将同步阻塞函数丢进线程池执行，避免卡死事件循环：
```python
data = await asyncio.to_thread(json.load, open("data.json"))
```
2. loop.run_in_executor：底层线程池接口，to_thread是它的简化封装。

---

# 六、锁/信号量（并发资源竞争）
1. `asyncio.Lock()` 互斥锁
同一时间只允许一个协程执行临界区；
2. `asyncio.Semaphore(N)` 信号量
限制最大并发数（如限制同时10个数据库连接）。

用法模板：
```python
lock = asyncio.Lock()
async def work():
    async with lock:
        # 临界区代码
```

---

# 七、极简分层总结
1. **事件循环（底层）**：IO多路复用、协程调度；
2. **协程/Task（中层）**：async def + create_task/gather 实现并发；
3. **网络高层API（业务层）**：start_server / open_connection，封装原生socket；
4. **业务代码**：StreamReader/Writer + length-prefix JSON 收发消息；
5. **背压机制**：`await writer.drain()` 基于事件循环实现应用层负反馈限流。

# 八、高频点
1. 普通函数不能await，只有协程、Task、Future可以await；
2. 同步IO（time.sleep、socket.recv、文件读写）会阻塞整个事件循环，要用 `to_thread`；
3. `readexactly` 只会抛断连异常，不会返回空字节；
4. 多客户端自动并发由start_server内部Task实现，无需手动创建任务；
5. `asyncio.run()` 是现代标准，替代旧写法 `loop = asyncio.get_event_loop()`。