1. Socket 核心定位
- Socket 不属于 TCP/IP 四层网络协议栈（不属于应用层/传输层/网络层/链路层）
- Socket 是 操作系统提供的系统调用抽象接口
- 作用：应用程序 ↔ 内核 TCP/IP 协议栈 的唯一通信桥梁
- 核心逻辑：应用程序无法直接操作端口、TCP、IP、网卡，所有网络操作必须通过 Socket
架构层级：应用代码 → Socket API → 内核协议栈(TCP/IP) → 网卡硬件
2. Socket 创建两大核心参数
源码：socket.socket(family, type)
2.1 AF_INET（地址族）
- 含义：使用 IPv4 协议
- 数值：2
- 替代：AF_INET6（IPv6）
2.2 SOCK_STREAM（套接字类型）
- 含义：TCP 流式套接字
- 特性：面向连接、可靠、有序、有重传机制
- 数值：1
2.3 补充常用组合
- AF_INET + SOCK_STREAM = IPv4 + TCP（主流）
- AF_INET + SOCK_DGRAM = IPv4 + UDP（无连接、数据包模式）
3. setsockopt 分层配置（内核调参）
格式：socket.setsockopt(level, 配置项, 值)
作用：不改变程序层级，仅微调内核网络协议栈底层规则
3.1 三大层级 Level
- SOL_SOCKET（层级1）：Socket 通用层，管控套接字本身属性（和TCP/IP协议无关）
- IPPROTO_TCP（层级6）：TCP 传输层，管控TCP协议传输规则
- IPPROTO_IP（层级0）：IP 网络层，管控IP寻址、路由规则
3.2 最常用配置详解
SO_REUSEADDR（SOL_SOCKET 层级）
- 作用：释放 TIME_WAIT 占用的端口，解决重启报错 Address already in use
- 限制：无法实现多进程/多Socket同时监听同一个TCP端口
SO_REUSEPORT（SOL_SOCKET 层级）
- 作用：支持 多个Socket绑定同一个TCP端口，内核自动负载均衡
- 用途：单端口部署多服务、高并发服务扩容
4. 端口与 Socket 核心
4.1 核心结论
- 端口：仅是内核的数字标记，用于区分本机不同通信服务，无独立操作能力
- Socket：端口的软件抽象载体，程序只操作Socket，不直接触碰端口
- 单个Socket 只能绑定1个端口
- 单个端口 可被多个Socket复用（需对应配置支持）
4.2 TCP / UDP 端口绑定差异
- TCP：默认单端口仅允许1个监听Socket；开启SO_REUSEPORT 支持多Socket同端口监听
- UDP：天然支持多Socket绑定同一端口，无需复杂配置
4.3 监听Socket vs 连接Socket
- 监听Socket：绑定固定业务端口（如8888），负责监听客户端连接
- accept() 生成的连接Socket：不占用业务端口，使用系统随机临时端口，支持万千客户端并发连接
5. 层级本质终极总结
1. Python服务永远运行在应用层，仅处理业务数据（HTTP、自定义协议等）
2. TCP、IP、分包、重传、路由均由操作系统内核自动完成
3. Socket是中间抽象层：隔离应用与底层网络，屏蔽硬件、协议细节
4. setsockopt 是内核调参工具，只改底层规则，不改变程序层级
5. 单端口多服务的核心：依靠Socket端口复用配置，而非端口本身功能
6. Socket不属于任何一个网络层级，属于IP到程序直接的一个软件抽象，用户可以通过socket对各层级网络进行配置