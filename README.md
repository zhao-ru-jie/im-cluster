# im-cluster — 可运行的 IM 集群骨架

一个能真跑起来的 IM 后端骨架：客户端 A 连在网关 1、客户端 B 连在网关 2（不同 JVM / 不同机器），
A 发给 B 的消息经 **Redis 在线路由表定位目标网关 + gRPC 实例间转发** 送达；网关不可达时转入离线队列，
用户重连后补齐。长连接同时支持 **WebSocket 与原生 TCP**，消息存储可在 **Redis 与 MySQL** 之间按配置切换。

工程的重点不是「能发一条消息」，而是把 IM 系统绕不开的几件事做对：
**幂等、保序、至少一次投递、在线路由、心跳探测死连接、优雅摘流、按连接数扩缩容**。

---

## 目录

- [技术栈](#技术栈)
- [架构总览](#架构总览)
- [长连接层（Netty 双通道）](#长连接层netty-双通道)
- [存储层](#存储层)
- [服务间通信](#服务间通信)
- [可靠性设计](#可靠性设计)
- [可观测性与弹性](#可观测性与弹性)
- [快速开始](#快速开始)
- [配置参考](#配置参考)
- [代码结构](#代码结构)
- [已知约束与坑](#已知约束与坑)

---

## 技术栈

| 领域 | 选型 | 说明 |
|------|------|------|
| 运行时 | **JDK 21+**（已验证 25）、虚拟线程 | 业务线程不阻塞在 IO 上 |
| 框架 | **Spring Boot 4.1.x** | Jackson 3、PEM SSL Bundle、gRPC starter |
| 长连接 | **Netty 4.2**（自建 ServerBootstrap） | WebSocket + 原生 TCP 双通道，不依赖 Tomcat 承载长连接 |
| 服务间通信 | **gRPC**（`spring-boot-starter-grpc-server`） | 双向：上行 `MessageIngress`、下行 `DownstreamPush` |
| 状态存储 | **Redis** | 在线路由、离线队列、seq 分配、幂等表 |
| 消息存储 | **Redis / MySQL / MongoDB 可切换** | `im.storage` 决定实现，同一份 jar |
| 服务发现 | **Redis 路由表 + Nacos（可选）** | 路由由 Redis 承担，Nacos 只做「实例 ID → 可达 ip:port」 |
| 部署 | Docker / Kubernetes | HPA 按连接数扩缩容 |

---

## 架构总览

### 模块划分

| 模块 | 角色 | 职责 | 产出物 |
|------|------|------|--------|
| `im-proto` | 契约层 | 两份 proto + 端口接口 `MessageIngress` / `PushPort` + `MessageEnvelope` + Redis key 契约 | 依赖 jar |
| `im-rpc` | gRPC 适配（客户端侧） | 按方向分包：`rpc.common`（channel 复用与 TLS、地址解析）、`rpc.ingress`（网关→IM：`RemoteMessageIngress`）、`rpc.push`（IM→网关：`RemoteGatewayPushPort` + 在线路由只读视图） | 依赖 jar |
| `im-gateway` | **长连接服务** | Netty 双通道接入、会话管理、心跳、上行转发、下行推送、优雅摘流、连接数指标 | 库 |
| `im-server` | **IM 服务** | 消息受理（幂等 + seq）、路由决策、离线兜底、历史消息、群成员；**不含任何存储实现** | 库 |
| `im-storage-redis` | 存储实现插件（缺省） | `MessageRepository` / `GroupRepository` / `GroupMemberRepository` / `RelationRepository` 的 Redis 实现 | 依赖 jar |
| `im-storage-jdbc` | 存储实现插件（生产） | `MessageRepository` / `GroupMemberRepository` 的 MySQL 实现 + DDL | 依赖 jar |
| `im-storage-mongodb` | 存储实现插件（文档形态） | 四个存储端口的 MongoDB 实现；实体映射（`@Document` / `@MongoId` / `@CompoundIndex`）+ 注解驱动的索引创建 | 依赖 jar |
| `im-app` | 单体装配入口 | 同进程装配网关 + IM 服务，可执行 fat jar | fat jar |
| `im-gateway-boot` | 拆分部署：长连接服务 | 只做接入，上行全走 gRPC | fat jar |
| `im-server-boot` | 拆分部署：IM 服务 | 只承载业务与存储，暴露 `MessageIngress`，经 `RemoteGatewayPushPort` 下发 | fat jar |

### 依赖方向（编译期单向）

```
im-app   ──> im-gateway ──┐
         ──> im-server ───┼──> im-proto
         ──> im-rpc ──────┘

im-gateway-boot ──> im-gateway / im-rpc ──> im-proto
im-server-boot  ──> im-server  / im-rpc / im-storage-redis / im-storage-jdbc / im-storage-mongodb ──> im-proto

im-storage-redis   ──> im-server ──> im-proto
im-storage-jdbc    ──> im-server ──> im-proto
im-storage-mongodb ──> im-server ──> im-proto
```

三条硬约束：

1. **`im-gateway` 与 `im-server` 互不依赖**，唯一共享物是 `im-proto`；
2. 业务模块不依赖 `im-rpc`，保持「业务代码里看不到传输细节」；
3. `im-storage-*` 都只单向依赖 `im-server` 的存储端口，**反向不依赖**——换存储实现不改业务。
   jdbc 模块刻意不用 `spring-boot-starter-jdbc`，避免只用 Redis 的部署被自动装配出一个连不上的 DataSource；
   mongodb 模块同理排除 Boot 的 Mongo 自动装配，避免 Redis 形态多出一个连不上 `localhost:27017` 的客户端。
   三者由 `im.storage` 三选一装配（`redis` 或缺省 / `jdbc` / `mongodb`），互斥条件写在各自的 `*StorageConfig` 里。

### 运行时拓扑

**单体装配**（`im-app`，一个 JVM 里跑两套逻辑，端口各自独立）：

```
                    ┌──────────────── im-app (一个 JVM) ────────────────┐
客户端 ──WS/TCP───> │ Netty(8081/9092) → 网关逻辑 → 本地直调 IM 服务    │
                    │ Tomcat(8080)     → REST / 测试页 / admin          │
                    └──────────────────────┬───────────────────────────┘
                                           │ Redis（路由/离线/seq/幂等/消息）
```

**拆分部署**（网关与 IM 服务各自扩容）：

```
客户端 ──WebSocket/TCP──> im-gateway ──gRPC MessageIngress.SubmitMessage──> im-server ──> Redis / MySQL
                             ↑                                                │
                             └────────── gRPC DownstreamPush.Push ────────────┘

im-gateway ──写──> Redis(im:gateway:user:{userId} = SET{网关实例})
im-server  ──读写──> Redis(im:conv:msgs / im:offline / im:seq / im:idem)（im.storage=jdbc 时消息落 MySQL，
                                                                         =mongodb 时落 MongoDB 的 im_message）
```

### 一条消息的完整链路

以「网关 1 上的 alice 发给网关 2 上的 bob」为例：

1. **接入**：alice 的帧走 WebSocket 文本帧或 TCP 长度帧到达网关 1，`SessionLifecycle` 解析出 `msg.send`；
2. **上行**：网关经 `MessageIngress.submit()` 交给 IM 服务（同进程直调，或 gRPC 远程）；
3. **幂等 + 保序（一次 Redis 往返）**：一个 Lua 脚本同时完成「查 `(from, clientMsgId)` 幂等表 +
   按 `conversationId` 原子递增取 `seq`」。重复提交直接回放原 `seq` / `serverMsgId`（`duplicate=true`），
   并且**不消耗序号**；新消息只需 1 次 RTT。幂等记录仍在**落库成功之后**才写——
   提前写会让「落库失败 + 客户端重试」命中一个实际不存在的 seq，消息就真丢了；
4. **标识**：`serverMsgId` 独立生成（UUIDv7），与 seq **解耦**——seq 允许有洞、回绕也不会撞 ID；
5. **落库（write-ahead）**：经 `MessageRepository.append()` 写存储——先落库，再推送；
6. **ack 先行**：落库成功 + 幂等记录写入 = 受理成功，**立刻回 `msg.ack`**（含 `serverMsgId`、`seq`），不等推送；
7. **异步投递**：`DeliveryDispatcher` 在虚拟线程池里做路由与下发——读 `im:gateway:user:{bob}`（集合）得到
   bob 所在的**所有**网关实例（Nacos 启用时解析成健康 ip:port），`PushPort.push()` 到这些实例
   （**跨实例并行**，总耗时 ≈ 最慢的那个实例，而不是 RTT 相加），各实例再按 `userId` 找**本机全部设备**写帧；
8. **离线兜底**：投递失败（不在线 / 推送异常 / 超时）→ 单聊把帧落 `im:offline:{bob}`，
   群聊只在 `im:offline:ptr:{bob}` 记一个「补到哪」的指针，bob 重连时补发（见「离线消息」一节）。

关键取舍：**先落库后推送**（宁可重复推送，不可丢消息）、**推送失败即转离线**（不做无限重试）、
**ack 不等推送**（受理成功的判据只有落库，实时性交给异步投递 + 离线补发）、
`frame` 由 IM 服务序列化后网关**原样透传不解析**（新增业务字段不需要动网关）。

ack 先行换来「ack 延迟 = 落库延迟」的代价，三条约束必须一起成立：

1. **客户端必须按 seq 排序**：异步投递会让实时到达顺序偶尔乱（`seq=2` 先到），这在 IM 里是常规约定，
   业务侧不能假设「先收到的 seq 更小」；
2. **进程在「已 ack、未推送」窗口崩溃时，这条消息只在库里**：不丢，但要等客户端重连后按 seq 拉历史；
   所以关闭时会 drain 最多 5s，把这批推送做完或落离线；
3. **推送并发有上限**（`im.delivery.max-inflight`，默认 1024）：打满时**退化成同步推送**——
   宁可 ack 慢几十毫秒，也不让推送任务把堆内存堆穿（那时丢的是整条链路）。

### seq 发号与三道下界

发号器**每条消息一次 Redis 原子 `INCR`**（Lua，key 为 `im:seq:{conversationId}`），返回值就是
所有实例共同认可的会话内顺序。代价是一次 Redis 往返（本机 0.2~0.5ms，相对上行 300ms 的 deadline 可忽略）。

**为什么不做本地号段缓存**：一次 `INCRBY N` 把号段缓存在进程内，会让同一会话的 seq 跨实例交错
（A 发 1、2、3，B 发 101、102、103 → 客户端看到 `1, 101, 2, 102`）。客户端补洞的常规实现是
「`seq <= lastSeq` 视为旧消息丢弃」，交错后 A 的 2、3 会被**永久丢掉**——不是可接受的跳号，而是
保序语义被破坏、离线 FIFO 与 seq 打架、水位失去意义。seq 允许有洞（客户端按 `lastSeq` 区间补洞），
但**不允许不单调**。

下界取三者最大值，用一段 Lua 原子地把计数器抬到 `max(计数器, 下界)` 之后再 `INCR`：

| 下界来源 | 覆盖的故障 | 失效条件 |
|---|---|---|
| 本进程已知的最大 seq | 只丢 `im:seq:*` 计数器 | 进程重启 |
| 已落库最大 seq（`MessageRepository.maxSeq`，每 100 条校准一次） | 只丢 `im:seq:*` 计数器 | 消息存储与计数器同库、且一起丢 |
| 客户端上报水位（`sync` 帧） | 消息存储与计数器**一起**丢 | 客户端从未上报过该会话 |

前两路住在「存储 / 本进程」里，第三路住在「客户端手里」——这正是它在 Redis 整体丢失时
仍然有效的唯一原因。缺了第三路，服务端会把 seq 从 1 重发，而客户端 `lastSeq` 已经推进到 N，
新消息被判成历史重复直接丢弃，表现为「消息永久不显示」（数据没错，错的是服务端水位低于客户端水位）。

---

## 长连接层（Netty 双通道）

### 为什么不用 Spring WebSocket

| 维度 | Tomcat + Spring WebSocket | Netty（本项目） |
|------|---------------------------|-----------------|
| 连接模型 | 一条连接一个线程栈（虚拟线程下仍有 Servlet 栈开销） | 少量 event loop 轮转，空闲连接几乎零成本 |
| 写线程安全 | `WebSocketSession` 非线程安全，必须 `synchronized` | `writeAndFlush` 由 event loop 串行执行，慢客户端不阻塞业务线程 |
| 协议扩展 | 只有 WebSocket | 同一套 `ServerBootstrap` 再加一条原生 TCP pipeline 即可 |
| 背压 | 无感知 | `channel.isWritable()` 判断出站水位，积压时按投递失败处理 |

Tomcat 仍然保留，但只服务 `/admin/*`、`/actuator/*`、测试页面等短请求。

### 上行业务的线程模型（event loop 不碰阻塞）

一条 `msg.send` 的链路上有一次同步 gRPC（≤300ms）和若干 Redis/DB 往返。如果直接在
`channelRead0` 里做，event loop 线程被占住，**挂在同一个线程上的所有连接一起停摆**，
单实例上行并发被锁死在 `2×CPU`，而且任何一个下游抖动都会放大成全实例卡死。

```text
event loop:  解码 → serial.offer(帧) → 立刻返回，继续服务其它连接
虚拟线程:    onOpen / onFrame / onClose 串行执行（每连接一个 SerialExecutor）
```

| 组件 | 职责 |
|---|---|
| `transport/BusinessDispatcher` | 虚拟线程池（`newVirtualThreadPerTaskExecutor`），为每条连接发一个 `SerialExecutor` |
| `transport/SerialExecutor` | 同一连接的任务 **FIFO 严格串行**：保序（connect 先于 msg.send）、但绝不占 event loop |
| `*FrameInboundHandler` | 只做「取数据 + 提交任务」，认证/会话判断也放在任务体内（否则连发两帧时会误判未认证） |

两条边界：

- **背压是「暂停读」而不是「断连」**：单连接积压超过 `im.gateway.biz.max-pending-frames`
  （默认 1024 帧，**每连接独立计数**）时执行 `setAutoRead(false)`，从 socket 停止读取；
  积压降到高水位一半时自动恢复。
  - 为什么不断连：客户端典型行为是「本地发件箱一次 flush」（离线期间攒了上千条），
    断连只会让它重连后重发同一批 → **越重试越糟的活锁**。
  - 为什么不丢帧：已经读进内存的这一批丢掉会破坏消息顺序语义，宁可让它排队。
  - 内存有界：暂停后数据停在客户端/内核缓冲区，TCP 窗口收缩，客户端自己就被限速；
    上限约为 `maxPending` + 一次 read 批次的帧数。
  - 最后的兜底：暂停读期间收不到任何上行，`IdleStateHandler` 45~90s 后判定超时断开，
    卡死的连接不会永久占着内存。
- **清理任务不走背压**：`onClose` 用 `submit` 而不是 `offer`——它要是被推迟到队列末尾，
  会话表和 Redis 路由就会留下脏数据（TTL 内消息会继续推到死连接上）。

> 注意 `max-pending-frames` 是**每连接**的：5000 人群里 5000 个用户各发一条，是 5000 条连接各
> `pending=1`，与群规模无关；群聊的**下行扇出**走 `ConnectionManager.send → writeAndFlush`，
> 根本不经过 `SerialExecutor`。

**代价**：同一连接内处理是串行的，单连接吞吐 = `1 / 单条消息耗时`（约 300 QPS @ 3ms）。
这是保序换来的，真实客户端一条连接也不需要更高；要更高并发应该「多连接」而不是「单连接并发」。
换来的是**实例级**上限：不再被 `2×CPU` 锁死，只受 Redis/DB/gRPC 这些真实下游限制。

**已验证**（本机单体实例 + 真实客户端）：WS 两个连接 alice→bob，双方拿到同一条 `seq=1` 的
`msg.ack` / `msg.push`；TCP 把 `connect` 与 `msg.send` **连发**（不等 `connected` 应答）时，
依次收到 `connected` → `msg.push` → `msg.ack`，证明串行队列确实保证「先认证后业务」；
客户端断开后 `/admin/status` 的 `connections` 从 2/1 归 0，虚拟线程承担清理也不泄漏会话。
背压也实测过：把高水位临时配成 2、同一连接连发 5 帧，服务端日志出现
`积压 3 帧超过高水位 2，暂停读取`，而 5 帧的 `msg.ack`（seq 1~5）**全部收到、连接未断**；
「暂停只触发一次 / 降到低水位才恢复 / 一帧不丢」由 `SerialExecutorTest` 覆盖。

配套的三件事：

- **背压可观测**：`gateway_biz_backpressure_paused_total` / `resumed_total` / `paused_active`
  三个指标（由 `BusinessDispatcher` 统一挂到每个连接，handler 只负责 `setAutoRead`），
  以及 `/admin/status` 的 `backpressure.pausedConnections`——没开 Prometheus 也能在本地看到。
- **关闭要收尾**：`BusinessDispatcher` 销毁时等 2s 让已入队的 `onClose` 跑完，
  否则刚断开的那些用户会在 Redis 路由里多留一个 TTL 周期的脏数据。
- **出站要合批**：两条 pipeline 都挂了 `FlushConsolidationHandler(256)`，
  群聊扇出时成百上千次 `writeAndFlush` 合并成少量系统调用（`consolidateWhenNoReadInProgress`
  保证没有读进行时立刻 flush，不引入额外延迟）。

### 双通道与端口

| 传输 | 默认端口 | 接入地址 | 身份来源 |
|------|---------|----------|----------|
| WebSocket | `8081`（`IM_WS_PORT`） | `ws://host:8081/ws?userId=alice&deviceId=web&token=...` | 握手 query 参数 |
| 原生 TCP | `9092`（`IM_TCP_PORT`） | 裸 TCP 连接 | 首帧 `connect`（含 `token`） |

> `userId` 一律必填；`token` 是否必填取决于 `im.auth.mode`（默认 `off`，见「接入鉴权」）。

两种传输**共用同一份会话抽象**（`ClientSession`）与同一份生命周期实现（`SessionLifecycle`）：
连接登记、心跳应答、上行转发、掉线清路由只有一份代码。传输差异被收敛到极小的位置：

- `NettyClientSession.send()`：WS 写文本帧、TCP 写 `String`（由编码器变成长度帧）；
- `close()`：WS 先发 1001（going away）再断，TCP 直接断链。

两条 pipeline：

```
WebSocket: HttpServerCodec → HttpObjectAggregator → WebSocketHandshakeGate(摘流503/路径/userId)
           → IdleStateHandler(计时) → WebSocketServerProtocolHandler
           → StringToWebSocketFrameEncoder → HeartbeatHandler → WebSocketFrameInboundHandler

TCP      : IdleStateHandler(按原始字节计时) → LengthFieldBasedFrameDecoder(4B) → LengthFieldPrepender(4B)
           → StringDecoder/Encoder(UTF-8) → HeartbeatHandler → TcpFrameInboundHandler(首帧 connect 认证)
```

> 计时器放在协议处理器**之前**（WS 协议级 Ping 会被协议处理器吃掉，放后面会误判死连接）；
> 心跳策略放在出站编码器**之后**（否则裸 `String` 会漏到字节层）。

### 接入协议

**WebSocket**：标准 HTTP Upgrade，`userId` 必填、`deviceId` 可选。摘流中的实例在握手阶段直接返回 503，
路径不匹配 404、缺 `userId` 400 —— 都在 101 之前决定，客户端拿得到明确状态码。

**原生 TCP**：帧格式为「4 字节大端长度 + UTF-8 JSON」，JSON 内容与 WebSocket 完全相同：

```text
[0x00 0x00 0x00 0x3A][{"ver":1,"type":"msg.send","to":"bob","body":"hi"}]
```

连上后**首帧必须是认证帧**（默认 10s 内不发会被断开，避免半开连接堆积）：

```json
{"ver":1,"type":"connect","userId":"alice","deviceId":"native"}
```

网关回 `{"type":"connected","userId":"alice","transport":"tcp"}` 后即可正常收发；认证前的其他帧一律拒绝并断开。
鉴权开启时连接帧里还要带 `token`，缺失/无效会收到 `{"type":"error","code":401,...}` 并被断开。

### 应用层帧

```jsonc
// 客户端 → 网关
{"ver":1,"type":"msg.send","clientMsgId":"c-1","to":"bob","body":"hi","traceId":"可选"}
{"ver":1,"type":"msg.send","clientMsgId":"c-2","to":"bob","body":"",
 "contentType":"IMAGE","attrs":"{\"url\":\"https://cdn/a.png\",\"w\":800,\"h\":600}"}   // 富媒体
{"ver":1,"type":"ping"}
{"ver":1,"type":"sync","watermarks":{"s:alice:bob":100}}          // 上报各会话已见最大 seq
{"ver":1,"type":"msg.recv","acks":[{"conversationId":"s:alice:bob","seq":7}]}    // 端到端「已送达」（可攒批）
{"ver":1,"type":"msg.read","conversationId":"s:alice:bob","seq":7}               // 「已读」（用户级，只前进）
{"ver":1,"type":"connect","userId":"alice","deviceId":"native","token":"..."}   // 仅 TCP 首帧

// 网关 → 客户端
{"ver":1,"type":"msg.ack","clientMsgId":"c-1","serverMsgId":"0192f0c4-8a3b-7c2d-9f01-4b7a2c8d5e60","seq":7,"code":0,"traceId":"..."}
{"ver":1,"type":"msg.push","conversationId":"s:alice:bob","convType":"SINGLE","seq":7,
 "serverMsgId":"0192f0c4-8a3b-7c2d-9f01-4b7a2c8d5e60","clientMsgId":"c-1","from":"alice","to":"bob",
 "body":"hi","contentType":"TEXT","ts":1750000000000}
{"ver":1,"type":"msg.state","conversationId":"s:alice:bob","convType":"SINGLE",
 "state":"READ","seq":7,"userId":"bob","ts":1750000000000}      // 发给发送方的状态回执
{"ver":1,"type":"msg.update","conversationId":"s:alice:bob","convType":"SINGLE",
 "seq":7,"change":"RECALLED","actorId":"alice","ts":1750000000000}   // 变更事件（撤回/本地删除）
{"ver":1,"type":"pong","ts":1750000000000}
{"ver":1,"type":"error","clientMsgId":"c-1","code":503,"message":"im service unavailable"}
{"ver":1,"type":"session.migrate","reason":"scale-in","retryAfterMs":1500}
```

`msg.push` 与 `GET /conv/{conversationId}/messages` 返回的历史消息**同构**（同一份模板渲染），
客户端「拉历史 + 接实时帧」不需要两套解析。旧报文 `{"to":...,"body":...}`（不带 `type`）仍然可用。

#### 消息状态机

一条消息在客户端可见的状态有四个，来源刻意分开（混在一起就没法回答「用户到底收到没有」）：

| 状态 | 帧 / 接口 | 谁产生 | 粒度 |
|---|---|---|---|
| 已发送（服务端受理） | 上行 `msg.send` → 下行 `msg.ack` | IM 服务（落库 + 幂等记录） | 单条 |
| 已送达（设备确认收到） | 上行 `msg.recv` → 下行 `msg.state`(DELIVERED) | **客户端**（唯一可信来源） | 设备 × 会话（`im:wm:{userId}:{deviceId}`） |
| 已读 | 上行 `msg.read` 或 `POST /conv/{id}/read` → 下行 `msg.state`(READ) | 客户端 | 用户 × 会话（`im:user:read:{userId}`） |
| 网关已写入连接 | 内部 `AckPending`（不出帧） | 网关 `writeAndFlush` 成功 | 设备 × 会话 |

- `msg.recv` **必须攒批**（几条或 50~100ms 一次），逐条 ACK 会把「消息数」放大成「RPC 数」；
  服务端按会话取 max，重复上报无害。
- 已送达 / 已读回执**只推单聊**：群聊的「已读」是按人统计的集合，扇出是「成员数 × 消息数」，
  属于另一个模型（群已读聚合），塞进单点回执只会得到一条没人能解释清楚的帧。
- 回执**只前进**：进程内按 `会话 + 状态` 去重，重复上报不会让对端收到一串相同帧；
  推送失败也不重试——下一条消息会带更大的 `seq` 把对端显示自愈。
- 端到端 ACK 会**抬高补发起点**（`max(锚点, 设备游标+1, 客户端水位+1)`），
  所以确认过的消息不会再被补发一遍；ACK 不是指标，它改变投递行为。

#### 消息生命周期：撤回与删除

| | 撤回（RECALL） | 本地删除（DELETE） |
|---|---|---|
| 影响范围 | **会话级**：会话里所有人都看不到原文 | **用户级**：只有操作者自己看不到 |
| 谁能做 | 只有发送方，且在 `im.recall.window` 内（默认 2 分钟） | 任何人，对自己视角 |
| 存储 | `im_message.recalled_at`（jdbc）/ `im:msg:recalled:{conv}`（redis） | `im_message_delete`（jdbc）/ `im:msg:del:{userId}:{conv}`（redis） |
| 帧的目标 | 会话内所有人（含发送方自己的其他端） | 只有操作者自己（同步给他的其他端） |
| 列表里 | 保留占位，`body` 为空 + `recalled:true` | 直接消失 |

```
POST /conv/{conversationId}/messages/{seq}/recall?operatorId=alice
POST /conv/{conversationId}/messages/{seq}/delete?userId=alice
GET  /conv/{conversationId}/messages?userId=alice&limit=50            ← 第一页（最新）
GET  /conv/{conversationId}/messages?userId=alice&limit=50&beforeSeq=   ← 上滑翻页
     带 userId 才会按他的删除记录过滤
```

**历史翻页用排他的 seq 游标，不用 offset。** 响应除了 `messages` 还带两个字段：

| 字段 | 含义 |
|------|------|
| `nextBeforeSeq` | 下一页游标（取 `seq < 它`）；原样带回来即可继续上滑，`0` 表示到头了 |
| `hasMore` | 存储里是否还有更早的消息（服务端用「多要一条」探测，不额外做 count） |
| `limit` | 服务端实际生效的条数（不传 → 50，超限 → 夹到 200） |

seq 单调且只追加，所以「翻页期间来了新消息」不会让切片错位；offset 会整体偏移，表现为上滑时重复或漏消息。
另一个关键细节：`nextBeforeSeq` 取的是**本页从存储里读到的最老一条**，不是「本页可见的最老一条」——
整页都被本地删除过滤掉时游标仍然前进，客户端最多滑过几个空页，不会卡在同一页反复请求。

**最关键的一条：脱敏必须在读路径上做，而不只是广播事件。** 撤回有三处会「把原文端出来」的入口，
全部都要过同一道处理：

1. 历史接口（`MessageService#history`）；
2. 群聊补发（按 seq 从历史重建帧）；
3. **单聊补发**——离线队列里存的是「发送时刻的帧快照」，它不会自己变。所以补发出口处会逐帧
   解析出 `conversationId + seq`，按最新状态改写成 `recalled:true` 并把 `body`/`attrs` 抹掉。

只做第 1 条是最容易犯的错：用户重连一次，撤回的消息就全回来了。

**为什么 `msg.update` 不复用 `msg.state`**：`state` 是「走到哪一步」（DELIVERED/READ，单调前进、
按会话去重、只推新高），`update` 是「内容或可见性变了」（可针对任意 seq、不单调、撤回要广播给
所有人而删除只给自己）。两者共用同一条下行通道（`PushPort`），但去重口径不同 ——
`msg.state` 只推新高，`msg.update` 只推一次（`change|conv|seq|userId`）。

三个明确的取舍：

- **删除不减未读数**：未读是 `lastSeq - readSeq` 算出来的，删掉一条未读消息不会改变两个游标。
  主流客户端会「删一条、角标减一」，那是计数模型的能力；游标模型的代价，写在这里而不是假装没有。
- **撤回不物理删除**：消息本体留着（多端要画「已撤回」占位、审计要留痕）。真正的物理清除是
  「合规删除」，需要级联到对象存储，属于另一个动作。
- **查询撤回状态失败时按未撤回下发**（记 error）：反过来会在存储抖动时把正常消息<b>永久</b>
  变成「已撤回占位」，那是不可恢复的错误；而按原文下发的窗口只有一次补发。

#### 富媒体

- `contentType`：`TEXT`（缺省）/ `IMAGE` / `FILE` / `VOICE` / `VIDEO` / `CUSTOM`。
- `attrs`：媒体元数据的 JSON **字符串**（`url` / `size` / `name` / `duration` / `width` / `height` / `thumb`），
  网关与 IM 服务**都不解析**——媒体字段的演进速度远快于消息通道，逐字段建模会让每加一个属性
  都动一次协议与表结构。
- 文件本体**不走本通道**：客户端直传对象存储，消息里只走 URL。把二进制塞进消息链路会拖慢
  seq 发号、幂等表与离线队列，而它们都不需要看到字节。
- 纯文本消息的帧里不带 `attrs`（保持与旧版一致的形状与长度）；`contentType` 始终下发。
- 会话列表摘要按类型渲染：媒体消息显示 `[图片]` / `[文件]` / `[语音]` / `[视频]`，
  这样算摘要时**不需要反序列化 body**，也不必为了列表去解析 `attrs`。

### 接入鉴权（token）

三档开关，默认 `off`（保持本地联调无需凭据）：

| `im.auth.mode` | 行为 | 用途 |
|---|---|---|
| `off`（默认） | 不校验，只认 `userId` | 本地联调、纯内网 |
| `warn` | **校验并记录**，但一律放行 | 上线前观察：看 `gateway_auth_rejected_total{reason}` 是否只覆盖该被拒的客户端 |
| `enforce` | 校验失败即拒绝：WS 回 HTTP 401、TCP 回 `error 401` 并断开 | 正式生效 |

token 由认证中心用**同一个密钥**签发，格式不引入 JWT 依赖：

```text
v1.<过期时间 epoch 秒>.<base64url(HMAC-SHA256(secret, "v1|userId|过期秒"))>
```

签名材料里带 `userId` 是为了**防冒用**：只签 token 不签身份的话，任何人拿自己的 token 改一下
`userId` 参数就能冒充别人。本地签发：

```bash
java -cp im-gateway/target/classes com.github.im.gateway.auth.HmacTokenAuthenticator <secret> <userId> [ttlSeconds=3600]
```

拒绝原因（同时是指标 tag，固定小枚举、不含 token 内容）：`missing_token` / `malformed_token` /
`expired_token` / `bad_signature` / `missing_user`。

配置错误炸在启动时：`mode=warn|enforce` 却没配 `im.auth.secret` 直接启动失败——避免出现
「以为开了鉴权、其实一直在放行」这种最危险的状态。换 JWT 或接认证中心时实现
`SessionAuthenticator` 即可，传输层不用动。

**已验证**（真实网关进程 + 真实客户端）：无 token → 401、`token=garbage` → 401、
有效 token → OPEN、**把 alice 的 token 拿去当 bob 用 → 401**（最关键的一条）。

### 关系链与投递前准入（ACL）

鉴权解决「你是谁」，准入解决「你能不能发这一条」。没有后者的后果很具体：
单聊会话只是两个 userId 排序拼接（`s:{a}:{b}`），**任何人都能给任何人发消息**；
群 ID 往往可枚举（`g:1`、`g:sales`），不校验等于任何人都能往任何群发言。
这类问题必须在**投递之前**拦——事后过滤等于骚扰信息的第一屏已经显示出来了。

三档开关（与 `im.auth.mode` 同一套风格）：

| `im.acl.mode` | 行为 | 用途 |
|---|---|---|
| `off`（默认） | 不校验，**零额外往返** | 本地联调、纯内网 |
| `warn` | 校验并记日志，但一律放行 | 上线前观察「本该拦掉多少」 |
| `enforce` | 校验失败即 403（`error` 帧，客户端不该重试） | 正式生效 |

规则：

| 场景 | 判定 | 说明 |
|---|---|---|
| 群聊 | 发送者必须是群成员（`SISMEMBER` / 唯一键存在性，O(1)） | 每条群消息一次，不能走全量 `SMEMBERS` |
| 单聊 · 黑名单 | **任一方向**拉黑都拒绝 | 拉黑只存「我拉黑了谁」，判定必须查两次 |
| 单聊 · 好友 | 可选（`im.acl.require-friend`） | 开放社交不要求；企业 IM 通常由组织架构同步关系 |
| 发给自己 | 始终允许 | 文件传输助手这类场景 |

三个设计点：

- **校验位置在「幂等 + 发号」之前**：被拒的消息不占 seq、不进幂等表、不进离线队列。
  否则骚扰消息虽然没投出去，也已经消耗了序号，还给对账留下一堆孤儿记录。
- **黑名单不通知被拉黑方**：`isBlocked(this, other)` 只能由 `this` 自己发起。
  让被拉黑方能查到「你被谁拉黑了」，本身就是信息泄露。
- **默认 off 是刻意的**：开启后每条单聊消息多 2~4 次 Redis 往返、每条群消息多 1 次。
  这个开销应该由部署方在知道自己在为什么付费的前提下打开；`warn` 档就是为此准备的过渡态。

存储与接口：

```
Redis（默认）   im:rel:friend:{userId}  Set，好友（双向两份，O(1) 点查）
                im:rel:block:{userId}   Set，黑名单（单向一份）
MySQL          im_relation (owner_id, target_id, relation_type) 唯一键；好友双向两行、黑名单单向一行

GET    /relation/{userId}/friends              好友列表
POST   /relation/{userId}/friends              ["bob"]   建立双向好友（幂等）
DELETE /relation/{userId}/friends/{friendId}             解除好友（双向）
GET    /relation/{userId}/blocks               黑名单
POST   /relation/{userId}/blocks               ["spammer"]   拉黑
DELETE /relation/{userId}/blocks/{blockedId}              取消拉黑
```

> 不做「好友申请 → 通知 → 同意」的两阶段流程：它需要通知子系统（红点、未读、跨端已处理状态），
> 属于业务侧。这些接口的定位是**由接入方的好友/组织架构系统同步过来**，IM 侧只消费「关系成立」这个事实。
> 也仍没有群角色权限（群主/管理员/禁言）——那是群体系的一部分，见「已知限制」。

### 链路追踪（traceId）

`msg.send` 可以带 `traceId`，不带则网关生成一个 32 位十六进制 ID。同一个 ID 贯穿：

```
网关日志 → 上行 SubmitMessageRequest.trace_id → IM 受理日志
        → 下行 PushTask.trace_id → 网关推送日志 → msg.ack 回显（客户端可记进自己的日志）
```

以前这条链路上塞的是 `serverMsgId`（消息标识），既搜不到请求也串不起链路，等于「看着有追踪、实际查不出东西」。
真实受理日志长这样：

```text
message accepted: traceId=trace-e2e-0001 conv=s:alice:bob seq=13 msgId=01a166bb-... from=alice to=bob offline=1
```

### 多端在线（同一账号多设备）

设备身份取 `deviceId`（WS query / TCP connect 帧），语义三条：

| 场景 | 行为 |
|---|---|
| 同一 `deviceId` 再建连 | 视为该设备重连 → 踢掉旧连接（避免消息推到僵尸连接） |
| 不同 `deviceId` | 并存；一条消息推给该用户的**每一个**设备 |
| 不带 `deviceId` | 落 `default` 槽位，等价于旧的「单用户单连接」 |

链路上四处同时配合才成立：

1. **本机会话表**：`ConnectionManager` 变成 `userId → deviceId → 会话`，`send(userId, frame)` 扇出到
   所有设备。部分设备写失败只要有一台成功就算投递成功——其余设备重连后按 seq 拉齐；
2. **在线路由**：`im:gateway:user:{userId}` 从 String 变成 **SET**：手机连网关 A、网页连网关 C 时集合里
   有两个实例，IM 服务逐个实例推，各实例再分给本机设备；
3. **failed 语义**：`failed = 目标 − 已投递`。不能直接汇总各实例返回的 failed，否则
   「手机推到、网页所在实例失败」会让一个**已经收到消息的人**被再排一份离线。
4. **设备登记表**：`im:gateway:devices:{userId}`（SET，网关建连时写，TTL 30 天，**断开不摘**）。
   它是补发侧判断「是不是所有端都补过某个位置」的唯一依据——没有它就只能二选一：
   要么误删共享锚点（另一台设备永久缺一段），要么让锚点留到 TTL 过期（每轮补发白查历史）。

#### 补发侧的状态划分：共享只留下界，进度按端分开

「用户级状态 + 多设备」这类问题的通解是：**能被多端共享的只有下界，进度必须按端分开**。
本项目的落地：

| 状态 | 粒度 | 为什么 |
|---|---|---|
| `im:offline:q:{userId}`（单聊帧队列） | 用户级一份 | 帧本身与设备无关，靠 ZSet 的入队序号定位 |
| `im:offline:cur:{userId}:{deviceId}` | **设备级** | 这台设备补到哪个位置 |
| `im:offline:ptr:{userId}`（群聊锚点） | 用户级一份 | 「最早的未补发位置」，只给新设备兜底，防止从 seq 1 拉整个历史 |
| `im:offline:gcur:{userId}:{deviceId}` | **设备级** | 这台设备在每个会话补到哪 |
| `im:wm:{userId}:{deviceId}` | **设备级** | 客户端亲口确认收到哪 |

群聊补发起点 = `max(锚点, 本设备游标 + 1, 客户端水位 + 1)`。锚点只有在
「所有已登记设备都已补过这个位置」时才退休——否则删掉它，另一台设备要么永久缺这段，
要么从 seq 1 重拉整个会话历史。

**补发的帧只推给发起请求的那条连接**（`ConnectionManager.sendTo`，不是 `send`）：
每台设备有自己的游标、会各自拉一遍；如果用扇出，手机上线会把同一批离线消息同时砸给网页，
重复一遍，还会让两端的未读数一起跳。

### 会话列表与未读数

在此之前「会话」只是一个字符串约定（`s:a:b` / `g:{id}`），没有实体，所以做不了会话列表、
未读数、最后一条消息摘要——它们都需要一个地方回答「这个会话现在到哪了」。

| 数据结构 | 内容 |
|---|---|
| `im:conv:meta:{convId}` (Hash) | `type` / `last_seq` / `last_time` / `last_from` / `last_digest` / `last_msg_id` |
| `im:user:convs:{userId}` (ZSet) | `member = convId`，`score = 最后消息时间`（会话列表 + 排序） |
| `im:user:read:{userId}` (Hash) | `convId -> 已读到的 seq`（**用户级**） |

**未读数是算出来的，不是存出来的**：`unread = last_seq - read_seq`。

存计数（每条消息给每个成员 `INCR`）在群聊里是「消息数 × 成员数」的写放大，而且**一次写失败就永久和真实值不一致**；
两个游标相减天然自愈——任何一端漏写，下次读到的都是正确的差值。代价是拉列表时每个会话要读一次已读位置，
所以一页共用一次 pipeline。

**已读是用户级，同步水位是设备级**——这两个概念最容易被混为一谈：

| | 粒度 | 回答的问题 |
|---|---|---|
| `im:user:read:{userId}` | **用户级** | 人看到哪了（一个人只有一个答案） |
| `im:wm:{userId}:{deviceId}` | 设备级 | 这条数据同步到哪个端了（传输语义） |

所以多端已读合并在这一层是**免费的**：手机读完，网页的未读角标下次拉列表就是 0，
不需要「N 个设备各报一个已读、读取时取 max」，也不用处理「A 端读到 10、B 端读到 20、C 端只读到 5」的偏序。
反过来，如果按设备存已读，你会被迫在读取路径上做这套合并。

**新会话的已读基线**：用户第一次出现在某个会话的列表里时，已读位置初始化为**当时的** `lastSeq`。
否则新人进一个十万条历史的群，未读数一上来就是 100000，客户端为了清零还得把整个历史标成已读。
这个初始化和「加入会话列表」必须在同一个 Lua 里完成（两个 key 共用 `{userId}` hash tag 保证同 slot），
否则会出现「ZSet 进去了、基线没设」的窗口，那一瞬间的未读数就是全量历史。

**两个只前进**（都是为了兜住乱序与并发）：
- `last_seq` 用 `HGET` 先比较再写：两条消息并发 submit 时，后分配的 seq 可能先落库，
  无条件覆盖会让会话列表的最后一条消息闪回旧内容，未读数还会算成负数；
- 已读位置 `HSET` 前比较，并且 **cap 到 `lastSeq`**：客户端可能上报未来的 seq（本地乐观计数），
  不 cap 未读数会变负。

**大群的写扩散降级**：一条消息要把所有成员的 `im:user:convs` 都推一遍，10 万人群就是 10 万次 ZADD。
超过 `im.conversation.fanout-limit`（默认 1000）人时**直接跳过写扩散**，只更新元信息，
成员的会话列表靠 `im:group:user:{userId}` 反查在**读时**补齐。所以群成员存储多了个反向索引
（Redis 版多维护一个 Set，JDBC 版直接走 `idx_group_member_user` 索引）。

**会话标记（置顶 / 免打扰）**：`im:user:convflags:{userId}`（Hash，`conversationId -> 位图`），
第 0 位置顶、第 1 位免打扰。

- **用位而不是两个字段**：这两项几乎总是一起读（会话列表一页几十行）、一起改（设置页一次提交），
  一个 HGET 就够了；
- **只改传了的那一项**：`pinned`/`muted` 是 `Boolean`，缺省 = 这一项不动。
  否则「设为置顶」会顺手把免打扰清掉，用户会以为自己在设置页丢了配置；
- **免打扰不影响未读数**：未读该算还得算，只是客户端不响铃、不打角标。
  把「免打扰」实现成「不计未读」是同一个词在两种语义间打架，最后一定会算错；
- 值得注意的语义边界：置顶只影响**服务端返回的顺序**（置顶优先，其次按最后消息时间倒序），
  不改变写入与补发的任何行为。

**接口**：

```
GET  /conv?userId=alice&limit=50                        会话列表，置顶优先，其次按最后消息时间倒序，带 unread/pinned/muted
POST /conv/{conversationId}/read?userId=alice&seq=100    上报已读（只前进，服务端 cap 到 lastSeq）
POST /conv/{conversationId}/flag?userId=alice&pinned=true            置顶（不改免打扰）
POST /conv/{conversationId}/flag?userId=alice&muted=false            取消免打扰（不改置顶）
```

> 仍然没做的：单聊会话的对方昵称/头像（需要用户资料子系统）、群公告/群名（需要群实体）、
> 会话删除/归档。会话列表的骨架（谁有哪些会话、未读多少、最后一句说了什么、置顶与免打扰）已经具备。

### 存储分片与 key 空间（appId / conversationId）

分片维度先于分片实现落地：**即使当前只有 1 个分片、1 个租户，两条维度也已经是 key 与路由的一等公民**。
理由很实际——分片这件事一旦要加，最先出错的永远是「同一份数据在两处被算到不同分片」，
而那种实现几乎总是各模块自己写 `Math.abs(id.hashCode()) % n`。

**每个 key 只带两个 hash tag**（`RedisKeys` 是唯一构造点）：

| 维度 | tag | key 形态 |
|---|---|---|
| 会话 | `{conversationId}` | `im:conv:{c}:meta` / `:msgs` / `:seq` / `:recalled` / `:idem:{from}\|{msgId}` |
| 用户 | `{userId}` | `im:user:{u}:convs` / `:read` / `:flags` / `:wm:{device}` / `:offline:*` / `:friends` … |
| 群 | `{groupId}` | `im:group:{g}:members` |

**这次改造修掉了一个真实缺陷**：改造前 `im:seq:{c}` 与 `im:idem:{from}|{msgId}` 都没有 tag，
而 `SeqAllocator` 的「幂等查询 + 发号」合并脚本同时访问这两个 key —— 在 Redis Cluster 上
会直接 `CROSSSLOT` 失败（单机 Redis 无感，所以一直没暴露）。现在幂等键挂在会话维度上
（`im:conv:{c}:idem:...`），与 seq 计数器天然同 slot。

同 slot 带来的结构性收益：`meta + msgs + seq + recalled` 现在可以放进**同一个 Lua**，
将来把「受理一条消息」（取号 → 追加 → 更新 meta）合并成一次往返，而不用改 key 设计。

**租户维度**用进程级根前缀（`im.tenant.app-id`，默认空 → `im:`）：

- 为什么不是每条请求带 appId：同一个 Redis 里用前缀隔离租户是"伪隔离"（备份、迁移、慢查询、
  内存水位全分不开），真多租户按实例或按库隔离；
- 为什么不走 ThreadLocal 上下文：项目里的推送是虚拟线程池，上下文在那里会静默丢失，
  那类 bug 的排查成本远高于收益。

**MySQL 侧**：`conversationId` 已经是 `im_message` 唯一键与主查询的首列，
物理切分用原生 `PARTITION BY HASH(conversation_id)`，脚本与前置条件（唯一键必须含分区键）
写在 `db/schema-shard.sql`，默认不启用——单机部署没有收益，而分区对主键有额外约束。

`ShardRouter` 是这套规则的唯一实现：`shardOf(conversationId)` 用 CRC32（与 `zlib.crc32` 一致，
离线脚本能算出同一个分片）、`messageTableOf` 在单分片时退化回 `im_message`。它的 debug 日志
同时打出 `mysql=` 与 `redis=`，用于对照两侧是否按同一维度切分。

> 迁移注意：key 名变了（`im:conv:meta:x` → `im:conv:{x}:meta`），旧数据不会自动读到。
> 本项目是自研单租户部署，直接清空 Redis 即可；如果要平滑迁移，需要对旧前缀做一次
> rename/双读，脚本成本高于收益。

### 离线通知（厂商推送）

长连接只在 App 前台、进程活着、网络可用时有效。用户真正收不到消息的场景恰好是**连接不在**：
进程被杀、后台被系统冻结、手机休眠。这时唯一的通道是手机厂商的推送服务——它由系统进程常驻
接管，能唤醒 App 或至少显示一条通知。

所以两条通道的触发条件是互补的：`PushPort`（长连接下发）返回的 `failed_user_ids`，
正是厂商推送的目标用户。

```text
MessageRouter.deliver → failed(userIds) → ① 落离线队列（权威通道）
                                        → ② VendorPushPort（提醒通道，尽力而为）
```

**边界为什么放在「中转」**：APNs 是 HTTP/2 + 每次签发的 JWT，FCM 是 OAuth2 服务账号，
华为/小米/OPPO/vivo 各有一套 AppKey 签名与字段命名。把这四套塞进消息服务只有一种结果——
消息服务的发布节奏被推送 SDK 的升级绑死，且任何一家的连接抖动都能影响投递线程池。
IM 服务只声明「推给谁、推什么」（`VendorPushRequest`：userId / title / body /
conversationId / seq / contentType），怎么推由 `VendorPushPort` 的实现承担。

| 配置 | 默认 | 说明 |
|---|---|---|
| `im.vendor-push.enabled` | `false` | 关闭时**不装配任何实现**，启动日志里不会出现连不上的告警 |
| `im.vendor-push.endpoint` | 空 | 中转服务的接收地址 |
| `im.vendor-push.token` | 空 | `Authorization: Bearer <token>` |
| `im.vendor-push.timeout` | `3s` | 连接与请求超时 |

两条内部护栏（都不需要配置）：

- **人数上限 200**：一个 500 人群九成不在线时，逐人推送等于自己给自己限流，
  还会让真实用户的通知排在后面；
- **正文截断 80 字符**：各家厂商对 payload 都有硬限制，通知正文只需要让用户判断「要不要点开」。

失败**不重试**：通知丢了，用户下次打开 App 照样能从离线队列拿到消息（那才是权威通道）。
为一条通知维护重试队列，是拿可靠性去换一件非权威的事。

设备数上限 `im.gateway.max-devices-per-user`（默认 5）：超限时**拒绝新连接**而不是踢掉老设备，
避免一个客户端 bug 反复重连把用户其它设备挤下线。

**已验证**（网关 + IM 服务 + 三个真实 WS 客户端）：alice 的 phone 与 web 同时在线，bob 发一条消息 →
两个设备都收到同一条 `seq=16` 的 `msg.push`；phone 重连后 `/admin/status` 的 `connections`
从 3 变 2（旧连接被踢且已移出会话表），全部断开后归 0（无空壳泄漏）。

`sync` 帧是客户端**重连后必须发一次**的对账动作：它把「每个会话我见到了哪个 seq」报给服务端。
服务端用它做两件事——补拉缺口、抬高 seq 发号下界。后者是 Redis 序号计数器整体丢失时
唯一还能防止「新消息 seq 比客户端水位还小、被当历史去重丢弃」的依据（详见「seq 发号与三道下界」）。

### 心跳保活

每条连接在 pipeline 里挂 `IdleStateHandler` + `HeartbeatHandler`，由连接自己的 event loop 计时，
**没有全局扫描任务**：

1. `ping-after-ms`（默认 45s）内没有任何上行 → 下发应用层 `{"type":"ping"}`，
   同时挂一个 `close-after-ms - ping-after-ms`（默认 45s）后的关闭任务；
2. 期间收到**任意**上行帧（`pong`、`msg.send` 都算）→ 取消关闭任务；
3. 到期仍无上行 → 判定死连接并断开（WS 1001 / TCP 断链）→ 触发 `channelInactive`
   → 清本机会话 + Redis 路由 → 用户转离线、消息落离线队列。

清理的是「进程被杀 / NAT 超时 / 拔网线」这类发不出 FIN 的半开连接，避免「显示在线但收不到消息」。
用应用层 JSON ping 而不是 WebSocket 协议级 Ping：浏览器 JS 发不了协议级 ping，TCP 更没有，
统一在业务帧层才能让两种传输的心跳语义一致。未认证连接不发 ping，第二次空闲直接断。

### 优雅摘流

缩容 / 发版必须走（否则客户端集体硬断、触发重连风暴）：

1. 注销注册中心（Nacos `deregisterInstance`），新流量不再进来；
2. 就绪探针翻 `REFUSING_TRAFFIC`，新握手直接 503；
3. 向存量连接广播 `session.migrate`（带 `retryAfterMs`，**抖动由客户端加**）；
4. 等待 `im.gateway.drain.timeout-ms` 后强制关闭残余连接。

进程收到 `SIGTERM` 时走同一套流程（`ContextClosedEvent`），
K8s 的 `terminationGracePeriodSeconds` 必须大于 `timeout-ms`。

---

## 存储层

### 消息存储端口

业务只依赖 `message.server.com.github.im.MessageRepository`：

```java
void append(StoredMessage message);
// 向上翻页：seq < beforeSeq 的至多 limit 条，升序返回；beforeSeq <= 0 表示第一页（最新）
List<StoredMessage> before(String conversationId, long beforeSeq, int limit);
// 离线补发：seq > afterSeq 的升序区间
List<StoredMessage> since(String conversationId, long afterSeq, int limit);
long maxSeq(String conversationId);
```

落库的是**结构化记录**（不是已序列化好的下行帧），帧在读取时按需重建。三种实现：

| 实现 | 位置 | 介质 | 适用 |
|------|------|------|------|
| `RedisMessageRepository` | `im-storage-redis`（默认） | `im:conv:msgs:{conversationId}` List，JSON | 本地开发 / 联调，零外部数据库 |
| `JdbcMessageRepository` | `im-storage-jdbc` | MySQL `im_message` | 生产形态，支持按会话 + 序号漫游 |
| `MongoMessageRepository` | `im-storage-mongodb` | MongoDB `im_message`，实体 `MessageEntity`，`_id = serverMsgId` | 文档形态，身份用 UUIDv7，`(conversationId, seq)` 唯一索引兜住保序与去重 |

切换只需配置（`im.storage=jdbc` 时才装配连接池、`=mongodb` 时才建 Mongo 客户端，默认形态完全不碰外部数据库）：

```yaml
im:
  storage: jdbc            # redis（默认）| jdbc | mongodb
  db:
    # rewriteBatchedStatements=true：驱动把批量插入压成一条多值 INSERT，省 round trip
    url: jdbc:mysql://127.0.0.1:3306/im-cluster?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true
    username: root
    password: root
    pool-size: 20
    init-schema: true      # 首次启动执行 db/schema.sql；生产建议关掉交给 Flyway
    batch-enabled: true    # group commit：并发到达的插入合并成一个事务
  # storage: mongodb 时改用下面这段（启动先 ping 一次，配错在启动阶段就报）
  mongo:
    uri: mongodb://127.0.0.1:27017/?connectTimeoutMS=3000&serverSelectionTimeoutMS=5000
    database: im
    init-indexes: true     # 建三条唯一索引（保序 / 幂等 / 消息 ID）+ 各反查索引，幂等
```

`im.storage` 只决定**持久化介质**：seq 发号、幂等表、离线队列、在线路由仍在 Redis
（`jdbc` / `mongodb` 两种形态都同样要连 Redis，只是消息与群/关系链落到外部数据库）。

启动时除了建索引，还会**清掉本模块自己废弃的索引**（`createIndex` 只增不减，删注解不会删索引）。
典型事故：`serverMsgId` 从普通字段改成文档 `_id` 后，遗留的 `uk_msg_server_id` 约束着一个永远缺失的字段，
MongoDB 把缺失当 null → 第一条消息占掉那个槽位，**从第二条起全部** `dup key: { serverMsgId: null }`。

MongoDB 形态用**实体映射**：`entity/` 下的 5 个 `@Document` 类持有字段与索引声明
（`@MongoId` 决定 `_id`、`@CompoundIndex` / `@Indexed` 决定索引），`MongoSchema` 用 Spring Data 的
`MongoPersistentEntityIndexResolver` 解析注解后建索引——字段名与索引各只声明一次。
落库约定与 MySQL 形态一一对应：

| 集合 | 文档 | 关键索引 |
|------|------|----------|
| `im_message` | 一条消息，`_id = serverMsgId`（UUIDv7，消息身份） | `uk_msg_conv_seq` / `uk_msg_idem`（均唯一） |
| `im_message_delete` | 用户级本地删除，`_id = userId:conversationId:seq` | `idx_msg_delete_user` / `idx_msg_delete_conv` |
| `im_group` / `im_group_member` / `im_relation` | 群资料 / 群成员（含角色与禁言）/ 有向关系边 | `idx_group_owner` / `uk_group_member`+`idx_group_member_user` / `uk_relation`+`idx_relation_target` |

### 写入吞吐：group commit

单条 `INSERT` 自动提交 = **每条消息一次 fsync**。`innodb_flush_log_at_trx_commit=1` 下一次 fsync
约 0.3~3ms，于是单个 MySQL 的写入天花板被压在**几百到三千 QPS，且与连接池大小无关**——
多开连接只是让更多事务争抢同一个 redo 刷盘。

`JdbcBatchAppender` 把「几乎同时到达的插入」合并进一个事务（最多 64 条），fsync 摊薄成 1/N。
关键是它**没有固定攒批窗口**：只在「队列里已经有别的消息」时才合批，所以
**稀疏写入的延迟与单条提交完全一样**，高负载时自然合并成大事务（Nagle 算法的同一个思路）。

| 场景 | 行为 |
|---|---|
| 单条到达 | 后台线程立刻取走并提交（0 额外延迟） |
| 并发到达 | 一次 `batchUpdate` 提交最多 64 条 |
| 整批失败 | 整批异常抛回调用方 → 客户端重试 → 命中 Redis 幂等表直接回放（一条坏消息会拖累同批，但只是坏路径上的重试） |
| 队列积压 / 已关闭 | 就地同步写，绝不丢消息 |

再配合两个参数收益才完整：

- 连接串 `rewriteBatchedStatements=true`：驱动把 N 条 INSERT 压成一条多值语句；
- MySQL `innodb_flush_log_at_trx_commit=2`：用「崩溃可能丢 1s 事务」换掉大部分 fsync。
  IM 场景通常可接受（幂等表 + 客户端重试兜底），但属于持久性取舍，要按业务容忍度决定。

### MySQL 表结构

`im-storage-jdbc/src/main/resources/db/schema.sql`：

```sql
CREATE TABLE im_message (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  conversation_id VARCHAR(160) NOT NULL,      -- 单聊 s:alice:bob / 群聊 g:{groupId}
  seq BIGINT NOT NULL,                -- 会话内单调序号，保序与漫游的主索引
  server_msg_id VARCHAR(64) NOT NULL,           -- UUIDv7，全局唯一，与 seq 解耦
  from_user VARCHAR(64) NOT NULL,
  to_user VARCHAR(160) NOT NULL,
  conv_type VARCHAR(16) NOT NULL,
  client_msg_id VARCHAR(96),
  body MEDIUMTEXT,
  created_at BIGINT NOT NULL,
  UNIQUE KEY uk_msg_idem (from_user, client_msg_id),   -- 幂等最终防线
  UNIQUE KEY uk_msg_conv_seq (conversation_id, seq),   -- 保序 / 漫游主索引
  UNIQUE KEY uk_msg_server_id (server_msg_id)          -- 消息 ID 全局唯一
);

CREATE TABLE im_group_member (
  group_id VARCHAR(160) NOT NULL,
  user_id VARCHAR(64) NOT NULL,
  role VARCHAR(16) NOT NULL,          -- OWNER / ADMIN / MEMBER
  created_at BIGINT NOT NULL,
  UNIQUE KEY uk_group_member (group_id, user_id),      -- 加成员幂等
  KEY idx_group_member_user (user_id)                  -- 反查「某用户加入了哪些群」
);
```

两个唯一索引是设计意图，不是顺手加的：

- `uk_msg_idem`：Redis 幂等表可能过期/丢失，数据库唯一索引是**幂等最终防线**。
  真冲突时让 `append` 抛异常、受理失败返回可重试错误，而不是写进乱序数据；
- `uk_msg_conv_seq`：即使 Redis 丢了 seq 计数，也不可能出现「同一会话同序号两条消息」。

### 离线消息

离线判定与存储都在 **IM 服务**侧（不是网关职责）。存储分两条通道，区别只在「放大倍数」：

| 场景 | key | 结构 | 为什么 |
|------|-----|------|--------|
| 单聊 | `im:offline:q:{userId}` | ZSet（score = 入队序号，member = 帧） | 一条消息只有一个收件人，存一份完整帧就是最省的形态 |
| 群聊 | `im:offline:ptr:{userId}` | Hash `conversationId -> 待补发 seq` | 群消息收件人是全体成员，按人存帧会「一条消息 = 不在线人数份」 |

> 旧的单聊 List 队列 `im:offline:{userId}` 已废弃，升级窗口内仍会读一次把积压帧发掉（读完即删）。

**群聊为什么不存帧**：500 人群、450 人不在线、50 人各发 5 条 = 250 条消息 × 450 人 =
112,500 份相同帧（~30MB），并且随消息数线性增长；每条群消息写入都是 450 次 RPUSH。
改成指针后，每个「用户 × 会话」只占一个 field（几十字节），后续消息只把同一个 field 往前推，
**占用不随消息数增长**——你那个场景从 112,500 条降到 450 个 field。

写路径都用 `HSETNX` 语义：字段已存在就保留**更小的**那个 seq（最早的未补发位置），
否则中间那段会永远补不回来。

#### 补发是「至少一次」，不是「读即消费」

这是整条离线链路最容易出错的地方。**旧实现是 at-most-once**：单聊 `LRANGE + LTRIM` 在一个 Lua 里
把帧原子移走，网关拿到之后才逐帧写 socket，一帧写失败就 `break` —— **已经移走但没发出的帧永远消失**，
而单聊没有「从历史重建」的兜底路径；群聊更直接，指针在**读的时候**就推进了。

现在的顺序是 **读 → 写进连接 → 确认 → 下次从确认处继续**：

1. `pullPending` **只读不删、不推进任何游标**，同时返回两个确认位置：
   `ack_single_seq`（单聊本批覆盖到的位置）和 `ack_group_seqs`（群聊每会话补到哪）；
2. 网关把帧全部写进客户端连接；
3. **全部成功才**调 `MessageIngress.ackPending` → 服务端推进游标；
4. 中途失败 / 进程崩溃 = 不确认 = 游标不动，下次重连从原位置重读（客户端按 seq 去重）。

**为什么单聊队列从 List 换成 ZSet**：补发起点是「这台设备已确认到的位置」，需要能按位置定位读
（`ZRANGEBYSCORE key (cursor +inf LIMIT n`）。List 只能按下标读，而 `LTRIM` 一裁下标就整体漂移。
用 score 索引还顺带修了旧 List 的另一个问题：补发顺序是「入队（到达）顺序」，本来可能乱序。

> **score 不是消息的 seq**。seq 是**会话内**单调的，同一用户在不同会话里会出现相同的 seq
> （`s:a:b` 的 5 和 `s:a:c` 的 5），拿它当 score 会让「游标推进到 5」直接吃掉另一个会话的 5。
> 所以 score 用 per-user 的入队序号（`im:offline:q:{userId}` 旁边的 `:pos` 计数器，
> 计数器丢了会从队列最大位置续上，不会从 1 重来导致顺序错乱）。

**游标按设备隔离**：单聊 `im:offline:cur:{userId}:{deviceId}`、群聊
`im:offline:gcur:{userId}:{deviceId}`。手机和网页各自补到哪互不相干——用户级单一游标的后果是
「先上线的设备一确认，另一个设备就永远补不到那段」。
删数据仍只按「总条数超上限」裁，不按游标裁（按游标裁会让先确认的设备删掉另一个设备还没读的消息）。
多端同步的整体设计见「多端在线」小节的「共享只留下界，进度按端分开」。

#### 群聊补发的起点与上限

1. 按指针逐会话补拉：起点 = `max(指针, 客户端上报水位 + 1)`，从会话历史取
   `seq > 起点` 的消息、用 `ClientFrames.frameOf()` 重建帧（与实时推送**同构**）；
2. 单轮最多 20 个会话、单次最多 200 帧，`remaining` 告诉客户端还有没有
   （单聊按**未确认**帧数计、群聊按会话数计）；
3. 只有「确实一条历史都查不到」时才清掉指针——那是唯一可以安全删除的时机。

四条硬约定：

- **拉取不推进游标**：推进只发生在 `ackPending` 里，即帧真的写进连接之后；
- **历史查询失败绝不推进指针**：宁可下次重复补（客户端按 seq 去重），也不能把指针推过去——那等于永久丢一段消息；
- **游标只前进不回退**：并发 pull（多端同时上线）读到同一个旧游标只会重复补发，回退会造成死循环；
- **两条通道都有 TTL**：帧队列另有长度上限（`im.offline.max-frames`，默认 500）。
  长期不上线的用户不该让队列无界增长；超期未上线则放弃补发，历史仍在消息存储里，客户端可主动拉历史。

#### 客户端水位：delivered 的唯一可信口径

网关 `writeAndFlush` 成功只代表「进了 socket 缓冲区」，客户端可能早就断网了；只有客户端自己
通过 `sync` 帧上报的水位才算「真的收到了」。这张表按 `im:wm:{userId}:{deviceId}` 落 Redis
（Hash `conversationId -> seq`，只前进，默认 30 天 TTL），有两个用途：

1. **delivered / 已读的对账数据**：排查「用户说没收到消息」时，这是唯一能证明「端侧收到了哪一条」的记录；
2. **seq 发号器的第三道下界**：进程内那份副本（`ClientWatermarkStore` 的内存 map）在
   「消息存储与计数器一起丢」时仍然有效，因为客户端水位是唯一不依赖 Redis 的证据。

> 注意粒度是「会话级最大 seq」，不是逐条 ACK。逐条确认会把 RPC 次数放大到帧数级别，
> 而 IM 的既有约定是「客户端按 seq 去重 + 补洞」，会话级水位已经够用——主流实现也是这么做的。

### 群聊与群成员

`GroupMemberRepository` 同样双实现：Redis Set `im:group:members:{groupId}`（默认）或 MySQL `im_group_member`。
群聊扇出 = 群成员列表去掉发送者；成员列表为空时**当成无人可投**，绝不退化成「投给群 ID」
（那会往离线队列塞进永远不被拉取的数据）。

除了正查（群 → 成员）还有**反查**（用户 → 群，`groupsOf`）：Redis 形态多维护一个
`im:group:user:{userId}` Set（写入/移除成员时两边一起改），JDBC 形态直接走 `idx_group_member_user` 索引。
反查的用途是**大群会话列表的补齐**——群消息的会话列表是写扩散的，超过 `im.conversation.fanout-limit`
会跳过写扩散，成员的列表只能靠反查在读时补出来。少了它，大群成员「明明在群里却看不到会话」。
退群时两边必须一起摘，否则退群的人会一直看到那个群。

**扇出成本与两步优化**。一条群消息 =「按人查在线路由」+「按实例 gRPC 下发」。5000 人群若逐个
`SMEMBERS` 查路由，就是 5000 次串行往返（1~2.5s），这是群消息延迟的全部来源：

1. **批量路由**：`instancesOfAll()` 把 key 切成 500 一片、并发 `executePipelined`，一片一次往返，
   顺带把不在线的人筛成空集合，只为在线成员构造下发。切片而不是一次发 5000 条，是因为
   Redis 单线程，大 pipeline 会把库顶住；Cluster 下跨 slot 会抛 `CROSSSLOT`，
   此时自动退化为逐条查询——慢，但不会让整条群消息全落离线；
2. **跨实例并行**：每个网关实例一次 gRPC，先全部提交再统一收集，总耗时 ≈ 最慢的那个实例；
3. **失败转离线同样批量**：群聊走 `recordGroupPending()`（每人一个 `HSETNX + PEXPIRE`），
   单聊走 `storeAll()`（`RPUSH + LTRIM + PEXPIRE`），都是每 500 人一次 pipeline。
   这段代码跑在投递许可里，逐条写会让「5000 人全离线」白占额度一整秒。

本机（Redis 在 localhost，RTT ≈ 0.07ms）5000 用户实测：

| 操作 | 逐个 | 批量 | 倍数 |
|---|---|---|---|
| 查路由 `SMEMBERS` | 596ms | 131ms | 4.5x |
| 落离线（每人 1 条写命令） | 370ms | 61ms | 6.1x |

批量收益与具体命令无关：两种离线写法都是「每人 1 条写命令」，收益全在往返次数从 N 降到 N/500。
跨机房（RTT 0.3ms 量级）时逐个查会退化到 1.5s 以上，批量仍在百毫秒级——
**换成网络 Redis 后收益比本机数字大得多**，本机测出来的 4.5x 是保守下限。

配合「ack 先行」，5000 人群的一条消息：**ack 与群规模无关**（只包含落库 + 幂等写），
扇出耗时主要是批量路由（百毫秒级）+ 各实例 gRPC 往返（并行，≈ 最慢实例）。
端到端用 20 人群验证过 19/20 到达（发送者按设计不回推）。

> 再往上（万人群、跨可用区），单靠这两步不够，需要「大群降级」：只推在线用户 + 其余人收一条
> 有新消息的通知，客户端主动拉取（写扩散 → 读写混合）。

> 群成员落到 MySQL 才能支持「查某用户加入了哪些群」「按角色做权限」，Redis Set 做不到。

---

## 服务间通信

### 契约层（`im-proto`）

两个服务唯一的共享物，包含四类内容：

| 内容 | 作用 |
|------|------|
| `im_ingress.proto`：`MessageIngress.SubmitMessage / BatchSubmit / PullPending` | 上行 |
| `im_push.proto`：`DownstreamPush.Push(PushTask) → PushResult` | 下行 |
| `MessageIngress` / `PushPort` 端口接口 + `MessageEnvelope` | 业务与传输解耦 |
| `RedisKeys` | key 空间 + **写方/读方归属**，数据一致性的落点 |

### 上行与下行

- **上行**：网关解帧 → `MessageIngress.submit()`；实现可换（本地直调 / gRPC 远程），业务代码不变；
- **下行**：IM 服务只调 `PushPort.push(task)`，`frame` 已是完整报文，网关**原样透传**；
- **在线路由**：`im:gateway:user:{userId}`（SET 集合，多端时含多个实例）只由网关写（TTL 30s + 10s 续期），IM 服务只读。

### 上行通道三档（按部署形态切换）

`im.ingress.transport` 决定网关如何把消息交给 IM 服务：

| 取值 | 上行路径 | 使用场景 |
|------|----------|----------|
| `local`（默认） | 本地直调 `MessageService` | 单体装配，零网络开销 |
| `shadow` | 本地为准 + 远程异步双跑比对 | 切换前验证两条通道等价性，`GET /shadow/stats` 看 `matchRate` |
| `grpc` | `RemoteMessageIngress` → 独立部署的 IM 服务 | 拆分部署 |

Shadow 判定口径：`matchRate` 稳定 1.0、`shadowErrors` 不增长才可切 `grpc`。
两条路径共享同一 Redis，shadow 侧必然命中幂等回放（`duplicate=true`），这是**预期**；
真正的落库差异需要「双套独立 Redis/DB 的 Shadow 环境」才能验证。`pullPending` 不做双跑（读即消费）。

### 服务发现

职责严格分离，两者不重叠：

- **Redis 负责**：`userId → 连在哪个网关实例`（per-user 会话状态，秒级最终一致）；
- **Nacos（可选）负责**：`实例 ID → 实时可达 ip:port`，带健康探针、自动剔除死节点。

```
IM 服务 → Redis 查 userId→网关实例 → Nacos 解析实例→健康 ip:port → gRPC DownstreamPush → 目标网关 → 推客户端
```

Nacos 未启用或连不上时，`NacosNodeDiscovery.resolveTarget()` **自动回退为直接用 nodeId(host:port)**，
单机/同机部署行为不变。节点被判定不健康时，健康节点会清理 Redis 中指向它的路由，
这些用户随即视为离线、消息转离线队列。清理是一次全库 SCAN，因此按「健康实例排序后
`hash(死实例) % 实例数`」选出唯一 owner 执行，避免 N-1 台各扫一遍全库；
选中的那台恰好也不可用时，靠路由 30s TTL 自然过期兜底。

#### 上行方向（网关 → IM 服务）怎么选实例

下行是按「用户在哪个网关」精确寻址，上行则是「在 N 个等价 im-server 里挑一个」，方向不同：

| | 注册 | 订阅 | 选择方式 |
|---|---|---|---|
| `GatewayNacosDiscovery`（im-gateway） | 自己 → `im-gateway` | 同服务实例（清死实例路由） | 不选，按 userId 精确寻址 |
| `IngressTargets`（im-gateway-boot） | 不注册 | `im-server` 的健康实例 | gRPC `round_robin`（subchannel 层轮询 + 故障拉黑） |
| `ServerNacosRegistrar`（im-server-boot） | 自己 → `im-server`（gRPC 端口） | — | — |

两条容易被忽略的规则：

1. **注册的必须是 gRPC 端口，不是 HTTP 端口**。网关调的是 `MessageIngress` gRPC 服务；
   `spring.grpc.server.host=0.0.0.0` 是监听地址、不是可达地址，不能拿去注册
   （`im.nacos.register-ip` 可显式指定，留空则按 POD_IP → 本机 IPv4 自动探测）。
2. **选实例交给 gRPC 的 `round_robin`，不要自己在应用层计数**。一条消息的处理路径上有固定个数的
   RPC（1 次 `pullPending` + 1 次 `submit`），如果按「每次 RPC 取模/轮询」选地址，只要
   「每请求 RPC 数」与「实例数」有公因子，同一类 RPC 就会永远命中同一实例——实测两个实例时
   6 条消息全打到其中一个，看起来像「发现没生效」。`round_robin` 工作在 subchannel 层，
   没有这个别名问题，且自带「连不上就摘掉、恢复后自动放回」。

实例下线后网关约 **15~30 秒**（Nacos 心跳超时窗口）才把它摘掉，这段窗口内打到它上面的
请求会得到 `503`，由客户端退避重发——所以 IM 服务优雅停机时应先 `deregisterSelf()`
再等在途请求收尾，而不是等心跳超时。

#### 健康检查：让「未就绪」的实例不参与轮询

`round_robin` 本来只挑 READY 的 subchannel（CONNECTING / 失败的不参与），但 **READY 只代表
「TCP + HTTP/2 连上了」**，不代表应用能服务。健康检查补的就是这一段：

| 层 | 机制 | 开关 |
|---|---|---|
| 服务端 | Spring Boot 自动把 `grpc.health.v1.Health` 绑到 gRPC 服务端，状态由 Actuator 健康聚合按周期刷新（启动未就绪 → NOT_SERVING，就绪 → SERVING，`include-overall-health` 暴露整体状态） | `spring.grpc.server.health.enabled: true`（需要 classpath 上有 `io.grpc:grpc-services`，否则自动配置静默不生效） |
| 客户端 | channel 上带 `healthCheckConfig` + `round_robin`：NOT_SERVING 的 subchannel 被当作不可用摘出 picker，恢复后自动放回 | `im.node.grpc.health-check-enabled: true` |
| 等待语义 | 一个 READY 实例都没有时，`withWaitForReady()` 排队等到 deadline，而不是立刻 `UNAVAILABLE` | 上行 `im.server.wait-for-ready: true`；下行 `im.push.wait-for-ready: false` |

为什么下行关掉 waitForReady：下行有「失败即落离线、上线补发」的确定性兜底，让 IM 服务把线程
挂在「等网关就绪」上没有收益；而上行等待能省掉客户端一次 1s+ 的退避重发（网关跑虚拟线程，等待几乎零成本）。

两条实测结论（写在测试里，别凭直觉改）：

- **对端没有健康服务时不会被摘掉**：Watch 返回 UNIMPLEMENTED，gRPC 保持该实例可用。
  所以「新版客户端 + 老版服务端」的滚动升级是安全的，没有硬性升级顺序。
- **关掉开关就回到旧行为**：NOT_SERVING 的实例照常参与轮询（`GrpcChannelFactoryHealthTest` 覆盖）。

验证位置：`im-rpc` 的 `GrpcChannelFactoryHealthTest`（两台真实服务端，一 SERVING 一 NOT_SERVING，
断言流量只落到前者、翻成 SERVING 后立刻恢复两边）与 `im-server-boot` 的 `GrpcHealthEndpointTest`
（起真实上下文，确认健康服务确实被绑定且状态最终为 SERVING）。

---

## 可靠性设计

| 问题 | 做法 |
|------|------|
| **重复投递** | `(from, clientMsgId)` 幂等：Redis 幂等表（带 TTL）+ MySQL 唯一索引兜底；重复提交返回原 `seq` |
| **消息乱序** | `seq` 按 `conversationId` 用 `INCR` 单调分配；客户端按 `seq` 排序、拉齐空洞 |
| **丢消息** | 先落库后推送；推送失败落离线队列；「可丢实时性，不可丢消息」 |
| **离线补发丢消息** | 补发是 at-least-once：`pullPending` 读不删，网关全部写成功才 `ackPending` 推进游标；失败重连重复补，客户端按 seq 去重 |
| **补发"已送达"不可判定** | 客户端 `sync` 上报的水位按 `im:wm:{userId}:{deviceId}` 落 Redis（30d TTL），是 delivered 的唯一可信口径 |
| **多端互相吃掉离线消息** | 补发进度按设备分开（`im:offline:cur` / `im:offline:gcur`），用户级只保留「最早未补发」锚点；锚点要等所有登记设备都补过才退休 |
| **会话列表写失败** | 只记 error，不影响受理：消息已落库，补发与历史拉取都不依赖会话列表（它只是「入口」） |
| **未读数与真实消息数不一致** | 不存计数，用 `last_seq - read_seq` 相减，天然自愈 |
| **网关宕机** | 路由 TTL 过期 + Nacos 健康剔除；残留路由由其他健康节点清理；消息转离线 |
| **IM 服务不可用** | 网关**保住连接**，回 `error 503` 让客户端退避重发，不主动断连（连接比单条消息的实时性稀缺） |
| **死连接** | Netty 心跳两段式探测，超时断开并清路由 |
| **快速重连误删** | 断连回调必须 `remove(userId, session)` 只摘自己那条会话，且仅在 `!isLocal(userId)` 时才清 Redis 路由 |
| **慢客户端** | `channel.isWritable()` 判断出站水位，积压时按投递失败转离线，不让内存被吃穿 |
| **缩容抖动** | 优雅摘流四步 + 客户端加抖动重连 |

---

## 可观测性与弹性

`/actuator/prometheus` 暴露的关键指标：

| 指标 | 类型 | 用途 |
|------|------|------|
| `gateway_connections_active` | Gauge | **HPA 主指标**（含 WS + TCP） |
| `gateway_connections_by_transport_active{transport}` | Gauge | 看客户端在 WS / TCP 之间的分布 |
| `gateway_sessions_opened_total` / `closed_total` | Counter | 连接抖动检测 |
| `gateway_sessions_migrated_total` | Counter | 摘流时主动迁走的连接数 |
| `gateway_sessions_idle_closed_total` | Counter | 心跳超时清理的连接数 |
| `gateway_biz_backpressure_paused_total` / `resumed_total` | Counter | 上行背压：累计暂停 / 恢复读取的次数。**两者基本相等**才正常 |
| `gateway_biz_backpressure_paused_active` | Gauge | 当前因积压而暂停读取的连接数。持续 >0 说明下游（Redis/DB/IM 服务）跟不上 |

> 空闲连接的 CPU≈0，**不能只用 CPU 扩网关**：夜间会被缩到 1 个实例，白天被重连风暴打穿。
> 连接数与 CPU 组合使用。

运维接口：

```bash
curl -X POST 'localhost:8080/admin/drain?reason=scale-in'   # 手动摘流（幂等）
curl localhost:8080/admin/status
# {"draining":false,"connections":2,"transports":{"websocket":":8081/ws","tcp":":9092"},
#  "websocket":1,"tcp":1,"backpressure":{"pausedConnections":0}}
```

K8s 清单：`k8s/im-gateway.yaml`（Deployment + 按连接数 HPA + preStop + 90s 宽限期）、
`k8s/im-server.yaml`（Deployment + HPA 按 CPU/吞吐）、`k8s/im-node.yaml`（单体形态，联调用）。

---

## 快速开始

### 环境要求

| 依赖 | 说明 |
|------|------|
| **JDK 21+** | 已验证 JDK 25；`JAVA_HOME` 必须指向 21+，否则编译报「不支持的发行版本」 |
| **Redis** | 必需，启动时即向 Redis 注册路由，无 Redis 会启动失败。`docker compose up -d` 一键起 |
| **Maven** | 仓库内置 Wrapper（`mvnw` / `mvnw.cmd`），无需单独安装 |
| **Nacos（可选）** | 仅用于实例地址动态发现，默认关闭 |
| **openssl（可选）** | 仅启用 gRPC TLS 时需要 |

```bash
export JAVA_HOME=/path/to/jdk21+     # Windows: set JAVA_HOME=D:\Environment\jdk25
```

### 单进程跑

```bash
docker compose up -d                                  # ① Redis（+ 可选 Nacos）
./mvnw -DskipTests package                            # ② 构建，产物 im-app/target/im-app-1.0.0.jar
IM_GRPC_PORT=9090 IM_GRPC_HOST=127.0.0.1 \
  java -jar im-app/target/im-app-1.0.0.jar            # ③ 启动
```

默认端口：Tomcat `8080`、WebSocket `8081`、TCP `9092`、gRPC `9090`。

或者用脚本一次拉起两个节点（验证跨节点转发）：

```bash
./scripts/run-two-nodes.sh     # 节点1: HTTP8080/WS8081/TCP9092/gRPC9090
                               # 节点2: HTTP18080/WS18081/TCP19092/gRPC9091
```

> 同机多实例必须**整组**错开端口：HTTP / WS / TCP 是三套独立端口。

### 测试

```bash
./mvnw test                            # 全量（当前 27 个用例，分布见下）
```

| 模块 | 用例 | 依赖 |
|---|---|---|
| `im-server` | `SeqAllocatorTest`（冷启动 / 客户端水位 / Redis 回退到旧快照 / 多实例顺序 / 8 线程并发 / DB 下界查询摊薄）、`MessageHistoryPagingTest`（翻页不重不漏 / 整页被删游标仍前进 / limit 夹取） | **真实 Redis**（`MessageHistoryPagingTest` 除外） |
| `im-storage-redis` | `RedisMessageRepositoryTest`（落库乱序时 `maxSeq` 仍取真正最大值；`before` 按 seq 而非物理顺序切片）、`RedisStorageConfigTest`（`im.storage` 缺省落 Redis、配 `jdbc` 时本模块不建 Bean） | **真实 Redis**（配置测试除外） |
| `im-storage-mongodb` | `MongoEntityMappingTest`（模型 ↔ 实体无损往返、`_id` 语义、索引注解解析出的名字与唯一性）、`MongoQueryMappingTest`（`id`/`serverMsgId` → `_id` 的查询与更新映射、`from`/`recalledAt` 等字段不被改名）、`MongoStorageConfigTest`（`im.storage=mongodb` 才装配四个端口、宿主已有客户端时不重复创建、其它形态下一律不建）、`MongoSchemaIndexTest`（复现「遗留 `uk_msg_server_id` 导致第二条消息撞 `dup key: null`」，并验证启动收敛会删掉它）、`MongoMessageRepositoryPagingTest`（`before` 的 seq 排他 / 升序 / 跨会话隔离） | 前三个不需要 Mongo；后两个要真实 Mongo（连不上自动跳过） |
| `im-rpc` | `GrpcChannelFactoryHealthTest`（NOT_SERVING 实例被摘出轮询、无健康服务的对端仍可用、waitForReady 等到 deadline、关开关回到旧行为） | 真实 gRPC 服务端（进程内起） |
| `im-gateway` | `HmacTokenAuthenticatorTest`（越权冒用 / 过期 / 篡改过期时间 / 换密钥伪造 / 格式错误）、`AuthGuardTest`（off 全放行、warn 只记录不误伤、enforce 拒绝、缺 secret 启动失败）、`SerialExecutorTest`（串行 / 保序 / 高水位暂停只触发一次且不丢帧 / 低水位才恢复 / 未到阈值不触发 / 控制任务不参与背压 / 指标监听收到暂停+恢复 / 单任务异常不中断） | 无 |
| `im-server-boot` | `GrpcHealthEndpointTest`（健康服务确实被绑定且最终 SERVING）、`ImServerApplicationScanTest`（三种介质的包都在扫描白名单里）、`StorageWiringTest`（`im.storage=redis` 时装配 Redis 实现，且不建 DataSource / MongoClient / MongoTemplate；两个上下文用例都显式钉住 `im.storage=redis`，不跟随部署侧默认值） | Spring 上下文 + Redis |

Redis 相关用例默认连 `127.0.0.1:6379`（可用 `-Dim.test.redis.host=... -Dim.test.redis.port=...` 改），
要验证的正是 Lua 的原子性和下界保护，把 Redis mock 掉就等于把最该测的部分测丢了。
用例只碰自己造的随机会话 ID（`im:seq:t:<uuid>`）并在结束后删除，不 FLUSH、不扫前缀批量删；
Redis 不可用时这些用例标记为 **skipped** 而非 failed，没有 Redis 的环境也能跑通构建。

### 双进程跑（网关与 IM 服务分开）

```bash
# ① IM 服务（不接长连接，只吃 gRPC + Redis/DB）
IM_GRPC_PORT=9091 SERVER_PORT=8088 java -jar im-server-boot/target/im-server-boot-1.0.0.jar

# ② 长连接服务（上行全走 gRPC）
IM_GRPC_PORT=9090 SERVER_PORT=8080 IM_SERVER_ADDR=127.0.0.1:9091 \
  java -jar im-gateway-boot/target/im-gateway-boot-1.0.0.jar

# 或一键：./scripts/run-split.sh
```

### 验证

**浏览器**：开两个标签页

- A 连节点1：`http://localhost:8080/?wsPort=8081`，userId 填 `alice`
- B 连节点2：`http://localhost:18080/?wsPort=18081`，userId 填 `bob`
- A 里「接收方」填 `bob` 发送，B 立刻收到（链路：`节点1 → Redis 查路由 → gRPC → 节点2 → 推 B`）

**REST**（不需要长连接客户端）：

```bash
# 发一条消息（复用同一套受理流程）
curl -X POST http://localhost:8080/send -H 'Content-Type: application/json' \
  -d '{"from":"alice","to":"bob","body":"hello"}'

# 拉会话历史（返回与 msg.push 同构的帧）
curl 'http://localhost:8080/conv/s:alice:bob/messages?limit=20'

# 群成员（没有它群永远是空的，扇出无从发生）
curl -X POST http://localhost:8080/group/g1/members \
  -H 'Content-Type: application/json' -d '["alice","bob","carol"]'
curl http://localhost:8080/group/g1/members
```

**原生 TCP 客户端**：连 `localhost:9092`，首帧发 `connect` 认证帧，之后按「4 字节大端长度 + JSON」收发。

### 启用 gRPC TLS（可选）

```bash
./scripts/gen-certs.sh                                # 生成自签名证书到 resources/grpc/
IM_GRPC_PORT=9090 IM_GRPC_TLS=true \
  ./mvnw -pl im-app spring-boot:run --spring.profiles.active=tls
```

证书走 Spring Boot 的 PEM SSL Bundle（`application-tls.yml`：`spring.ssl.bundle.pem.imssl` +
`spring.grpc.server.ssl.bundle=imssl`），客户端按 `im.node.grpc.tls.*` 自动切换 `useTransportSecurity()`。

### 跨机器 / K8s 要点

- 跨机物理部署时 `IM_GRPC_HOST` 填其他节点可达的真实 IP/域名（不能用 `127.0.0.1`）；
- **K8s 下留空即可**：`NodeAddressResolver` 通过 Downward API 的 `POD_IP`（兜底 `POD_NAME`）
  自动得到节点 ID = `POD_IP:9090`，gRPC Server 绑定 `0.0.0.0`；
- Redis 必须是所有节点都能访问的同一实例。

```bash
docker build -t your-registry/im-node:latest .
kubectl apply -f k8s/redis.yaml
kubectl apply -f k8s/im-node.yaml     # 或 im-gateway.yaml + im-server.yaml（拆分形态）
```

---

## 配置参考

### 存储

| 配置 | 环境变量 | 默认 | 说明 |
|------|----------|------|------|
| `im.storage` | `IM_STORAGE` | `redis` | `redis` / `jdbc` / `mongodb` |
| `im.db.url` | `IM_DB_URL` | 空 | 仅 `jdbc` 时读取 |
| `im.db.username` / `password` | `IM_DB_USERNAME` / `IM_DB_PASSWORD` | `root` / 空 | |
| `im.db.pool-size` | `IM_DB_POOL_SIZE` | `20` | Hikari 连接池 |
| `im.db.batch-enabled` | `IM_DB_BATCH_ENABLED` | `true` | group commit：并发到达的插入合并成一个事务 |
| `im.db.init-schema` | `IM_DB_INIT_SCHEMA` | `true` | 首次启动执行 DDL |
| `im.mongo.uri` | `IM_MONGO_URI` | `mongodb://127.0.0.1:27017/` | 仅 `mongodb` 时读取；启动先 `ping` 一次 |
| `im.mongo.database` | `IM_MONGO_DATABASE` | `im` | 库名 |
| `im.mongo.init-indexes` | `IM_MONGO_INIT_INDEXES` | `true` | 启动建索引（幂等）；三条唯一索引是保序与幂等的最终防线 |

### 长连接接入

| 配置 | 环境变量 | 默认 | 说明 |
|------|----------|------|------|
| `im.gateway.transport.ws-enabled` / `ws-port` / `ws-path` | `IM_WS_ENABLED` / `IM_WS_PORT` / `IM_WS_PATH` | `true` / `8081` / `/ws` | WebSocket |
| `im.gateway.transport.tcp-enabled` / `tcp-port` | `IM_TCP_ENABLED` / `IM_TCP_PORT` | `true` / `9092` | 原生 TCP |
| `im.gateway.transport.max-frame-bytes` | `IM_MAX_FRAME_BYTES` | `1048576` | 单帧上限，超限断链 |
| `im.gateway.max-devices-per-user` | `IM_MAX_DEVICES_PER_USER` | `5` | 单账号并存设备数上限，超限拒绝新连接 |
| `im.gateway.biz.max-pending-frames` | `IM_MAX_PENDING_FRAMES` | `1024` | 单连接上行积压**高水位**：超过即暂停读取（降到一半恢复），不丢帧不断连 |
| `im.gateway.transport.tcp-auth-timeout-ms` | `IM_TCP_AUTH_TIMEOUT_MS` | `10000` | TCP 首帧认证窗口 |
| `im.gateway.idle.enabled` | `IM_IDLE_ENABLED` | `true` | 心跳总开关 |
| `im.gateway.idle.ping-after-ms` | `IM_IDLE_PING_AFTER_MS` | `45000` | 空闲多久发 ping |
| `im.gateway.idle.close-after-ms` | `IM_IDLE_CLOSE_AFTER_MS` | `90000` | 空闲多久断开 |
| `im.gateway.drain.enabled` / `timeout-ms` / `retry-after-ms` | `IM_DRAIN_*` | `true` / `45000` / `1500` | 优雅摘流 |

### 接入鉴权

| 配置 | 环境变量 | 默认 | 说明 |
|------|----------|------|------|
| `im.auth.mode` | `IM_AUTH_MODE` | `off` | `off` / `warn` / `enforce` |
| `im.auth.secret` | `IM_AUTH_SECRET` | 空 | `warn`/`enforce` 时必填，与 token 签发方共用 |

### 集群与服务发现

| 配置 | 环境变量 | 默认 | 说明 |
|------|----------|------|------|
| `im.ingress.transport` | `IM_INGRESS_TRANSPORT` | `local` | `local` / `shadow` / `grpc`（仅单体装配入口） |
| `im.server.address` / `timeout-ms` / `wait-for-ready` | `IM_SERVER_ADDR` / `IM_SERVER_TIMEOUT_MS` / `IM_SERVER_WAIT_FOR_READY` | `127.0.0.1:9091` / `300` / `true` | 网关 → IM |
| `im.push.timeout-ms` / `wait-for-ready` | `IM_PUSH_TIMEOUT_MS` / `IM_PUSH_WAIT_FOR_READY` | `800` / `false` | IM → 网关，超时即落离线 |
| `im.delivery.max-inflight` | `IM_DELIVERY_MAX_INFLIGHT` | `1024` | 异步推送并发上限，打满即退化为同步推送 |
| `im.offline.frame-ttl` | `IM_OFFLINE_FRAME_TTL` | `7d` | 单聊离线队列与设备游标的过期时间 |
| `im.offline.max-frames` | `IM_OFFLINE_MAX_FRAMES` | `500` | 单聊离线队列每人上限（超出丢最旧） |
| `im.offline.pointer-ttl` | `IM_OFFLINE_POINTER_TTL` | `7d` | 群聊补发指针过期时间（超期放弃补发，历史仍可主动拉） |
| `im.watermark.ttl` | `IM_WATERMARK_TTL` | `30d` | 客户端消费水位保留时长（delivered 对账数据） |
| `im.conversation.list-ttl` | `IM_CONVERSATION_LIST_TTL` | `30d` | 会话列表 / 已读位置保留时长（长期无消息自然消失） |
| `im.conversation.fanout-limit` | `IM_CONVERSATION_FANOUT_LIMIT` | `1000` | 会话列表写扩散的人数上限，超过则只更新元信息（靠群反查表补齐） |
| `im.node.grpc-port` / `grpc-host` | `IM_GRPC_PORT` / `IM_GRPC_HOST` | `9090` / 自动探测 | 实例 ID |
| `im.node.grpc.tls.enabled` / `trust-cert` | `IM_GRPC_TLS` / `IM_GRPC_TRUST` | `false` / 空 | gRPC TLS |
| `im.node.grpc.health-check-enabled` | `IM_GRPC_HEALTH_CHECK` | `true` | 客户端健康检查（排除 NOT_SERVING 实例） |
| `spring.grpc.server.health.enabled` | — | `true` | 服务端 grpc.health.v1.Health（需 `grpc-services`） |
| `im.nacos.enabled` / `server-addr` / `service-name` / `namespace` | `IM_NACOS_*` | `false` / `127.0.0.1:8848` / `im-gateway` / 空 | 实例发现 |

### Redis key 归属

| key | 写方 | 读方 |
|-----|------|------|
| `im:gateway:user:{userId}` (SET) | 网关（TTL 续期） | IM 服务（下行路由：用户 → 网关实例集合） |
| `im:offline:q:{userId}` (ZSet) + `:pos` (String) | IM 服务 | IM 服务（单聊离线队列，score = 入队序号；读不删，TTL + 长度上限） |
| `im:offline:cur:{userId}:{deviceId}` (String) | IM 服务（网关 ack 驱动） | IM 服务（每设备已确认补发到的位置） |
| `im:offline:ptr:{userId}` (Hash) | IM 服务 | IM 服务（群聊补发**锚点**：会话 → 最早的未补发 seq，TTL） |
| `im:offline:gcur:{userId}:{deviceId}` (Hash) | IM 服务（网关 ack 驱动） | IM 服务（每设备在每个会话已补发到的 seq） |
| `im:gateway:devices:{userId}` (SET) | 网关 | IM 服务（该用户登记过的设备，判断锚点能否退休） |
| `im:wm:{userId}:{deviceId}` (Hash) | IM 服务 | IM 服务（客户端亲口确认的已见 seq，delivered 口径，30d TTL） |
| `im:offline:{userId}` (List) | — | **已废弃**，升级窗口内读一次把积压帧发掉 |
| `im:conv:msgs:{conversationId}` | IM 服务（`im.storage=redis`） | IM 服务 |
| `im:seq:{conversationId}` | IM 服务 | IM 服务 |
| `im:idem:{from}\|{clientMsgId}` | IM 服务 | IM 服务 |
| `im:group:members:{groupId}` | IM 服务（`im.storage=redis`） | IM 服务（群聊扇出） |
| `im:group:user:{userId}` (SET) | IM 服务（`im.storage=redis`） | IM 服务（反查「我加入了哪些群」，大群会话列表补齐） |
| `im:conv:meta:{convId}` (Hash) | IM 服务 | IM 服务（会话元信息：last_seq / last_time / 最后一条摘要） |
| `im:user:convs:{userId}` (ZSet) | IM 服务 | IM 服务（会话列表，score = 最后消息时间） |
| `im:user:read:{userId}` (Hash) | IM 服务 | IM 服务（**用户级**已读位置，多端共用） |

---

## 代码结构

```
im-cluster/
├── pom.xml                      # 父 POM + 模块列表 + 版本管理
├── docker-compose.yml           # Redis + Nacos(standalone)
├── scripts/
│   ├── run-two-nodes.sh         # 一键起两个 im-app 节点
│   ├── run-split.sh             # 一键起网关 + IM 服务（拆分形态）
│   └── gen-certs.sh             # 生成 gRPC TLS 自签名证书
├── k8s/
│   ├── im-gateway.yaml          # Deployment + 按连接数 HPA + preStop
│   ├── im-server.yaml           # Deployment + HPA(CPU)
│   ├── im-node.yaml             # 单体形态（联调）
│   └── redis.yaml
│
├── im-proto/                    # 契约层：两个服务唯一共享物
│   ├── src/main/proto/im_ingress.proto       # 上行 SubmitMessage / BatchSubmit / PullPending / AckPending
│   ├── src/main/proto/im_push.proto          # 下行 DownstreamPush.Push
│   └── contract/
│       ├── MessageEnvelope.java              # 消息信封（clientMsgId 幂等键 / conversationId 保序键）
│       ├── MessageEnvelopes.java             # 信封 ⇄ RPC 报文转换
│       ├── MessageIngress.java               # 上行端口（网关 → IM）
│       ├── PushPort.java                     # 下行端口（IM → 网关）
│       └── RedisKeys.java                    # key 空间 + 读写归属
│
├── im-rpc/                      # gRPC 适配（客户端侧），按传输方向分包
│   ├── common/                               # 两个方向共用
│   │   ├── GrpcChannelFactory.java           # channel 复用 + TLS
│   │   └── AddressResolver.java              # 实例 ID → 可 dial 地址（默认 DIRECT）
│   ├── ingress/                              # 网关 → IM 服务（上行）
│   │   └── RemoteMessageIngress.java         # 上行 gRPC 客户端
│   └── push/                                 # IM 服务 → 网关（下行）
│       ├── RemoteGatewayPushPort.java        # 下行：按路由分组推送
│       └── GatewayRouteReader.java           # 在线路由只读视图
│
├── im-gateway/                  # 长连接服务
│   ├── auth/AuthGuard.java                   # 鉴权闸门（off/warn/enforce 三档）
│   ├── auth/HmacTokenAuthenticator.java      # HMAC token 校验 + 本地签发工具
│   ├── node/NodeAddressResolver.java         # 探测本实例可达地址（POD_IP 等）
│   ├── node/GatewayNacosDiscovery.java       # 实例注册 + 健康剔除 + 清理脏路由
│   ├── session/ClientSession.java            # 传输无关的会话抽象
│   ├── session/NettyClientSession.java       # Netty Channel 上的实现（文本帧 / 长度帧）
│   ├── session/ConnectionManager.java        # 本实例 用户→设备→会话（唯一不外部化的状态）
│   ├── session/GatewayRegistry.java          # Redis 路由 im:gateway:user:{userId}=SET{实例} + 续期
│   ├── session/SessionAttributes.java        # Channel 属性：userId/deviceId/session
│   ├── transport/NettyGatewayServer.java     # Netty 双通道接入（WS + TCP）
│   ├── transport/SessionLifecycle.java       # 连接生命周期 + 上行帧处理（两种传输唯一实现）
│   ├── transport/BusinessDispatcher.java     # 上行业务执行器：虚拟线程池，每连接发一个串行执行器
│   ├── transport/SerialExecutor.java         # 每连接 FIFO 串行（保序 + 积压上限背压）
│   ├── transport/HeartbeatHandler.java       # 心跳：发 ping / 超时断开
│   ├── transport/websocket/*                 # WS pipeline：握手准入 + 文本帧适配
│   ├── transport/tcp/*                       # TCP pipeline：长度帧 + 首帧 connect 认证
│   ├── protocol/ClientFrames.java            # 控制帧编解码（ack/pong/ping/migrate/error/connected）
│   ├── drain/GatewayDrainService.java        # 优雅摘流
│   ├── metrics/GatewayMetrics.java           # 连接数 HPA 指标 + 上行背压指标
│   ├── push/LocalSessionPusher.java          # 推给本实例会话
│   ├── push/DownstreamPushService.java       # @GrpcService：接收下行推送
│   ├── push/GatewayPushClient.java           # 跨实例 gRPC 客户端
│   ├── push/GatewayPushPort.java             # PushPort 实现（单体装配的快路径）
│   └── controller/GatewayAdminController.java # POST /admin/drain、GET /admin/status
│
├── im-server/                   # IM 服务
│   ├── message/MessageService.java           # 受理主流程：幂等 + 取号 + 落库 + 立刻回 ack
│   ├── message/DeliveryDispatcher.java       # 异步投递 + 失败落离线（把推送挪出 ack 关键路径）
│   ├── message/SeqAllocator.java             # seq 发号：幂等查询与 INCR 合成一个 Lua（一次 RTT）
│   ├── message/MessageRouter.java            # 投递目标决策（含群聊扇出），唯一依赖 PushPort
│   ├── message/MessageRepository.java        # 存储端口（本模块不含任何实现）
│   ├── message/StoredMessage.java            # 结构化存储记录
│   ├── message/OfflineMessageStore.java      # 离线消息
│   ├── message/ClientFrames.java             # 下行 msg.push / msg.state 帧序列化（实时与历史同构）
│   ├── message/MessageStateNotifier.java     # 状态回执：把 delivered/read 推回发送方（仅单聊）
│   ├── message/MessageUpdateNotifier.java    # 变更事件：撤回广播全会话、删除只推操作者自己的端
│   ├── message/MessageDigests.java           # 会话摘要与媒体占位符（列表与通知共用同一份文案）
│   ├── push/HttpVendorPush.java              # 厂商推送（离线通知）：HTTP 中转，默认关闭
│   ├── group/GroupMemberRepository.java      # 群成员端口（实现见 im-storage-*）
│   ├── relation/MessageAcl.java              # 投递前准入：群成员 + 黑名单 + 可选好友（三档开关）
│   ├── relation/RelationRepository.java      # 关系链端口（好友双向 / 黑名单单向）
│   ├── grpc/MessageIngressGrpcService.java   # @GrpcService：供远程网关上行
│   └── controller/                           # /send、/conv/{conversationId}/messages、/group/...
│
├── im-storage-redis/            # 存储实现插件（Redis，缺省形态）
│   ├── RedisStorageConfig.java               # 条件装配：im.storage=redis 或未配置（四个端口一起）
│   ├── RedisMessageRepository.java           # List 存 JSON + 撤回/删除用 Set
│   ├── RedisGroupRepository.java             # 群资料 Hash（单字段改，避免并发覆盖）
│   ├── RedisGroupMemberRepository.java       # 群成员 Set + 反向索引 + 角色/禁言
│   └── RedisRelationRepository.java          # 好友双向两份 / 黑名单单向
│
├── im-storage-jdbc/             # 存储实现插件（MySQL）
│   ├── JdbcStorageConfig.java                # 条件装配：仅 im.storage=jdbc
│   ├── JdbcMessageRepository.java
│   ├── JdbcBatchAppender.java                # group commit：并发插入合并成一次事务提交
│   ├── JdbcGroupMemberRepository.java
│   └── resources/db/schema.sql               # DDL
│
├── im-storage-mongodb/          # 存储实现插件（MongoDB）
│   ├── MongoStorageConfig.java               # 条件装配：仅 im.storage=mongodb（自建客户端 + 启动 ping）
│   ├── MongoMessageRepository.java           # _id = serverMsgId；(conv, seq) 走唯一索引定位；撤回用带条件的单文档更新
│   ├── MongoGroupRepository.java
│   ├── MongoGroupMemberRepository.java       # 角色 / 禁言挂在成员文档上，退群即一起消失
│   ├── MongoRelationRepository.java          # 有向边：好友两条 / 黑名单一条
│   ├── MongoSchema.java                      # 解析实体上的索引注解并建索引（幂等）
│   └── entity/                               # 实体映射：MessageEntity / MessageDeleteEntity /
│                                             #   GroupEntity / GroupMemberEntity / RelationEntity
│                                             #   （@Document + @MongoId + @CompoundIndex/@Indexed）
│
├── im-app/                      # 单体装配入口
│   ├── ImClusterApplication.java
│   ├── app/config/IngressConfig.java         # local / grpc / shadow 选型（@Primary）
│   ├── app/shadow/ShadowMessageIngress.java  # 双跑比对（异步，不影响主路径）
│   ├── app/controller/ShadowReportController.java  # GET /shadow/stats
│   └── resources/static/index.html           # 浏览器验证页面
│
├── im-gateway-boot/             # 拆分部署：长连接服务入口
└── im-server-boot/              # 拆分部署：IM 服务入口
```

**两个 `ClientFrames` 同名类**：网关只产出控制帧、IM 服务只产出业务 `msg.push` 帧，语义不同故各留一份；
单体装配时分别以 `gatewayClientFrames` / `serverClientFrames` 命名 bean 避免冲突。

---

## 已知约束与坑

### 长连接相关

- **Netty 的 WebSocket 路径匹配不剥 query string**：`WebSocketServerProtocolHandler` 默认用
  `uri().equals(websocketPath)` 判断，`/ws?userId=alice` 不等于 `/ws`，请求会被静默转发到 pipeline 尾部丢弃
  ——既不应答 101 也不报错，客户端只表现为握手超时。本项目在握手闸门里校验完身份后
  `request.setUri(wsPath)` 再交下去。
- **Lua 脚本要兼容 Redis 3.x**：本机联调用的 Redis 是 `3.2.100`（Windows 老版本），所以脚本里
  **不能用**多字段 `HSET k f1 v1 f2 v2`（4.0+）、`ZADD ... GT/XX/NX`（6.2+）、`UNLINK`（4.0+）、
  `GETEX`（6.2+）等新命令，否则报的是 `Wrong number of args calling Redis command From Lua script`
  这种和真实原因完全无关的错。需要「只前进」语义时先用 `HGET`/`ZSCORE` 比较再写。
  `SET k v PX ms`（2.6.12+）、`HSETNX`、`ZRANGEBYSCORE`、`ZREMRANGEBYRANK`、`PEXPIRE` 都可以放心用。
- **多端靠 deviceId 区分**：`ConnectionManager` 的 key 是 `userId → deviceId → 会话`。
  同一 deviceId 重连踢旧连接；**不带 deviceId 的客户端全部落到 `default` 槽位**，
  等价于旧的「单用户单连接」——想多端必须带 deviceId。
- **离线队列/锚点共享，进度按设备分开**：队列和群聊锚点还是「用户级」的一份（写入时不知道
  用户有哪些设备，按设备写扩散代价太大），但**确认进度是 per-device** 的，补发帧也只推给
  发起请求的那条连接。所以先上线的设备不会把后上线那份吃掉。
- **未读数是算的，已读是用户级的**：见「会话列表与未读数」。多端合并在这一层是免费的——
  已读只有一份状态（`im:user:read:{userId}`），不存在「N 个设备各报一个已读、读取时取 max」的合并逻辑。
  需要按设备分的只有**同步**水位（`im:wm:{userId}:{deviceId}`），那是传输语义，别混。
- **分片只落到「维度 + 路由函数」**：Redis 侧已按会话/用户分 slot，MySQL 侧的分区脚本未启用，
  也没有跨实例的库路由（`ShardRouter.messageTableOf` 目前只产出表名，不产出库名）。
  key 名已变更（`im:conv:{c}:meta` 形态），旧数据不会自动读到，需要清库或做一次 rename 迁移。
- **撤回/删除的能力边界**：没有「群主/管理员撤回他人消息」（需要群角色），没有「双向删除」
  （delete for everyone，等价于撤回但不限发送方），没有「撤回后可编辑重发」，
  也没有合规层面的物理清除（消息本体与对象存储里的文件都还在）。
- **本地删除不影响未读数**：未读是 `lastSeq - readSeq` 的游标差值，删掉一条未读消息不会让它减一。
- **准入只到「关系 + 群成员」粒度**：没有群角色权限（群主/管理员/谁能踢人、谁能禁言），
  也没有「拉黑后历史会话如何处理」「删除好友后能否继续会话」这类产品策略。
  `im_relation` 的写入也不是原子的：好友是两行，中途失败会留下单边残留，
  而 `areFriends` 要求两侧都存在（所以残留只会让关系「看起来没建立」，不会误放行），重试即收敛。
- **状态回执只覆盖单聊**：`msg.state`（DELIVERED / READ）不推群聊。群里的「已读」是
  「谁读到第几号」的集合，要对前缀内每个发送者分发，扇出是「成员数 × 消息数」，
  得先有群已读的聚合视图（已读/未读人数）才谈得上正确。当前的群聊消息仍可查水位，只是不发帧。
- **富媒体只传 URL，不传字节**：`attrs.url` 指向对象存储，上传/下载/鉴权/清理都不在本项目范围内
  （也没有内置上传接口）。服务端不校验 URL 是否可达——那是接入方与存储之间的事。
- **`contentType` / `attrs` 需要库表同步升级**：老库升级要手工执行 `schema.sql` 头部注释里的
  `ALTER TABLE im_message ADD COLUMN ...`，否则 MyBatis-Plus 生成的列清单会带上不存在的列。
- **断连必须按会话移除**：见「可靠性设计」里的快速重连条目。
- **客户端必须回 `pong`**：服务端发的应用层 `{"type":"ping"}` 需要应答，否则空闲超时后会被断开。
- **鉴权只在建连时校验一次**：连接建立后 token 过期不会主动断连（长连接可能远活过 token 的 TTL）。
  需要「过期即踢」的话，得在心跳里带剩余有效期，或定期重放一次校验——目前没做。
  另外 `warn` 档是**每条连接一条 warn 日志**，客户端规模大时上线前观察期要注意日志量。
- **鉴权默认是 `off`**：这意味着不配 `im.auth.mode` 时任何人构造 `?userId=任意人` 就能冒充。
  对外发布前必须切到 `warn`（观察）→ `enforce`，并通过环境变量注入 `IM_AUTH_SECRET`。

### 框架与依赖

- **Jackson 3**：Boot 4.1 起 `ObjectMapper` / `JsonNode` 包名是 `tools.jackson.databind`；
  `MessageEnvelope.clientTs` 用 `Long` 而非 `long`，否则旧报文不带该字段时 REST 入口直接 400
  （`FAIL_ON_NULL_FOR_PRIMITIVES` 默认开启）。
- **gRPC / protobuf 版本**：引用 Boot BOM 管理的 `${grpc-java.version}` / `${protobuf-java.version}`，
  不要自行覆盖，避免与 BOM 内 Netty 版本冲突。
- **Redis 必需**：应用启动时即向 Redis 注册网关路由，无 Redis 会启动失败。
- **多模块构建**：父 POM 是 `pom` 打包，`mvnw spring-boot:run` 必须带 `-pl im-app`（或对应入口模块）；
  `spring-boot-maven-plugin` 只配在部署入口，库模块不 repackage。
- **打包前先停服务**：fat jar 被占用时 `repackage` 会 rename 失败；更隐蔽的是 `jar` 阶段已覆盖普通 jar，
  运行中的 JVM 后续懒加载类失败，现象是网关一直返回 `503 im service unavailable`。

### 数据与运维

- **Redis `keys()` 扫描**：清理死节点路由用的是 `keys()` 全量扫描，规模大时建议改 `SCAN` 游标遍历。
- **MySQL 形态需要自备库**：`im.storage=jdbc` 前先准备好 schema 与账号，并确认
  `im.db.init-schema` 是否符合预期（生产建议关闭，用 Flyway/Liquibase 管理）。
- **HPA 不要只看 CPU**：空闲连接 CPU≈0，见「可观测性与弹性」。
