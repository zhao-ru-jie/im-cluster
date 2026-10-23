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
| 消息存储 | **Redis / MySQL 可切换** | `im.storage` 决定实现，同一份 jar |
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
| `im-server` | **IM 服务** | 消息受理（幂等 + seq）、存储、路由决策、离线兜底、历史消息、群成员 | 库 |
| `im-storage-jdbc` | 存储实现插件 | `MessageRepository` / `GroupMemberRepository` 的 MySQL 实现 + DDL | 依赖 jar |
| `im-app` | 单体装配入口 | 同进程装配网关 + IM 服务，可执行 fat jar | fat jar |
| `im-gateway-boot` | 拆分部署：长连接服务 | 只做接入，上行全走 gRPC | fat jar |
| `im-server-boot` | 拆分部署：IM 服务 | 只承载业务与存储，暴露 `MessageIngress`，经 `RemoteGatewayPushPort` 下发 | fat jar |

### 依赖方向（编译期单向）

```
im-app   ──> im-gateway ──┐
         ──> im-server ───┼──> im-proto
         ──> im-rpc ──────┘

im-gateway-boot ──> im-gateway / im-rpc / im-storage-jdbc ──> im-proto
im-server-boot  ──> im-server  / im-rpc / im-storage-jdbc ──> im-proto
```

三条硬约束：

1. **`im-gateway` 与 `im-server` 互不依赖**，唯一共享物是 `im-proto`；
2. 业务模块不依赖 `im-rpc`，保持「业务代码里看不到传输细节」；
3. `im-storage-jdbc` 单向依赖 `im-server` 的存储端口，**反向不依赖**——换存储实现不改业务。
   刻意不用 `spring-boot-starter-jdbc`，避免只用 Redis 的部署被自动装配出一个连不上的 DataSource。

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

im-gateway ──写──> Redis(im:gw:user:{userId} = SET{网关实例})
im-server  ──读写──> Redis(im:conv:msgs / im:offline / im:seq / im:idem)（im.storage=jdbc 时消息落 MySQL）
```

### 一条消息的完整链路

以「网关 1 上的 alice 发给网关 2 上的 bob」为例：

1. **接入**：alice 的帧走 WebSocket 文本帧或 TCP 长度帧到达网关 1，`SessionLifecycle` 解析出 `msg.send`；
2. **上行**：网关经 `MessageIngress.submit()` 交给 IM 服务（同进程直调，或 gRPC 远程）；
3. **幂等**：IM 服务以 `(from, clientMsgId)` 查/写幂等表，重复提交直接返回原 `seq`（`duplicate=true`）；
4. **保序 + 标识**：按 `conversationId` 用**逐条 Redis 原子递增**（Lua `INCR`）分配单调 `seq`，
   并**独立**生成 `serverMsgId`（UUIDv7）——两者解耦，seq 允许有洞、回绕也不会撞 ID；
5. **落库（write-ahead）**：经 `MessageRepository.append()` 写存储——先落库，再推送；
6. **路由**：读 `im:gw:user:{bob}`（集合）得到 bob 所在的**所有**网关实例；Nacos 启用时再解析成健康 ip:port；
7. **下发**：`PushPort.push()` → gRPC `DownstreamPush.Push` 到这些实例 → 各实例按 `userId` 找**本机全部设备** → 逐条写帧；
8. **离线兜底**：网关 2 返回失败（不在线 / 推送异常）→ 该帧落 `im:offline:{bob}`，bob 重连时补发；
9. **确认**：网关 1 收到结果后给 alice 回 `msg.ack`（含 `serverMsgId`、`seq`）。

关键取舍：**先落库后推送**（宁可重复推送，不可丢消息）、**推送失败即转离线**（不做无限重试）、
`frame` 由 IM 服务序列化后网关**原样透传不解析**（新增业务字段不需要动网关）。

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
{"ver":1,"type":"ping"}
{"ver":1,"type":"sync","watermarks":{"s:alice:bob":100}}          // 上报各会话已见最大 seq
{"ver":1,"type":"connect","userId":"alice","deviceId":"native","token":"..."}   // 仅 TCP 首帧

// 网关 → 客户端
{"ver":1,"type":"msg.ack","clientMsgId":"c-1","serverMsgId":"0192f0c4-8a3b-7c2d-9f01-4b7a2c8d5e60","seq":7,"code":0,"traceId":"..."}
{"ver":1,"type":"msg.push","conversationId":"s:alice:bob","convType":"SINGLE","seq":7,
 "serverMsgId":"0192f0c4-8a3b-7c2d-9f01-4b7a2c8d5e60","clientMsgId":"c-1","from":"alice","to":"bob","body":"hi","ts":1750000000000}
{"ver":1,"type":"pong","ts":1750000000000}
{"ver":1,"type":"error","clientMsgId":"c-1","code":503,"message":"im service unavailable"}
{"ver":1,"type":"session.migrate","reason":"scale-in","retryAfterMs":1500}
```

`msg.push` 与 `GET /conv/{conversationId}/messages` 返回的历史消息**同构**（同一份模板渲染），
客户端「拉历史 + 接实时帧」不需要两套解析。旧报文 `{"to":...,"body":...}`（不带 `type`）仍然可用。

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

链路上三处同时配合才成立：

1. **本机会话表**：`ConnectionManager` 变成 `userId → deviceId → 会话`，`send(userId, frame)` 扇出到
   所有设备。部分设备写失败只要有一台成功就算投递成功——其余设备重连后按 seq 拉齐；
2. **在线路由**：`im:gw:user:{userId}` 从 String 变成 **SET**：手机连网关 A、网页连网关 C 时集合里
   有两个实例，IM 服务逐个实例推，各实例再分给本机设备；
3. **failed 语义**：`failed = 目标 − 已投递`。不能直接汇总各实例返回的 failed，否则
   「手机推到、网页所在实例失败」会让一个**已经收到消息的人**被再排一份离线。

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
List<StoredMessage> recent(String conversationId, int limit);
```

落库的是**结构化记录**（不是已序列化好的下行帧），帧在读取时按需重建。两种实现：

| 实现 | 位置 | 介质 | 适用 |
|------|------|------|------|
| `RedisMessageRepository` | `im-server`（默认） | `im:conv:msgs:{conversationId}` List，JSON | 本地开发 / 联调，零外部数据库 |
| `JdbcMessageRepository` | `im-storage-jdbc` | MySQL `im_message` | 生产形态，支持按会话 + 序号漫游 |

切换只需配置（`im.storage=jdbc` 时才装配连接池，默认形态完全不碰数据库）：

```yaml
im:
  storage: jdbc            # redis（默认）| jdbc
  db:
    url: jdbc:mysql://127.0.0.1:3306/im-cluster?useSSL=false&allowPublicKeyRetrieval=true
    username: root
    password: root
    pool-size: 20
    init-schema: true      # 首次启动执行 db/schema.sql；生产建议关掉交给 Flyway
```

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

离线判定与存储都在 **IM 服务**侧（不是网关职责）：

1. 网关推送返回失败（不在线 / 在别的实例但推失败）；
2. IM 服务把帧写入 `im:offline:{userId}`；
3. 用户重连后网关调 `MessageIngress.pullPending()` 拉取并逐条下发（单次上限 200 条，返回 `remaining`）。

### 群聊与群成员

`GroupMemberRepository` 同样双实现：Redis Set `im:group:members:{groupId}`（默认）或 MySQL `im_group_member`。
群聊扇出 = 群成员列表去掉发送者；成员列表为空时**当成无人可投**，绝不退化成「投给群 ID」
（那会往离线队列塞进永远不被拉取的数据）。

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
- **在线路由**：`im:gw:user:{userId}`（SET 集合，多端时含多个实例）只由网关写（TTL 30s + 10s 续期），IM 服务只读。

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
单机/同机部署行为不变。节点被判定不健康时，其他健康节点会清理 Redis 中指向它的路由，
这些用户随即视为离线、消息转离线队列。

#### 上行方向（网关 → IM 服务）怎么选实例

下行是按「用户在哪个网关」精确寻址，上行则是「在 N 个等价 im-server 里挑一个」，方向不同：

| | 注册 | 订阅 | 选择方式 |
|---|---|---|---|
| `GatewayNacosDiscovery`（im-gateway） | 自己 → `im-gateway` | 同服务实例（清死实例路由） | 不选，按 userId 精确寻址 |
| `IngressNacosDiscovery`（im-gateway-boot） | 不注册 | `im-server` 的健康实例 | gRPC `round_robin`（subchannel 层轮询 + 故障拉黑） |
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

> 空闲连接的 CPU≈0，**不能只用 CPU 扩网关**：夜间会被缩到 1 个实例，白天被重连风暴打穿。
> 连接数与 CPU 组合使用。

运维接口：

```bash
curl -X POST 'localhost:8080/admin/drain?reason=scale-in'   # 手动摘流（幂等）
curl localhost:8080/admin/status
# {"draining":false,"connections":2,"transports":{"websocket":":8081/ws","tcp":":9092"},
#  "websocket":1,"tcp":1}
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
| `im-server` | `SeqAllocatorTest`（冷启动 / 客户端水位 / Redis 回退到旧快照 / 多实例顺序 / 8 线程并发 / DB 下界查询摊薄）、`RedisMessageRepositoryTest`（落库乱序时 `maxSeq` 仍取真正最大值） | **真实 Redis** |
| `im-rpc` | `GrpcChannelFactoryHealthTest`（NOT_SERVING 实例被摘出轮询、无健康服务的对端仍可用、waitForReady 等到 deadline、关开关回到旧行为） | 真实 gRPC 服务端（进程内起） |
| `im-gateway` | `HmacTokenAuthenticatorTest`（越权冒用 / 过期 / 篡改过期时间 / 换密钥伪造 / 格式错误）、`AuthGuardTest`（off 全放行、warn 只记录不误伤、enforce 拒绝、缺 secret 启动失败）、`SerialExecutorTest`（串行 / 保序 / 积压上限拒新 / 控制任务不受背压 / 单任务异常不中断） | 无 |
| `im-server-boot` | `GrpcHealthEndpointTest`（健康服务确实被绑定且最终 SERVING） | Spring 上下文 + Redis |

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
| `im.storage` | `IM_STORAGE` | `redis` | `redis` / `jdbc` |
| `im.db.url` | `IM_DB_URL` | 空 | 仅 `jdbc` 时读取 |
| `im.db.username` / `password` | `IM_DB_USERNAME` / `IM_DB_PASSWORD` | `root` / 空 | |
| `im.db.pool-size` | `IM_DB_POOL_SIZE` | `20` | Hikari 连接池 |
| `im.db.init-schema` | `IM_DB_INIT_SCHEMA` | `true` | 首次启动执行 DDL |

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
| `im.node.grpc-port` / `grpc-host` | `IM_GRPC_PORT` / `IM_GRPC_HOST` | `9090` / 自动探测 | 实例 ID |
| `im.node.grpc.tls.enabled` / `trust-cert` | `IM_GRPC_TLS` / `IM_GRPC_TRUST` | `false` / 空 | gRPC TLS |
| `im.node.grpc.health-check-enabled` | `IM_GRPC_HEALTH_CHECK` | `true` | 客户端健康检查（排除 NOT_SERVING 实例） |
| `spring.grpc.server.health.enabled` | — | `true` | 服务端 grpc.health.v1.Health（需 `grpc-services`） |
| `im.nacos.enabled` / `server-addr` / `service-name` / `namespace` | `IM_NACOS_*` | `false` / `127.0.0.1:8848` / `im-gateway` / 空 | 实例发现 |

### Redis key 归属

| key | 写方 | 读方 |
|-----|------|------|
| `im:gw:user:{userId}` (SET) | 网关（TTL 续期） | IM 服务（下行路由：用户 → 网关实例集合） |
| `im:offline:{userId}` | IM 服务 | IM 服务 |
| `im:conv:msgs:{conversationId}` | IM 服务（`im.storage=redis`） | IM 服务 |
| `im:seq:{conversationId}` | IM 服务 | IM 服务 |
| `im:idem:{from}\|{clientMsgId}` | IM 服务 | IM 服务 |
| `im:group:members:{groupId}` | IM 服务（`im.storage=redis`） | IM 服务（群聊扇出） |

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
│   ├── src/main/proto/im_ingress.proto       # 上行 SubmitMessage / BatchSubmit / PullPending
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
│   ├── session/GatewayRegistry.java          # Redis 路由 im:gw:user:{userId}=SET{实例} + 续期
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
│   ├── metrics/GatewayMetrics.java           # 连接数 HPA 指标
│   ├── push/LocalSessionPusher.java          # 推给本实例会话
│   ├── push/DownstreamPushService.java       # @GrpcService：接收下行推送
│   ├── push/GatewayPushClient.java           # 跨实例 gRPC 客户端
│   ├── push/GatewayPushPort.java             # PushPort 实现（单体装配的快路径）
│   └── controller/GatewayAdminController.java # POST /admin/drain、GET /admin/status
│
├── im-server/                   # IM 服务
│   ├── message/MessageService.java           # 幂等 + seq + 落库 + 推送 + 离线兜底
│   ├── message/MessageRouter.java            # 投递目标决策（含群聊扇出），唯一依赖 PushPort
│   ├── message/MessageRepository.java        # 存储端口
│   ├── message/RedisMessageRepository.java   # 默认存储实现
│   ├── message/StoredMessage.java            # 结构化存储记录
│   ├── message/OfflineMessageStore.java      # 离线消息
│   ├── message/ClientFrames.java             # 下行 msg.push 帧序列化（实时与历史同构）
│   ├── group/GroupMemberRepository.java      # 群成员端口 + Redis 实现
│   ├── grpc/MessageIngressGrpcService.java   # @GrpcService：供远程网关上行
│   └── controller/                           # /send、/conv/{conversationId}/messages、/group/...
│
├── im-storage-jdbc/             # 存储实现插件（MySQL）
│   ├── JdbcStorageConfig.java                # 条件装配：仅 im.storage=jdbc
│   ├── JdbcMessageRepository.java
│   ├── JdbcGroupMemberRepository.java
│   └── resources/db/schema.sql               # DDL
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
- **多端靠 deviceId 区分**：`ConnectionManager` 的 key 是 `userId → deviceId → 会话`。
  同一 deviceId 重连踢旧连接；**不带 deviceId 的客户端全部落到 `default` 槽位**，
  等价于旧的「单用户单连接」——想多端必须带 deviceId。
- **离线队列是「用户级」的**：`im:offline:{userId}` 会被**第一个**重连的设备一次性取走（`LTRIM` 语义），
  其它设备拿不到同一批帧，需要按 seq 拉历史补齐。要真正做到「每设备独立水位」得把队列按
  `userId+deviceId` 分片，那是下一步的事。
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
