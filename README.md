# IM 集群（跨节点 gRPC 转发）— Spring Boot 4.1.x Demo

一个可运行的 IM 服务集群骨架：客户端 A 连在节点 1，客户端 B 连在节点 2（不同 JVM / 不同机器），
A 发给 B 的消息通过 **Redis 路由表定位目标节点 + gRPC 节点间转发** 送达。

技术栈：Spring Boot 4.1.x、官方 `spring-boot-starter-grpc-server`（Netty gRPC、`@GrpcService`）、
WebSocket 长连接、Redis（路由表 + 离线消息）、虚拟线程（JDK 21+）、Jackson 3。

---

## 1. 环境要求

| 依赖 | 说明 |
|------|------|
| **JDK 21+** | 已验证 JDK 25（本机 `D:/Environment/jdk25`）。Boot 4.1 + 虚拟线程需要 JDK 21+。**运行前务必 `export JAVA_HOME` 指向 JDK 21+**，否则编译报「不支持的发行版本 21」。 |
| **Redis** | 路由表与离线消息存储。用仓库内 `docker-compose.yml` 一键起（推荐）。 |
| **Nacos(可选)** | 仅用于「节点 gRPC 地址动态发现」演示。默认不启用；`docker-compose.yml` 已附带 standalone 版，需要时开启 `IM_NACOS_ENABLED=true`。**不走 Spring Cloud Alibaba 全家桶，只引入 `nacos-client` SDK**，规避 Boot 4.1 兼容风险。 |
| **Maven** | 已内置 Maven Wrapper（`mvnw` / `mvnw.cmd`），**无需单独安装 Maven**。 |
| **openssl** | 仅启用 gRPC TLS 时需要。 |

> ⚠️ 本机默认 `JAVA_HOME` 指向 JDK 17，请用你自己的 JDK 21+ 覆盖，例如：
> ```bash
> export JAVA_HOME=/d/Environment/jdk25   # Linux / Git Bash
> set JAVA_HOME=D:\Environment\jdk25      # Windows cmd
> ```

---

## 2. 三步跑起来（默认明文，单/双节点）

```bash
# ① 起 Redis（首次会拉取镜像）
docker compose up -d

# ② 编译打包
./mvnw -DskipTests package          # Windows: mvnw.cmd -DskipTests package

# ③ 启动节点（节点 ID 自动 = grpc-host:grpc-port，无需手动指定 IM_NODE_ID）
#   节点1（HTTP 8080 / gRPC 9090，nodeId=127.0.0.1:9090）
IM_GRPC_PORT=9090 mvnw spring-boot:run
#   另开一个终端，节点2（HTTP 8081 / gRPC 9091，nodeId=127.0.0.1:9091）
IM_GRPC_PORT=9091 mvnw spring-boot:run --server.port=8081
```

**懒人模式**：直接跑 `./scripts/run-two-nodes.sh`，会自动构建并拉起两个节点（日志在 `logs/`）。

---

## 3. 验证跨节点转发

打开两个浏览器标签页（或两台机器）：

- A 连节点1：访问 `http://localhost:8080/`，userId 填 `alice`，点「连接」
- B 连节点2：访问 `http://localhost:8081/`，userId 填 `bob`，点「连接」
- A 页面里「接收方」填 `bob`、输入内容，点「发送」

B 应能立刻收到消息 —— 这条消息走的是 `节点1 → (Redis 查路由) → gRPC 转发 → 节点2 → WebSocket 推 B`。

也可以用 REST 快速验证（不需要 WebSocket 客户端）：
```bash
curl -X POST http://localhost:8080/send \
  -H 'Content-Type: application/json' \
  -d '{"from":"alice","to":"bob","body":"hello"}'
```

---

## 4. 离线消息

目标用户不在线（Redis 路由表里没有该用户）时，消息写入 Redis 列表 `im:offline:{userId}`；
用户重新连上后由 `ImWebSocketHandler` 自动补发。无需额外配置。

---

## 5. 启用 gRPC TLS（可选）

```bash
# ① 生成自签名证书（脚本会写进 src/main/resources/grpc/）
./scripts/gen-certs.sh

# ② 启动节点时开启 TLS profile（节点 ID 仍为 grpc-host:grpc-port，自动生成）
IM_GRPC_PORT=9090 IM_GRPC_TLS=true \
  mvnw spring-boot:run --spring.profiles.active=tls
```

证书用 Spring Boot 的 PEM SSL Bundle（`application-tls.yml`：
`spring.ssl.bundle.pem.imssl` + `spring.grpc.server.ssl.bundle=imssl`）。
客户端按 `im.node.grpc.tls.*` 自动切换 `useTransportSecurity()`。

---

## 6. 跨机器部署要点

- 跨机**物理部署**时 `IM_GRPC_HOST` 填其他节点可达的真实 IP / 域名（不能用 `127.0.0.1`）。
- **K8s / 容器化部署下 `IM_GRPC_HOST` 留空即可**：`NodeAddressResolver` 通过 K8s Downward API 的 `POD_IP`
  自动拿到 pod 真实 IP 作为节点 ID，无需写死（详见第 7 节）。
- 节点 ID 自动由「探测到的可达地址:grpc-port」构成，**无需也不应手动指定**，天然唯一。
- Redis 需是所有节点都能访问到的同一个实例。

---

## 7. 用 Nacos-Client 做 gRPC 地址动态发现（可选）

默认路由只用 Redis 里的 `nodeId`(= host:port) 直接 dial，已能工作。引入 Nacos 的目的是：
**让「节点 IP」这一层也变成可动态发现 + 自动剔除死节点**，而不是把 IP 写死在 Redis 里。

本工程只依赖 `nacos-client` 这个轻量 SDK（**不走 Spring Cloud Alibaba 全家桶**，规避 Boot 4.1 兼容风险）。

### 职责边界（很重要）

- **Nacos 负责**：「nodeId → 实时可达的 gRPC 地址(ip:port)」，带健康探针（节点宕机自动剔除）。
- **Redis 仍负责**：「userId → 连在哪个节点(nodeId)」——这是 per-user 会话状态，Nacos 管不了。
- 二者不重叠：Nacos 解决不了「alice 在 node-A 还是 node-B」，那必须查 Redis 路由表。

### 启用方式

```bash
# ① 起 Redis + Nacos（docker-compose 已附带 standalone 版 Nacos）
docker compose up -d

# ② 编译
./mvnw -DskipTests package

# ③ 启动两个节点，开启 Nacos 动态发现
IM_GRPC_PORT=9090 IM_NACOS_ENABLED=true IM_NACOS_ADDR=127.0.0.1:8848 \
  mvnw spring-boot:run
IM_GRPC_PORT=9091 IM_NACOS_ENABLED=true IM_NACOS_ADDR=127.0.0.1:8848 \
  mvnw spring-boot:run --server.port=8081
```

启动后可在 `http://localhost:8848/nacos`(默认账号 nacos/nacos) 的「服务管理 → 服务列表」
看到 `im-grpc-nodes` 下有 2 个健康实例，IP 即各节点真实地址。消息转发链路变为：

```
节点1 → Redis 查 userId→nodeId → Nacos 解析 nodeId→健康 ip:port → gRPC 转发 → 节点2 → 推 B
```

### 关键配置（application.yml 的 im.nacos.*）

| 配置 | 环境变量 | 默认 | 说明 |
|------|----------|------|------|
| `im.nacos.enabled` | `IM_NACOS_ENABLED` | `false` | 是否启用；关闭时转发直接回退为用 nodeId，零侵入 |
| `im.nacos.server-addr` | `IM_NACOS_ADDR` | `127.0.0.1:8848` | Nacos Server 地址 |
| `im.nacos.service-name` | — | `im-grpc-nodes` | 所有 IM 节点注册到的服务名 |
| `im.nacos.namespace` | `IM_NACOS_NAMESPACE` | (空/public) | 命名空间，多环境隔离用 |

### 容错

- Nacos 未启用、或 Nacos 连不上 / 查不到健康实例时，`NacosNodeDiscovery.resolveTarget()`
  **自动回退为直接用 nodeId(host:port)**，不影响同机 / 单机 demo。
- 节点优雅关闭时会 `deregisterInstance`；异常宕机则由 Nacos 心跳超时自动剔除。

> K8s 部署见第 8 节，`k8s/im-node.yaml` 已通过环境变量 `IM_NACOS_ENABLED` / `IM_NACOS_ADDR`
> 打开 Nacos 注册（把 `IM_NACOS_ADDR` 改成你的 Nacos Service 地址即可）。

### 节点不健康时的 Redis 路由清理（关键）

`NacosNodeDiscovery` 会 **订阅 Nacos 实例变更事件**。当某节点被 Nacos 标记为不健康 / 已注销：

1. 其它**健康节点**通过 diff 健康集合，识别出「掉队」的 nodeId；
2. 调用 `UserNodeRegistry.removeNodeBindings(nodeId)`，扫描 Redis 把 `im:user:node:{userId}`
   中值等于该死节点的条目全部删除；
3. 这些用户随即被视为「离线」，后续发给他们的消息不再转发到死节点，而是落入离线队列；
4. 节点恢复、用户重连后路由自动重建。

> 设计要点：清理动作发生在**其它健康节点**上（死节点自己已无法执行）。本地节点自身的路由不会被误删。
> `removeNodeBindings` 用 `keys()` 全量扫描，规模大时建议改用 `SCAN` 游标遍历。

---

## 8. Kubernetes 部署（自动获取真实 pod IP，无需写死）

容器化后 pod IP 是运行时才分配的，不能预先写死 `IM_GRPC_HOST`。本工程通过
`NodeAddressResolver` + K8s **Downward API** 自动把 pod 真实 IP 注入并广播到 Redis 路由表，
gRPC Server 绑定 `0.0.0.0` 保证 pod 网络内可达。

清单见 `k8s/`：

- `k8s/redis.yaml`：Redis Deployment + Service（也可改用已有 Redis）
- `k8s/im-node.yaml`：StatefulSet（replicas=3）+ Headless Service + Downward API

部署步骤：

```bash
# ① 构建镜像并推到你的仓库（记得改 im-node.yaml 里的 image 地址）
docker build -t your-registry/im-node:latest .
docker push your-registry/im-node:latest

# ② 部署 Redis 与 IM 节点
kubectl apply -f k8s/redis.yaml
kubectl apply -f k8s/im-node.yaml

# ③ 观察：3 个 pod 各自拿到真实 IP，并自动注册到 Redis（节点 ID = POD_IP:9090）
kubectl get pods -o wide
```

要点：

- 每个 pod 通过 `status.podIP` 拿到真实 IP，节点 ID 自动 = `POD_IP:9090`，其他 pod 直接 dial 该 IP（标准 CNI 下 pod IP 跨节点可达）。
- `StatefulSet + Headless Service` 还提供稳定 DNS（`im-node-0.im-service.default.svc.cluster.local`），即使 pod 重建 IP 变化，`POD_NAME` 也能解析到新 IP（作为 `POD_IP` 缺失时的兜底）。
- 同 StatefulSet 内所有 pod 都用同一 `containerPort 9090`（各自独立网络命名空间，不冲突）。
- 外部要连某个用户的 WebSocket，需要按 userId 路由的接入层/Ingress（IM 通常前置一层按用户查路由表的网关），本 demo 不内置。
- **TLS**：集群内 pod 网络默认私有，明文可用；跨集群/公网再按 `application-tls.yml` 启用，且证书 SAN 需包含 `POD_IP` 或 Headless DNS 名。

## 9. 代码结构

```
im-cluster/
├── pom.xml                      # Boot 4.1 父工程 + grpc/protobuf 插件 + nacos-client
├── docker-compose.yml           # 一键起 Redis + Nacos(standalone)
├── scripts/
│   ├── run-two-nodes.sh         # 一键起两个节点
│   └── gen-certs.sh             # 生成 TLS 自签名证书
├── k8s/                         # K8s 部署清单（StatefulSet + Headless Service）
└── src/main/
    ├── proto/im_forward.proto   # 节点间转发 RPC 定义
    ├── java/com/example/im/
    │   ├── ImClusterApplication.java
    │   ├── connection/
    │   │   ├── ConnectionManager.java     # 本节点 用户→WebSocket 会话
    │   │   ├── UserNodeRegistry.java      # Redis 路由表 + 心跳
    │   │   ├── NodeAddressResolver.java   # 自动探测可达地址（K8s POD_IP 等）+ getRegisterIp()
    │   │   └── OfflineMessageStore.java    # 离线消息（Redis 列表）
    │   ├── discovery/
    │   │   └── NacosNodeDiscovery.java     # Nacos-Client：节点 gRPC 地址注册 + 动态发现
    │   ├── grpc/
    │   │   ├── ForwardService.java        # @GrpcService 接收转发
    │   │   └── ForwardClient.java         # 按地址缓存 ManagedChannel 转发
    │   ├── route/
    │   │   ├── MessageEnvelope.java       # 消息信封
    │   │   └── MessageRouter.java         # 路由决策（本地/跨节点/离线），跨节点前经 Nacos 解析地址
    │   ├── ws/
    │   │   ├── WebSocketConfig.java       # /ws?userId=xxx 长连接入口
    │   │   └── ImWebSocketHandler.java    # 连接/收发/补发离线
    │   └── controller/TestSendController.java  # POST /send 测试入口
    └── resources/
        ├── application.yml                 # 默认（明文 + im.nacos.* 配置）
        ├── application-tls.yml             # TLS profile
        └── static/index.html               # 前端测试页
```

---

## 10. 已知坑（已规避）

- **Jackson 3**：Boot 4.1 升级到 Jackson 3，`ObjectMapper`/`JsonNode` 包名从
  `com.fasterxml.jackson.databind` 变为 `tools.jackson.databind`，代码已改正。
- **gRPC / protobuf 版本**：pom 中引用 Boot BOM 管理的 `${grpc-java.version}` / `${protobuf-java.version}`，
  不要自行覆盖，避免与 BOM 内 netty 版本冲突。
- **Redis 必需**：应用启动时即向 Redis 注册节点路由，无 Redis 会启动失败。
```
