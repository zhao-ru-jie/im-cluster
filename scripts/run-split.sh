#!/usr/bin/env bash
# 一键启动「阶段 3 拆分拓扑」：长连接服务 + IM 服务 两个独立进程。
#
#   网关   : HTTP 8080（/admin、/actuator）、Netty WS 8081、Netty TCP 9092、gRPC 9090
#            （实例 ID = 127.0.0.1:9090）
#   IM 服务 : HTTP 8088、gRPC 9091
#
# 用法:  ./scripts/run-split.sh
# 前置:  JAVA_HOME 指向 JDK 21+；Redis 已启动（docker compose up -d）；先 ./mvnw package
set -e

cd "$(dirname "$0")/.."

if [ -z "$JAVA_HOME" ]; then
  echo "请先设置 JAVA_HOME 指向 JDK 21+，例如: export JAVA_HOME=/path/to/jdk21"
  exit 1
fi

SERVER_JAR=im-server-boot/target/im-server-boot-1.0.0.jar
GATEWAY_JAR=im-gateway-boot/target/im-gateway-boot-1.0.0.jar
for jar in "$SERVER_JAR" "$GATEWAY_JAR"; do
  if [ ! -f "$jar" ]; then
    echo "未找到 $jar，请先执行: ./mvnw -DskipTests package"
    exit 1
  fi
done

mkdir -p logs
JAVA="$JAVA_HOME/bin/java"

echo "==> 启动 IM 服务 (HTTP :8088, gRPC :9091)"
IM_GRPC_PORT=9091 SERVER_PORT=8088 \
  "$JAVA" -jar "$SERVER_JAR" > logs/im-server.log 2>&1 &
PID_SERVER=$!

# 等 gRPC 入口就绪，避免网关一上来就 503
sleep 6

echo "==> 启动长连接服务 (HTTP :8080, WS :8081, TCP :9092, gRPC :9090)，上行 -> 127.0.0.1:9091"
IM_GRPC_PORT=9090 SERVER_PORT=8080 IM_WS_PORT=8081 IM_TCP_PORT=9092 \
  IM_GRPC_HOST=127.0.0.1 IM_SERVER_ADDR=127.0.0.1:9091 \
  "$JAVA" -jar "$GATEWAY_JAR" > logs/im-gateway.log 2>&1 &
PID_GATEWAY=$!

echo "server pid=$PID_SERVER, gateway pid=$PID_GATEWAY"
echo "长连接: ws://localhost:8081/ws?userId=alice&deviceId=web  |  TCP: localhost:9092（首帧 type=connect）"
echo "观测:  curl -s localhost:8080/actuator/prometheus | grep gateway_connections_active"
echo "摘流:  curl -X POST 'localhost:8080/admin/drain?reason=manual'"
echo "Ctrl-C 停止所有进程"

cleanup() {
  echo ""
  echo "==> 先给网关发 SIGTERM 触发优雅摘流，再停 IM 服务 ..."
  kill "$PID_GATEWAY" 2>/dev/null || true
  sleep 3
  kill "$PID_SERVER" 2>/dev/null || true
}
trap cleanup EXIT INT TERM
wait
