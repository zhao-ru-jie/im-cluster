#!/usr/bin/env bash
# 一键启动「两个 IM 节点」演示跨节点转发。
# 用法:  ./scripts/run-two-nodes.sh
# 前置: 已设置 JAVA_HOME 指向 JDK 21+；Redis 已通过 docker compose 启动。
set -e

cd "$(dirname "$0")/.."

if [ -z "$JAVA_HOME" ]; then
  echo "请先设置 JAVA_HOME 指向 JDK 21+，例如:"
  echo "  export JAVA_HOME=/path/to/jdk21"
  exit 1
fi

echo "==> 正在构建 (./mvnw package) ..."
./mvnw -q -DskipTests package

JAR=target/im-cluster-1.0.0.jar
if [ ! -f "$JAR" ]; then
  echo "构建失败：未找到 $JAR"
  exit 1
fi

mkdir -p logs
JAVA="$JAVA_HOME/bin/java"

echo "==> 启动 node-1 (HTTP :8080, gRPC :9090, nodeId=127.0.0.1:9090)"
IM_GRPC_PORT=9090 IM_GRPC_HOST=127.0.0.1 \
  "$JAVA" -jar "$JAR" --server.port=8080 > logs/node1.log 2>&1 &
PID1=$!

echo "==> 启动 node-2 (HTTP :8081, gRPC :9091, nodeId=127.0.0.1:9091)"
IM_GRPC_PORT=9091 IM_GRPC_HOST=127.0.0.1 \
  "$JAVA" -jar "$JAR" --server.port=8081 > logs/node2.log 2>&1 &
PID2=$!

echo "node-1 pid=$PID1, node-2 pid=$PID2"
echo "浏览器打开: http://localhost:8080/  (用户 alice)  和  http://localhost:8081/  (用户 bob)"
echo "Ctrl-C 停止所有节点"

cleanup() {
  echo ""
  echo "==> 正在停止节点 ..."
  kill "$PID1" "$PID2" 2>/dev/null || true
}
trap cleanup EXIT INT TERM
wait
