#!/usr/bin/env bash
# 生成演示用自签名证书（gRPC TLS 用）。
# 生成后位置: im-app/src/main/resources/grpc/server.crt 与 server.key
# 启动 TLS: 给节点加 --spring.profiles.active=tls 且 IM_GRPC_TLS=true
set -e
cd "$(dirname "$0")/.."
mkdir -p im-app/src/main/resources/grpc
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout im-app/src/main/resources/grpc/server.key \
  -out im-app/src/main/resources/grpc/server.crt \
  -days 365 -subj "/CN=localhost"
echo "证书已生成: im-app/src/main/resources/grpc/server.crt / server.key"
echo "提示: 自签名证书仅用于本地演示，生产环境请使用受信任 CA 签发的证书。"
