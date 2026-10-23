# 多阶段构建：构建阶段用 JDK 25 编译多模块工程，运行阶段用 JRE 25 跑 fat jar
# 注意：Spring Boot 4.1 + 虚拟线程需要 JDK 21+，这里固定用 25
#
# 构建参数 APP_JAR 决定打哪个部署入口的镜像：
#   im-app           （阶段 1/2）网关 + 本地 IM 服务（单体装配，联调/回归用）
#   im-gateway-boot  （阶段 3）长连接服务独立部署
#   im-server-boot   （阶段 2）IM 服务独立部署
# 例：docker build --build-arg APP_JAR=im-server-boot  -t your-registry/im-server:latest  .
#     docker build --build-arg APP_JAR=im-gateway-boot -t your-registry/im-gateway:latest .

# ---- 构建阶段 ----
FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /app
COPY pom.xml .
COPY im-proto/pom.xml im-proto/
COPY im-rpc/pom.xml im-rpc/
COPY im-gateway/pom.xml im-gateway/
COPY im-server/pom.xml im-server/
COPY im-app/pom.xml im-app/
COPY im-gateway-boot/pom.xml im-gateway-boot/
COPY im-server-boot/pom.xml im-server-boot/
COPY im-proto/src im-proto/src
COPY im-rpc/src im-rpc/src
COPY im-gateway/src im-gateway/src
COPY im-server/src im-server/src
COPY im-app/src im-app/src
COPY im-gateway-boot/src im-gateway-boot/src
COPY im-server-boot/src im-server-boot/src
RUN mvn -B -DskipTests package

# ---- 运行阶段 ----
FROM eclipse-temurin:25-jre
ARG APP_JAR=im-app
WORKDIR /app
COPY --from=build /app/${APP_JAR}/target/${APP_JAR}-1.0.0.jar app.jar
# 网关: 8080(WS) / 9090(gRPC-Push)    IM 服务: 8088(HTTP) / 9091(gRPC-Ingress)
EXPOSE 8080 8088 9090 9091
ENTRYPOINT ["java", "-jar", "app.jar"]
