# 多阶段构建：构建阶段用 JDK 25 编译，运行阶段用 JRE 25 跑 fat jar
# 注意：Spring Boot 4.1 + 虚拟线程需要 JDK 21+，这里固定用 25

# ---- 构建阶段 ----
FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
# 离线优先，首次会下载依赖（建议挂载 ~/.m2 缓存加速）
RUN mvn -B -DskipTests package

# ---- 运行阶段 ----
FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar
EXPOSE 8080 9090
ENTRYPOINT ["java", "-jar", "app.jar"]
