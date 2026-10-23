package com.github.im.gatewayboot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 长连接服务独立部署入口（阶段 3）。
 *
 * <p>本进程只做「连接」这一件事：
 * <ul>
 *   <li>维护长连接会话（Netty 双通道：WebSocket + 原生 TCP）、心跳续期、空闲清理、优雅摘流；</li>
 *   <li>写在线路由表 {@code im:gw:user:{userId}}；</li>
 *   <li>上行：gRPC {@code MessageIngress} 交给 IM 服务；下行：暴露 {@code DownstreamPush} 供 IM 服务回推。</li>
 * </ul>
 * 不含消息存储、会话业务、数据库访问——所以它的扩缩容只跟「连接数」有关，
 * 扩容判定看 {@code gateway_connections_active}，而不是 CPU。
 *
 * <p>扫描范围限定在网关包：IM 服务的实现类即使被误引入依赖也不会生效。
 */
@EnableScheduling
@SpringBootApplication(scanBasePackages = {"com.github.im.gateway", "com.github.im.gatewayboot"})
public class GatewayBootApplication {

    public static void main(String[] args) {
        SpringApplication.run(GatewayBootApplication.class, args);
    }
}
