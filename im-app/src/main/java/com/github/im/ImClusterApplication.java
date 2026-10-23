package com.github.im;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 单体装配入口：同一进程内同时装配
 * 长连接服务（{@code com.github.im.gateway}）与 IM 服务（{@code com.github.im.server}）。
 *
 * <p>两者之间只通过 {@code com.github.im.contract} 的端口接口通信
 * （{@code MessageIngress} / {@code PushPort}），因此后续把它们拆成两个独立进程时，
 * 只需要替换端口实现并各自加一个启动类，业务代码无需改动。
 *
 * <p>端口分工：Tomcat 占 {@code server.port}（默认 8080，只服务 REST/actuator/测试页面），
 * 长连接由 {@code NettyGatewayServer} 在独立端口提供（WebSocket 8081、原生 TCP 9092）。
 * 两个端口必须错开——同一进程内两种容器各自 bind，不会互相复用。
 *
 * <p>新增 {@code im-gateway-boot}、{@code im-server-boot} 两个独立入口模块，
 * 本模块届时仅保留用于本地联调与回归对比。
 */
@EnableScheduling
@SpringBootApplication
public class ImClusterApplication {

    public static void main(String[] args) {
        SpringApplication.run(ImClusterApplication.class, args);
    }
}
