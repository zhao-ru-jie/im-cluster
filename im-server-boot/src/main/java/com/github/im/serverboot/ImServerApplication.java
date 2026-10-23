package com.github.im.serverboot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * IM 服务独立部署入口（阶段 2）。
 *
 * <p>与阶段 1 的 {@code im-app} 相比，本进程：
 * <ul>
 *   <li><b>不含</b> WebSocket 接入、会话表、在线路由表写入（这些在 im-gateway）；</li>
 *   <li><b>暴露</b> {@code MessageIngress} gRPC 服务端，供网关远程上行；</li>
 *   <li><b>使用</b> {@code RemoteGatewayPushPort} 把下行帧推到持有连接的网关实例。</li>
 * </ul>
 *
 * <p>因此它可以像普通无状态服务一样水平扩容、随意重启（重启会短暂丢失推送能力，
 * 但消息已落库 + 落离线，客户端重连拉齐即可）。
 *
 * <p>扫描范围只包含 IM 服务包与存储实现包，确保网关相关组件即使被误引入依赖也不会生效。
 * 存储实现包（im-storage-jdbc）必须显式列入：它的装配类只在 {@code im.storage=jdbc} 时才会创建
 * DataSource，默认 redis 形态下不产生任何数据库相关 Bean。
 */
@SpringBootApplication(scanBasePackages = {
        "com.github.im.server",
        "com.github.im.storage.jdbc",
        "com.github.im.serverboot"})
public class ImServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ImServerApplication.class, args);
    }
}
