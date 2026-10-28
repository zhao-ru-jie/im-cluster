package com.github.im.serverboot;

import com.github.im.server.message.MessageRepository;
import com.github.im.storage.redis.RedisMessageRepository;
import com.mongodb.client.MongoClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;

import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 {@code im.storage} 真的决定介质，而不是「所有介质的 Bean 一起上场」。
 *
 * <h2>为什么值得起一个真实上下文</h2>
 *
 * <p>本进程的 classpath 上同时有 Redis、MyBatis/Hikari、Spring Data MongoDB 三套依赖，
 * 而它们的自动装配都是「classpath 上有就生效」。默认形态（{@code im.storage} 缺省 = redis）
 * 一旦漏排某套自动装配，后果不是启动失败，而是<b>多出一个没人用的连接</b>：
 * Hikari 会因为找不到 DSN 直接启动失败（好查），Mongo 客户端则会安静地连不上
 * {@code localhost:27017}，再经 Actuator 聚合把健康状态拉成 DOWN——网关据此把实例摘掉，
 * 表现是「服务活着但没人给它发消息」。所以这里断言的是<b>缺席</b>。
 *
 * <p>与 {@link GrpcHealthEndpointTest} 用同一份配置启动，Spring 测试上下文缓存会复用同一个
 * 上下文，不会额外多起一个进程。
 */
@SpringBootTest(classes = ImServerApplication.class, properties = {
        // 固定 Redis 形态：本用例断言的是「只装配被选中的那一种介质」，
        // 跟着 application.yml 的默认值走会让它依赖部署侧配置（甚至依赖一台 MySQL/Mongo）。
        "im.storage=redis",
        "spring.grpc.server.port=19291",
        "im.node.grpc-port=19291",
        "server.port=0",
        "im.nacos.enabled=false",
        "spring.grpc.server.health.schedule.delay=100ms",
        "spring.grpc.server.health.schedule.period=200ms"
})
class StorageWiringTest {

    @Autowired
    private ApplicationContext context;

    @Test
    @DisplayName("im.storage=redis 时装配的是 Redis 实现")
    void redisIsTheSelectedMedium() {
        assertInstanceOf(RedisMessageRepository.class, context.getBean(MessageRepository.class),
                "四种端口应全部由 Redis 实现提供（见 RedisStorageConfig）");
    }

    @Test
    @DisplayName("Redis 形态下不创建 MySQL 连接池与 Mongo 客户端")
    void noOtherMediumFootprint() {
        assertTrue(context.getBeansOfType(DataSource.class).isEmpty(),
                "redis 形态不该建 DataSource：im-storage-jdbc 在 classpath 上不等于要用它");
        assertTrue(context.getBeansOfType(MongoClient.class).isEmpty(),
                "redis 形态不该建 MongoClient：它会安静地连不上 localhost:27017，把健康状态拖成 DOWN");
        assertTrue(context.getBeansOfType(MongoTemplate.class).isEmpty(),
                "redis 形态不该建 MongoTemplate（同上，MongoAutoConfiguration 应已在 yml 里排除）");
    }
}
