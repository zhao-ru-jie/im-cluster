package com.github.im.storage.redis;

import com.github.im.contract.RedisKeys;
import org.opentest4j.TestAbortedException;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * 测试专用的 Redis 连接（不起 Spring 上下文，直连 Lettuce）。
 *
 * <p>与 {@code com.github.im.server.message.TestRedis} 是同一套约定，这里是本模块的精简副本：
 * 那个类在 im-server 的 <b>test</b> 源码里且是包私有，跨模块拿不到（也不该为此产出 test-jar）。
 * 本模块只用到「取模板 / 造隔离的会话 ID / 拼消息键」三件事。
 *
 * <h2>对宿主环境的边界</h2>
 *
 * <p>只碰自己造的 key：会话 ID 带随机 UUID（{@code im:conv:msgs:t:<uuid>}），用完即删；
 * <b>绝不</b> FLUSH、不扫前缀批量删——开发机上的 Redis 里还有别人的数据。
 *
 * <p>Redis 不可用时抛 {@link TestAbortedException}：整个用例记为 skipped 而不是 failed，
 * 让没有 Redis 的环境（CI 纯编译、他人机器）也能跑 {@code mvn test}。
 */
final class TestRedis {

    private static final String PROBE_KEY = "im:test:probe";

    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate template;

    private TestRedis() {
    }

    static synchronized StringRedisTemplate template() {
        if (template == null) {
            RedisStandaloneConfiguration config = new RedisStandaloneConfiguration(
                    System.getProperty("im.test.redis.host", "127.0.0.1"),
                    Integer.getInteger("im.test.redis.port", 6379));
            factory = new LettuceConnectionFactory(config);
            factory.afterPropertiesSet();
            template = new StringRedisTemplate(factory);
        }
        try {
            template.hasKey(PROBE_KEY);
        } catch (RuntimeException e) {
            throw new TestAbortedException(
                    "本地 Redis 不可用（可用 -Dim.test.redis.host/-Dim.test.redis.port 指定），跳过 Redis 消息存储测试: "
                            + e.getMessage(), e);
        }
        return template;
    }

    /** 会话 ID 一律带随机后缀，保证测试之间、测试与开发数据之间互不干扰 */
    static String conversationId() {
        return "t:" + java.util.UUID.randomUUID();
    }

    static String messagesKey(String conversationId) {
        return RedisKeys.convMessages(conversationId);
    }
}
