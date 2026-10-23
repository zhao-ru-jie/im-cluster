package com.github.im.server.message;

import com.github.im.contract.RedisKeys;
import org.opentest4j.TestAbortedException;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * 测试专用的 Redis 连接（不起 Spring 上下文，直连 Lettuce）。
 *
 * <h2>为什么这些用例必须用真实 Redis</h2>
 *
 * <p>seq 发号的正确性<b>几乎全部</b>落在 Lua 脚本的原子性上：{@code GET} 与 {@code INCR} 之间
 * 是否可能被其它实例插入、计数器低于下界时是否真的先抬后增。用 mock 替掉 Redis 只能验证
 * 「Java 侧按顺序调了几次」，验证不了这两点，等于把最该测的部分测丢了。
 *
 * <h2>对宿主环境的边界</h2>
 *
 * <p>只碰自己造的 key：会话 ID 带随机 UUID（{@code im:seq:t:<uuid>} / {@code im:conv:msgs:t:<uuid>}），
 * 用完即删；<b>绝不</b> FLUSH、不扫前缀批量删——开发机上的 Redis 里还有别人的数据。
 *
 * <p>Redis 不可用时抛 {@link TestAbortedException}：整个用例标记为 skipped 而不是 failed，
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
                    "本地 Redis 不可用（可用 -Dim.test.redis.host/-Dim.test.redis.port 指定），跳过发号器集成测试: "
                            + e.getMessage(), e);
        }
        return template;
    }

    /** 会话 ID 一律带随机后缀，保证测试之间、测试与开发数据之间互不干扰 */
    static String conversationId() {
        return "t:" + java.util.UUID.randomUUID();
    }

    static String seqKey(String conversationId) {
        return RedisKeys.SEQ_PREFIX + conversationId;
    }

    static String messagesKey(String conversationId) {
        return RedisKeys.CONV_MESSAGES_PREFIX + conversationId;
    }
}
