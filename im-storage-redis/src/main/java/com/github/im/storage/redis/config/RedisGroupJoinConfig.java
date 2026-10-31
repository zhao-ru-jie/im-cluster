package com.github.im.storage.redis.config;

import com.github.im.server.group.GroupJoinStore;
import com.github.im.storage.redis.RedisGroupJoinStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;

/**
 * 入群申请 / 邀请码的装配。
 *
 * <h2>为什么单独一个配置类，而且<b>没有</b> {@code @ConditionalOnProperty}</h2>
 *
 * <p>{@link RedisStorageConfig} 整体挂在 {@code im.storage=redis} 下，因为那四个端口存的是
 * <b>领域真相</b>，换 MySQL / Mongo 就该换实现。但入群申请与邀请码是<b>短生命周期的协作状态</b>
 * （默认 7 天就过期），过期即无意义——和离线队列、客户端水位、seq 发号同类，那些在这套系统里
 * 一律只落 Redis，不随 {@code im.storage} 切换。
 *
 * <p>所以这里故意不受 {@code im.storage} 约束：{@code im.storage=jdbc} 的部署同样需要它，
 * 少了这个 Bean，{@code GroupService} 直接起不来。反过来把它塞进 {@link RedisStorageConfig}
 * 会造成一个很难查的问题——「换成 MySQL 之后入群申请功能凭空消失」。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Configuration
public class RedisGroupJoinConfig {

    /** 待审申请的存活时长：没人处理就自然消失，审批列表不会无限堆积 */
    private final Duration requestTtl;
    /** 长期有效（{@code expiresAt=0}）的邀请码的兜底上限 */
    private final Duration inviteTtl;

    public RedisGroupJoinConfig(@Value("${im.group.join-request-ttl:7d}") Duration requestTtl,
                                @Value("${im.group.invite-ttl:30d}") Duration inviteTtl) {
        this.requestTtl = requestTtl;
        this.inviteTtl = inviteTtl;
    }

    @Bean
    public GroupJoinStore groupJoinStore(StringRedisTemplate redis, ObjectMapper objectMapper) {
        return new RedisGroupJoinStore(redis, objectMapper, requestTtl, inviteTtl);
    }
}
