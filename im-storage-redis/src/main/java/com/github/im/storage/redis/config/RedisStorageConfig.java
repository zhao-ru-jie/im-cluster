package com.github.im.storage.redis.config;

import com.github.im.server.group.GroupMemberRepository;
import com.github.im.server.group.GroupRepository;
import com.github.im.server.message.MessageRepository;
import com.github.im.server.relation.RelationRepository;
import com.github.im.storage.redis.RedisGroupMemberRepository;
import com.github.im.storage.redis.RedisGroupRepository;
import com.github.im.storage.redis.RedisMessageRepository;
import com.github.im.storage.redis.RedisRelationRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.databind.ObjectMapper;

/**
 * Redis 存储的手工装配（{@code im.storage=redis}，缺省即生效），与
 * {@code com.github.im.storage.jdbc.JdbcStorageConfig} 是同一层的开关。
 *
 * <h2>为什么用配置类而不是在实现上加 {@code @Component}</h2>
 *
 * <p>换介质这件事应该在一个地方看得见。以前 {@code RedisMessageRepository} 自己带
 * {@code @Component + @ConditionalOnProperty}，实现类和装配条件混在一起；现在两种介质都由各自的
 * {@code *StorageConfig} 集中声明，{@code im.storage} 的取值与「谁会建 Bean」是同一个文件里的同一行注解，
 * 排查「为什么启动时没有 MessageRepository」只需看这两个类。
 *
 * <h2>与 im-storage-jdbc 提供同一组端口</h2>
 *
 * <p>四个端口一一对应：{@code MessageRepository}（{@link RedisMessageRepository}）、
 * {@code GroupRepository}（{@link RedisGroupRepository}）、{@code GroupMemberRepository}
 * （{@link RedisGroupMemberRepository}）、{@code RelationRepository}（{@link RedisRelationRepository}）。
 * 业务侧（{@code MessageService} / {@code GroupService} / {@code MessageAcl}）只认端口，
 * 换介质不改一行业务代码。
 *
 * <h2>两个条件类互斥，不会出现两个实现</h2>
 *
 * <ul>
 *   <li>本类：{@code im.storage=redis}，或<b>根本没配</b>（{@code matchIfMissing = true}，历史默认值）；</li>
 *   <li>{@code JdbcStorageConfig}：{@code im.storage=jdbc}。</li>
 * </ul>
 *
 * <p>因此部署侧只需要决定一件事：要不要连 MySQL。不配就落在 Redis 上——这也是本地开发、
 * 联调、以及「只想跑起来看看」的最小依赖形态。
 *
 * <p>{@link StringRedisTemplate} 由 {@code spring-boot-starter-data-redis} 自动装配提供；
 * {@link ObjectMapper} 用 Boot 装配的那份（Jackson 3），消息体按 {@code StoredMessage} 的 JSON 存。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Configuration
@ConditionalOnProperty(name = "im.storage", havingValue = "redis", matchIfMissing = true)
public class RedisStorageConfig {

    @Bean
    public MessageRepository messageRepository(StringRedisTemplate redis, ObjectMapper objectMapper) {
        return new RedisMessageRepository(redis, objectMapper);
    }

    /** 群资料（群名 / 公告 / 全员禁言 / 已解散）：Hash 单字段读改写，避免并发覆盖整对象 */
    @Bean
    public GroupRepository groupRepository(StringRedisTemplate redis) {
        return new RedisGroupRepository(redis);
    }

    /** 群成员 + 反向索引（用户加入了哪些群）+ 角色 / 禁言 */
    @Bean
    public GroupMemberRepository groupMemberRepository(StringRedisTemplate redis) {
        return new RedisGroupMemberRepository(redis);
    }

    /** 关系链：好友双向写两份（准入是热路径 O(1) 查询），黑名单单向 */
    @Bean
    public RelationRepository relationRepository(StringRedisTemplate redis) {
        return new RedisRelationRepository(redis);
    }
}
