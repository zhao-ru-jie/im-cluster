package com.github.im.storage.jdbc.config;

import com.baomidou.mybatisplus.autoconfigure.ConfigurationCustomizer;
import com.github.im.server.group.GroupMemberRepository;
import com.github.im.server.group.GroupRepository;
import com.github.im.server.relation.FriendRequestRepository;
import com.github.im.server.relation.RelationRepository;
import com.github.im.server.shard.ShardRouter;
import com.github.im.server.message.MessageRepository;
import com.github.im.storage.jdbc.JdbcFriendRequestRepository;
import com.github.im.storage.jdbc.JdbcGroupMemberRepository;
import com.github.im.storage.jdbc.JdbcGroupRepository;
import com.github.im.storage.jdbc.JdbcMessageRepository;
import com.github.im.storage.jdbc.JdbcRelationRepository;
import com.github.im.storage.jdbc.mapper.FriendRequestMapper;
import com.github.im.storage.jdbc.mapper.GroupMemberMapper;
import com.github.im.storage.jdbc.mapper.GroupMapper;
import com.github.im.storage.jdbc.mapper.MessageDeleteMapper;
import com.github.im.storage.jdbc.mapper.MessageMapper;
import com.github.im.storage.jdbc.mapper.RelationMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.annotation.MapperScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import javax.sql.DataSource;

/**
 * MySQL 存储的手工装配（`im.storage=jdbc` 时生效），持久层用 MyBatis-Plus。
 *
 * <p>为什么不用 {@code spring-boot-starter-jdbc} 的自动装配：它会在<b>没有任何 DSN</b> 的情况下
 * 也去自动装配 DataSource，导致「只想用 Redis 的部署」启动失败。这里保持显式条件装配：
 * 没配 {@code im.storage=jdbc} 就完全不碰数据库，三种形态（redis / jdbc / mongodb）可以在同一份 jar 里共存。
 * 那个「classpath 上有就生效」的连通性负担由部署入口的 {@code spring.autoconfigure.exclude} 关掉，
 * 见 im-server-boot / im-app 的 application.yml。
 *
 * <p>MyBatis-Plus 的自动装配（{@code MybatisPlusAutoConfiguration}）是
 * {@code @ConditionalOnSingleCandidate(DataSource.class)}，同样只在 {@link #imDataSource} 存在时生效，
 * 所以 redis 形态下不会凭空多出 SqlSessionFactory 之类的 Bean。两个需要在这里显式声明的是：
 * <ul>
 *   <li>{@link MapperScan}：Mapper 在 {@code com.github.im.storage.jdbc.mapper} 下，
 *       不在部署入口的 {@code @SpringBootApplication} 包内（Boot 的自动扫描只覆盖入口包）；
 *       挂在条件装配类上意味着「不启用 MySQL 存储就不注册 Mapper」；</li>
 *   <li>{@link ConfigurationCustomizer}：把下划线列名到驼峰字段的映射<b>显式打开</b>，
 *       不依赖框架默认值（默认值会随版本变，列名对不上时表现为整行字段为 null，很难查）。</li>
 * </ul>
 *
 * <p>配置项：
 * <pre>
 * im.storage=jdbc
 * im.db.url=jdbc:mysql://host:3306/im?useSSL=false&amp;serverTimezone=Asia/Shanghai&amp;rewriteBatchedStatements=true
 * im.db.username=im
 * im.db.password=***
 * im.db.pool-size=20
 * im.db.init-schema=true    # 首次建表用；生产建议关掉，交给 Flyway/Liquibase
 * im.db.batch-enabled=true  # group commit：把并发到达的插入合并成一个事务（默认开）
 * </pre>
 *
 * <p>本类同时提供两个存储端口的 MySQL 实现：{@code MessageRepository}（im_message）与
 * {@code GroupMemberRepository}（im_group_member）。两者共用同一个连接池，
 * 保证「消息落库 + 群成员变更」在同一个事务边界内可用。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Configuration
@ConditionalOnProperty(name = "im.storage", havingValue = "jdbc")
@MapperScan("com.github.im.storage.jdbc.mapper")
public class JdbcStorageConfig {

    private static final Logger log = LoggerFactory.getLogger(JdbcStorageConfig.class);

    @Bean(destroyMethod = "close")
    public HikariDataSource imDataSource(@Value("${im.db.url}") String url,
                                         @Value("${im.db.username:root}") String username,
                                         @Value("${im.db.password:}") String password,
                                         @Value("${im.db.pool-size:200}") int poolSize) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(poolSize);
        config.setMinimumIdle(20);
        config.setConnectionTimeout(30000);
        config.setIdleTimeout(600000);
        config.setMaxLifetime(1800000);
        config.setKeepaliveTime(30000);
        config.setValidationTimeout(5000);
        config.setPoolName("im-db");
        // 启动时就校验连通性：DB 配错要立刻暴露，而不是等第一条消息
        config.setInitializationFailTimeout(5_000);
        log.info("[storage] MySQL 消息存储已启用: {} (pool={})", url, poolSize);
        return new HikariDataSource(config);
    }

    /** 显式声明列名映射与缓存策略，见类注释 */
    @Bean
    public ConfigurationCustomizer imMybatisConfigurationCustomizer() {
        return configuration -> {
            configuration.setMapUnderscoreToCamelCase(true);
            // 二级缓存默认关：消息表只按会话范围读、写完即读，开了只会多一层失效开销
            configuration.setCacheEnabled(false);
        };
    }

    @Bean
    public MessageRepository messageRepository(MessageMapper messageMapper,
                                               MessageDeleteMapper messageDeleteMapper,
                                               SqlSessionFactory sqlSessionFactory,
                                               DataSource dataSource,
                                               ShardRouter shardRouter,
                                               @Value("${im.db.init-schema:true}") boolean initSchema,
                                               @Value("${im.db.batch-enabled:true}") boolean batchEnabled) {
        if (initSchema) {
            ResourceDatabasePopulator populator = new ResourceDatabasePopulator(new ClassPathResource("db/schema.sql"));
            populator.execute(dataSource);
            log.info("[storage] 已执行 db/schema.sql（im.db.init-schema=false 可关闭）");
        }
        return new JdbcMessageRepository(messageMapper, messageDeleteMapper, sqlSessionFactory, batchEnabled,
                shardRouter);
    }

    @Bean
    public GroupMemberRepository groupMemberRepository(GroupMemberMapper groupMemberMapper) {
        return new JdbcGroupMemberRepository(groupMemberMapper);
    }

    /** 群实体（群资料 + 群级开关）；Redis 形态下由 {@code RedisGroupRepository} 提供 */
    @Bean
    public GroupRepository groupRepository(GroupMapper groupMapper) {
        return new JdbcGroupRepository(groupMapper);
    }

    /**
     * 关系链（好友 / 黑名单）。Redis 形态下由 {@code RedisRelationRepository} 提供，
     * 两者都实现同一端口，业务侧（{@code MessageAcl}）不感知介质。
     */
    @Bean
    public RelationRepository relationRepository(RelationMapper relationMapper) {
        return new JdbcRelationRepository(relationMapper);
    }

    /**
     * 好友申请（{@code im_friend_request}）。
     *
     * <p>建表语句在 {@code db/schema.sql} 里，跟随 {@code messageRepository} 的
     * {@code im.db.init-schema} 一起执行——申请列表为空时的表现与「表不存在」时的表现
     * 都是空列表，只能靠这行 DDL 保证不是后者。
     */
    @Bean
    public FriendRequestRepository friendRequestRepository(FriendRequestMapper friendRequestMapper) {
        return new JdbcFriendRequestRepository(friendRequestMapper);
    }
}
