package com.github.im.storage.jdbc;

import com.baomidou.mybatisplus.autoconfigure.ConfigurationCustomizer;
import com.github.im.server.group.GroupMemberRepository;
import com.github.im.server.message.MessageRepository;
import com.github.im.storage.jdbc.mapper.GroupMemberMapper;
import com.github.im.storage.jdbc.mapper.MessageMapper;
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
 * 没配 {@code im.storage=jdbc} 就完全不碰数据库，两种存储形态可以在同一份 jar 里共存。
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
                                         @Value("${im.db.pool-size:20}") int poolSize) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(poolSize);
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
                                               SqlSessionFactory sqlSessionFactory,
                                               DataSource dataSource,
                                               @Value("${im.db.init-schema:true}") boolean initSchema,
                                               @Value("${im.db.batch-enabled:true}") boolean batchEnabled) {
        if (initSchema) {
            ResourceDatabasePopulator populator = new ResourceDatabasePopulator(new ClassPathResource("db/schema.sql"));
            populator.execute(dataSource);
            log.info("[storage] 已执行 db/schema.sql（im.db.init-schema=false 可关闭）");
        }
        return new JdbcMessageRepository(messageMapper, sqlSessionFactory, batchEnabled);
    }

    @Bean
    public GroupMemberRepository groupMemberRepository(GroupMemberMapper groupMemberMapper) {
        return new JdbcGroupMemberRepository(groupMemberMapper);
    }
}
