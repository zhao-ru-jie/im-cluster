package com.github.im.storage.jdbc;

import com.github.im.server.group.GroupMemberRepository;
import com.github.im.server.message.MessageRepository;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import javax.sql.DataSource;

/**
 * MySQL 存储的手工装配（`im.storage=jdbc` 时生效）。
 *
 * <p>为什么不用 {@code spring-boot-starter-jdbc}：它会在<b>没有任何 DSN</b> 的情况下
 * 也去自动装配 DataSource，导致「只想用 Redis 的部署」启动失败。这里改成显式条件装配：
 * 没配 {@code im.db.url} 就完全不碰数据库，两种存储形态可以在同一份 jar 里共存。
 *
 * <p>配置项：
 * <pre>
 * im.storage=jdbc
 * im.db.url=jdbc:mysql://host:3306/im?useSSL=false&amp;serverTimezone=Asia/Shanghai&amp;rewriteBatchedStatements=true
 * im.db.username=im
 * im.db.password=***
 * im.db.pool-size=20
 * im.db.init-schema=true    # 首次建表用；生产建议关掉，交给 Flyway/Liquibase
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

    @Bean
    public JdbcTemplate imJdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean
    public MessageRepository messageRepository(JdbcTemplate imJdbcTemplate,
                                               DataSource dataSource,
                                               @Value("${im.db.init-schema:true}") boolean initSchema) {
        if (initSchema) {
            ResourceDatabasePopulator populator = new ResourceDatabasePopulator(new ClassPathResource("db/schema.sql"));
            populator.execute(dataSource);
            log.info("[storage] 已执行 db/schema.sql（im.db.init-schema=false 可关闭）");
        }
        return new JdbcMessageRepository(imJdbcTemplate);
    }

    @Bean
    public GroupMemberRepository groupMemberRepository(JdbcTemplate imJdbcTemplate) {
        return new JdbcGroupMemberRepository(imJdbcTemplate);
    }
}
