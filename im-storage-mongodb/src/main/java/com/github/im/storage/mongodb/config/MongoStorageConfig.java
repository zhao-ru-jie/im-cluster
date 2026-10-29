package com.github.im.storage.mongodb.config;

import com.github.im.server.group.GroupMemberRepository;
import com.github.im.server.group.GroupRepository;
import com.github.im.server.message.MessageRepository;
import com.github.im.server.relation.FriendRequestRepository;
import com.github.im.server.relation.RelationRepository;
import com.github.im.storage.mongodb.MongoFriendRequestRepository;
import com.github.im.storage.mongodb.MongoGroupMemberRepository;
import com.github.im.storage.mongodb.MongoGroupRepository;
import com.github.im.storage.mongodb.MongoMessageRepository;
import com.github.im.storage.mongodb.MongoRelationRepository;
import com.github.im.storage.mongodb.MongoSchema;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.concurrent.TimeUnit;

/**
 * MongoDB 存储的手工装配（{@code im.storage=mongodb} 时生效）。
 *
 * <h2>为什么不用 Spring Boot 的 Mongo 自动装配</h2>
 *
 * <p>{@code MongoAutoConfiguration} / {@code DataMongoAutoConfiguration} 是
 * 「classpath 上有就生效」，与 {@code im.storage} 无关：只要 im-storage-mongodb 在依赖里，
 * 一个只想用 Redis 的部署也会凭空多出一个指向 {@code localhost:27017} 的 MongoClient，
 * 以及一个会因为连不上而报 DOWN 的 Mongo 健康指示器——后者会经 Actuator 聚合传到
 * {@code grpc.health.v1}，直接让网关心跳把实例摘掉。
 *
 * <p>所以两个部署入口都在 {@code application.yml} 里静态排除了那几个自动装配，
 * 由本类按 {@code im.storage=mongodb} 显式创建客户端，与
 * {@code com.github.im.storage.jdbc.JdbcStorageConfig} 自建 HikariDataSource 同构：
 * 选哪个介质、以及「谁会建 Bean」，都收敛到一处。
 *
 * <h2>启动即验连通性</h2>
 *
 * <p>创建客户端后先跑一次 {@code ping}：地址/认证配错要像 MySQL 形态那样<b>在启动时报错</b>，
 * 而不是等到第一条消息才炸（那时客户端已经拿到 ack 了）。索引同样在启动阶段建，
 * 见 {@code MongoSchema}。
 *
 * <p>客户端与模板两个 Bean 都标了 {@code @ConditionalOnMissingBean}：宿主（或测试）已经提供了
 * {@code MongoClient} / {@code MongoTemplate} 时就不重复创建——这样「四个端口挂在哪套模板上」
 * 可以在没有 Mongo 实例的情况下被单独验证（见 {@code MongoStorageConfigTest}）。
 *
 * <h2>配置项</h2>
 * <pre>
 * im.storage=mongodb
 * im.mongo.uri=mongodb://user:pass@host:27017/?authSource=admin&replicaSet=rs0
 * im.mongo.database=im
 * im.mongo.init-indexes=true    # 首次部署建索引用；索引变更也可以交由 DBA 脚本管理
 * </pre>
 *
 * <p>{@code MongoHealthContributorAutoConfiguration} 没有排除：它是
 * {@code @ConditionalOnBean(MongoClient)}，只在 mongodb 形态下生效，
 * 正好给到「Mongo 挂了就别再给我发消息」的探针能力（依赖它的正是本类创建的客户端）。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Configuration
@ConditionalOnProperty(name = "im.storage", havingValue = "mongodb")
public class MongoStorageConfig {

    private static final Logger log = LoggerFactory.getLogger(MongoStorageConfig.class);

    /**
     * 客户端（线程安全、内部自带连接池，整个进程共用一个）。
     *
     * <p>只做一次 {@code ping} 来确认「地址可达 + 认证通过」；它同时把
     * 「配置写错」的暴露时间点从「第一条消息」提前到「启动」。
     *
     * <p>启动日志会把<b>地址与三个超时</b>打出来（不含账号密码）：Mongo 相关的报错
     * （{@code MongoSocketReadTimeoutException} / {@code MongoTimeoutException}）几乎都取决于
     * 「到底连的谁、超时配了多少」，而 uri 本身不能回显——落地一行摘要，比在故障时猜配置省事得多。
     * 尤其是 {@code socketTimeout=0}：那意味着<b>驱动永远不会因为读不到响应而失败</b>，
     * 存储假死时线程会一直等，只有从这行日志才看得出来。
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean(MongoClient.class)
    public MongoClient imMongoClient(@Value("${im.mongo.uri}") String uri) {
        ConnectionString connectionString = new ConnectionString(uri);
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();
        MongoClient client = MongoClients.create(settings);
        client.getDatabase("admin").runCommand(new Document("ping", 1));
        log.info("[storage] MongoDB 消息存储已启用（ping 成功）hosts={} connectTimeout={}ms "
                        + "socketTimeout={}ms serverSelectionTimeout={}ms",
                connectionString.getHosts(),
                settings.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS),
                settings.getSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS),
                settings.getClusterSettings().getServerSelectionTimeout(TimeUnit.MILLISECONDS));
        return client;
    }

    @Bean
    @ConditionalOnMissingBean(MongoTemplate.class)
    public MongoTemplate imMongoTemplate(MongoClient client,
                                         @Value("${im.mongo.database:im}") String database,
                                         @Value("${im.mongo.init-indexes:true}") boolean initIndexes) {
        MongoTemplate template = new MongoTemplate(client, database);
        if (initIndexes) {
            MongoSchema.ensureIndexes(template);
        }
        log.info("[storage] MongoDB database={} initIndexes={}", database, initIndexes);
        return template;
    }

    @Bean
    public MessageRepository messageRepository(MongoTemplate mongoTemplate) {
        return new MongoMessageRepository(mongoTemplate);
    }

    @Bean
    public GroupRepository groupRepository(MongoTemplate mongoTemplate) {
        return new MongoGroupRepository(mongoTemplate);
    }

    @Bean
    public GroupMemberRepository groupMemberRepository(MongoTemplate mongoTemplate) {
        return new MongoGroupMemberRepository(mongoTemplate);
    }

    @Bean
    public RelationRepository relationRepository(MongoTemplate mongoTemplate) {
        return new MongoRelationRepository(mongoTemplate);
    }

    /** 好友申请（流程）：与关系边分属两个集合，理由见 FriendRequestEntity 注释 */
    @Bean
    public FriendRequestRepository friendRequestRepository(MongoTemplate mongoTemplate) {
        return new MongoFriendRequestRepository(mongoTemplate);
    }
}
