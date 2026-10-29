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
 * im.mongo.heartbeat-ms=5000    # 节点探测间隔；uri 里写了 heartbeatFrequencyMS 则以 uri 为准
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

    /** 启动期握手最多试几次；与 {@link #PING_BACKOFF_MS} 相乘即最长容忍的启动抖动时长 */
    private static final int PING_ATTEMPTS = 3;
    private static final long PING_BACKOFF_MS = 1_000;

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
    public MongoClient imMongoClient(@Value("${im.mongo.uri}") String uri,
                                     @Value("${im.mongo.heartbeat-ms:5000}") long heartbeatMs) {
        MongoClientSettings settings = buildSettings(uri, heartbeatMs);
        MongoClient client = MongoClients.create(settings);
        pingWithRetry(client, settings);
        log.info("[storage] MongoDB 消息存储已启用（ping 成功）hosts={} connectTimeout={}ms "
                        + "socketTimeout={}ms serverSelectionTimeout={}ms heartbeat={}ms",
                settings.getClusterSettings().getHosts(),
                settings.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS),
                settings.getSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS),
                settings.getClusterSettings().getServerSelectionTimeout(TimeUnit.MILLISECONDS),
                settings.getServerSettings().getHeartbeatFrequency(TimeUnit.MILLISECONDS));
        return client;
    }

    /**
     * 由 uri 与心跳配置构建设置（不连库，可在单元测试里断言）。
     *
     * <h2>为什么要把心跳从默认的 10s 改掉</h2>
     *
     * <p>{@code heartbeatFrequencyMS} 默认 10 秒，它决定「监控线程多久探一次节点」。
     * mongod 抖动一下（磁盘 fsync 卡顿、Windows 上前台进程被控制台 QuickEdit 挂起）之后，
     * 驱动要等下一次心跳才知道它回来了——<b>这 10 秒里所有请求仍然按
     * {@code MongoTimeoutException: Timed out while waiting for a server} 失败</b>，
     * 而用户在客户端看到的就是「明明已经恢复了却还在报错」。
     *
     * <p>默认 5s 是折中：恢复延迟减半，探测频率也只到不足以给 mongod 添负担的程度。
     * uri 里显式写了 {@code heartbeatFrequencyMS} 时以 uri 为准，免得「配置文件写了但不生效」。
     *
     * <p>注意参数挂在 {@code ServerSettings} 上而不是 {@code ClusterSettings}——驱动 5.x 挪过去过，
     * 用老写法编译直接报「找不到符号」。
     */
    static MongoClientSettings buildSettings(String uri, long heartbeatMs) {
        ConnectionString connectionString = new ConnectionString(uri);
        MongoClientSettings.Builder builder = MongoClientSettings.builder()
                .applyConnectionString(connectionString);
        boolean heartbeatFromUri = connectionString.getHeartbeatFrequency() != null;
        if (heartbeatMs > 0 && !heartbeatFromUri) {
            // 驱动 5.x 把心跳从 ClusterSettings 挪到了 ServerSettings，别再用 applyToClusterSettings
            builder.applyToServerSettings(b -> b.heartbeatFrequency(heartbeatMs, TimeUnit.MILLISECONDS));
        }
        return builder.build();
    }

    /**
     * 启动期握手：失败重试几次再放弃。
     *
     * <p>原来只 ping 一次，于是「mongod 重启中 / 磁盘刚被占满 / 上一次实例的连接还没回收」
     * 这类几秒级的抖动会直接让<b>整个进程起不来</b>，而它其实再等一两秒就通了。
     * 重试不放宽判据（3 次 × 5s 后仍然失败就抛），只是不再拿一次瞬时失败当结论。
     *
     * <p>失败信息里带上实际生效的地址与三个超时，并列出 Windows 上最常见的那几个原因——
     * 这条报错本身（{@code MongoSocketReadTimeoutException} 出现在握手阶段）几乎总被误读成
     * 「查询太慢」，但它的真正含义是「mongod 那一刻没在处理任何请求」。
     */
    static void pingWithRetry(MongoClient client, MongoClientSettings settings) {
        RuntimeException last = null;
        for (int attempt = 1; attempt <= PING_ATTEMPTS; attempt++) {
            try {
                client.getDatabase("admin").runCommand(new Document("ping", 1));
                if (attempt > 1) {
                    log.warn("[storage] MongoDB 握手在第 {} 次尝试后成功（前 {} 次失败，属启动期抖动）",
                            attempt, attempt - 1);
                }
                return;
            } catch (RuntimeException e) {
                last = e;
                log.warn("[storage] MongoDB 握手失败（第 {}/{} 次）：{}", attempt, PING_ATTEMPTS, e.getMessage());
                if (attempt < PING_ATTEMPTS) {
                    sleepQuietly(PING_BACKOFF_MS);
                }
            }
        }
        throw new IllegalStateException(diagnosis(settings), last);
    }

    private static String diagnosis(MongoClientSettings settings) {
        return "无法连接 MongoDB：hosts=" + settings.getClusterSettings().getHosts()
                + " connectTimeout=" + settings.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS) + "ms"
                + " socketTimeout=" + settings.getSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS) + "ms"
                + " serverSelectionTimeout="
                + settings.getClusterSettings().getServerSelectionTimeout(TimeUnit.MILLISECONDS) + "ms。"
                + "排查顺序：① mongod 是否在运行（Windows: Get-Process mongod / netstat -ano | findstr 27017）；"
                + "② 前台跑的 mongod 在控制台里被鼠标选中（QuickEdit）会挂起整个进程——在该窗口按 Esc 释放，"
                + "或把它装成 Windows 服务（mongod --install）彻底避免；"
                + "③ 磁盘是否被占满 / 杀软是否在实时扫描 dbpath。"
                + "注意「握手阶段 Read timed out」不是查询慢，而是 mongod 当时没有处理任何请求。";
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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
