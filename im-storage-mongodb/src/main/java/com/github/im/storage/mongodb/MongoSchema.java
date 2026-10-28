package com.github.im.storage.mongodb;

import com.github.im.storage.mongodb.entity.GroupEntity;
import com.github.im.storage.mongodb.entity.GroupMemberEntity;
import com.github.im.storage.mongodb.entity.MessageDeleteEntity;
import com.github.im.storage.mongodb.entity.MessageEntity;
import com.github.im.storage.mongodb.entity.RelationEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.core.TypeInformation;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolver;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolver.IndexDefinitionHolder;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * MongoDB 的「建表脚本」：把实体上的索引注解变成真实索引
 * （{@code im.mongo.init-indexes=true} 时在启动阶段执行）。
 *
 * <h2>为什么索引写在实体上、由这里统一创建</h2>
 *
 * <p>MongoDB 不像 MySQL 那样有 DDL 兜底：没有唯一索引，{@code append} 就不会因为
 * 「同一 conversationId + seq 重复写入」而失败，事故表现从「启动即报错」退化成
 * 「历史消息里出现重复序号，客户端补拉时反复收到同一条」——这类问题只能靠对数事后发现。
 *
 * <p>索引定义因此必须离实体很近（{@code @CompoundIndex} / {@code @Indexed} 就贴在字段和类上），
 * 但创建时机由我们自己掌控：Boot 的「首次访问实体时自动建索引」既不保证发生在流量进来之前，
 * 也不产出「建了哪些索引」的启动日志。这里在启动阶段把它们一次性建齐，
 * 并且用 {@link MongoPersistentEntityIndexResolver} 解析注解——它是 Spring Data 自己的解析器，
 * 不需要我们再手写一遍字段名，也就不会出现「注解改了、创建代码没改」的错位。
 *
 * <p>创建是幂等的（同名同定义即 no-op），所以可以一直留在启动路径上。
 *
 * <p>另一半同样是启动期必须做的：<b>删掉本模块自己废弃的索引</b>（见 {@link #OBSOLETE_INDEXES}）。
 * {@code createIndex} 只增不减——注解删了索引还在，而一条约束「已不再落库的字段」的唯一索引
 * 会让写入从第二条起全部失败（缺失字段被当成 null，撞 {@code dup key: null}）。
 * 这种事只能靠启动期收敛，否则就是「服务重启能起、但一发消息就 500」。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public final class MongoSchema {

    private static final Logger log = LoggerFactory.getLogger(MongoSchema.class);

    /** 需要建索引的实体：新增集合时在这里登记，忘了登记的表现是「查询慢但结果对」，很难被发现 */
    private static final List<Class<?>> ENTITIES = List.of(
            MessageEntity.class,
            MessageDeleteEntity.class,
            GroupEntity.class,
            GroupMemberEntity.class,
            RelationEntity.class);

    /**
     * 曾经由本模块创建、后来废弃的索引：集合名 → 索引名。
     *
     * <p>{@code createIndex} 是「加了就有」的操作，<b>删注解不会删索引</b>。
     * 这是 {@code uk_msg_server_id} 的教训：{@code serverMsgId} 原本是普通字段上的
     * {@code @Indexed(unique = true)}，后来改成文档 {@code _id}，字段本身不再落库——
     * 于是这条唯一索引约束的是一个<b>永远缺失的字段</b>，MongoDB 把缺失当 null，
     * 库里只要有一条文档，之后每条新消息都会撞 {@code dup key: { serverMsgId: null }}。
     *
     * <p>为什么手写清单而不是「把不在实体声明里的索引全删掉」：生产上很可能有人为了某个
     * 慢查询手工加过索引，自动清扫会把它们悄悄删掉，变成另一种「上线后才发现的性能事故」。
     * 这里只清理<b>本模块自己创建过</b>的名字，边界清楚。
     */
    private static final Map<String, List<String>> OBSOLETE_INDEXES = Map.of(
            MongoCollections.MESSAGE, List.of("uk_msg_server_id"));

    private MongoSchema() {
    }

    /** 收敛索引：先清掉本模块废弃的索引，再把实体声明的索引建齐（两步都幂等） */
    public static void ensureIndexes(MongoTemplate template) {
        int dropped = dropObsoleteIndexes(template);
        int created = 0;
        for (Class<?> type : ENTITIES) {
            for (IndexDefinitionHolder definition : resolveIndexes(type)) {
                template.indexOps(type).createIndex(definition);
                created++;
            }
        }
        log.info("[storage] MongoDB 索引已就绪：{} 个实体 / {} 条索引 / 清理过时索引 {} 条"
                        + "（im.mongo.init-indexes=false 可关闭）",
                ENTITIES.size(), created, dropped);
    }

    /** 只删 {@link #OBSOLETE_INDEXES} 里点名的索引；集合不存在就跳过（全新库很正常） */
    private static int dropObsoleteIndexes(MongoTemplate template) {
        int dropped = 0;
        for (Map.Entry<String, List<String>> entry : OBSOLETE_INDEXES.entrySet()) {
            String collection = entry.getKey();
            if (!template.collectionExists(collection)) {
                continue;
            }
            IndexOperations indexOps = template.indexOps(collection);
            Set<String> existing = new HashSet<>();
            for (IndexInfo info : indexOps.getIndexInfo()) {
                existing.add(info.getName());
            }
            for (String obsolete : entry.getValue()) {
                if (!existing.contains(obsolete)) {
                    continue;
                }
                indexOps.dropIndex(obsolete);
                dropped++;
                log.warn("[storage] 已删除过时索引 {}.{}：它约束的字段在新版里改成了文档 _id（不再落库字段），"
                        + "留着会让第二条消息之后全部撞「dup key: null」", collection, obsolete);
            }
        }
        return dropped;
    }

    /**
     * 解析某个实体声明的索引（不连数据库，纯元数据）。
     *
     * <p>包级可见：这个解析结果是「索引确实被声明了」唯一的可断言形态，
     * 而它不需要 Mongo 实例，所以能在单元测试里跑（见 {@code MongoEntityMappingTest}）。
     */
    static List<IndexDefinitionHolder> resolveIndexes(Class<?> entityType) {
        MongoMappingContext mappingContext = new MongoMappingContext();
        mappingContext.afterPropertiesSet();
        MongoPersistentEntityIndexResolver resolver = new MongoPersistentEntityIndexResolver(mappingContext);
        List<IndexDefinitionHolder> definitions = new ArrayList<>();
        for (IndexDefinitionHolder holder : resolver.resolveIndexFor(TypeInformation.of(entityType))) {
            definitions.add(holder);
        }
        return definitions;
    }
}
