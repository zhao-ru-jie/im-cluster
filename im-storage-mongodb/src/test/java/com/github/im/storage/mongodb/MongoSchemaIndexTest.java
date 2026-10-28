package com.github.im.storage.mongodb;

import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * 索引收敛的回归测试：<b>必须连真实 MongoDB</b>（本机没有实例时自动跳过）。
 *
 * <h2>这条用例复现的事故</h2>
 *
 * <p>{@code serverMsgId} 最初是普通字段上的 {@code @Indexed(unique = true)}，后来改成文档
 * {@code _id}——字段不再落库。而 {@code createIndex} 只增不减，老库里的
 * {@code uk_msg_server_id} 依然存在，约束着一个<b>永远缺失</b>的字段：
 * MongoDB 把缺失当 null，于是第一条消息占掉那个 null 槽位，<b>从第二条起全部</b>写入失败：
 *
 * <pre>E11000 duplicate key error collection: im.im_message
 * index: uk_msg_server_id dup key: { serverMsgId: null }</pre>
 *
 * <p>所以这里先手工造出「旧版本的库」，断言事故可复现，再跑 {@link MongoSchema#ensureIndexes}，
 * 断言索引被清掉、写入恢复正常。用临时库（{@code im_cluster_it_*}）并在结束时删除，
 * 不碰真实业务库。
 */
class MongoSchemaIndexTest {

    private static final String URI = "mongodb://127.0.0.1:27017/?serverSelectionTimeoutMS=800&connectTimeoutMS=800";

    @Test
    @DisplayName("启动收敛会删掉遗留的 uk_msg_server_id，写入随之恢复")
    void dropsLegacyServerMsgIdIndex() {
        assumeTrue(mongoReachable(), "本机没有 MongoDB（127.0.0.1:27017），跳过这条集成用例");

        String database = "im_cluster_it_" + System.nanoTime();
        try (MongoClient client = MongoClients.create(URI)) {
            MongoTemplate mongo = new MongoTemplate(client, database);

            // 1) 造出旧版本的库：serverMsgId 是普通字段，且带着唯一索引
            mongo.getCollection(MongoCollections.MESSAGE).createIndex(Indexes.ascending("serverMsgId"),
                    new IndexOptions().unique(true).name("uk_msg_server_id"));

            // 2) 事故可复现：文档里没有 serverMsgId 字段（新版把它当 _id），第二条就撞 null
            insert(mongo, "m-1", 1L);
            assertThrows(MongoWriteException.class, () -> insert(mongo, "m-2", 2L),
                    "遗留索引存在时，第二条消息就该撞 dup key: { serverMsgId: null }");

            // 3) 启动收敛：删掉废弃索引，建齐实体声明的索引
            MongoSchema.ensureIndexes(mongo);

            Set<String> indexes = indexNames(mongo);
            assertFalse(indexes.contains("uk_msg_server_id"), "遗留索引没被清掉，实际：" + indexes);
            assertTrue(indexes.contains("uk_msg_conv_seq"), "保序索引没建成，实际：" + indexes);
            assertTrue(indexes.contains("uk_msg_idem"), "幂等索引没建成，实际：" + indexes);

            // 4) 写入恢复：连续两条都能落库（同会话不同 seq，且 clientMsgId 不同）
            insert(mongo, "m-3", 3L);
            insert(mongo, "m-4", 4L);
        } finally {
            try (MongoClient client = MongoClients.create(URI)) {
                client.getDatabase(database).drop();
            }
        }
    }

    private static void insert(MongoTemplate mongo, String suffix, long seq) {
        mongo.getCollection(MongoCollections.MESSAGE).insertOne(new Document("_id", "id-" + suffix)
                .append("conversationId", "s:alice:bob")
                .append("seq", seq)
                .append("from", "alice")
                .append("clientMsgId", "c-" + suffix)
                .append("body", "hi"));
    }

    private static Set<String> indexNames(MongoTemplate mongo) {
        Set<String> names = new HashSet<>();
        for (org.springframework.data.mongodb.core.index.IndexInfo info
                : mongo.indexOps(MongoCollections.MESSAGE).getIndexInfo()) {
            names.add(info.getName());
        }
        return names;
    }

    /** 探测本机有没有 MongoDB：没有就跳过，而不是让整条构建链失败 */
    private static boolean mongoReachable() {
        try (MongoClient client = MongoClients.create(URI)) {
            client.listDatabaseNames().first();
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }
}
