package com.github.im.storage.mongodb;

import com.github.im.storage.mongodb.entity.GroupMemberEntity;
import com.github.im.storage.mongodb.entity.MessageDeleteEntity;
import com.github.im.storage.mongodb.entity.MessageEntity;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 查询 / 更新语句的字段映射断言（不需要 Mongo 实例）。
 *
 * <h2>为什么这个测试比看起来重要</h2>
 *
 * <p>实体映射最容易踩的不是语法，而是<b>字段名到底映射成了什么</b>：
 * 实体里的 {@code id} 属性在数据库里叫 {@code _id}，其余属性名与字段名一致。
 * 一旦这里映射错了（比如手写 {@code where("id")} 而框架没做转换），
 * 表现是<b>查询永远不命中、撤回永远返回 false、删除去重永远失效</b>——
 * 不报错、不回滚，只能靠演练时才发现。
 *
 * <p>用一个真实的 {@link MappingMongoConverter} + {@link QueryMapper} 把语句映射成 BSON 断言，
 * 就把这件事从「起个 Mongo 手点一下」变成了纯函数验证。
 */
class MongoQueryMappingTest {

    private static final MongoMappingContext CONTEXT = mappingContext();

    private static final QueryMapper MAPPER = new QueryMapper(converter());

    @Test
    @DisplayName("消息身份 serverMsgId 映射成 _id（撞 _id 才能挡住同一消息重复落库）")
    void serverMsgIdBecomesUnderscoreId() {
        String serverMsgId = "0190f3c1-7a2b-7c3d-9e4f-5a6b7c8d9e0f";

        Document mapped = MAPPER.getMappedObject(
                Query.query(Criteria.where("serverMsgId").is(serverMsgId)).getQueryObject(),
                CONTEXT.getRequiredPersistentEntity(MessageEntity.class));

        assertEquals(serverMsgId, mapped.getString("_id"));
        assertFalse(mapped.containsKey("serverMsgId"), "映射后不该留下字面叫 serverMsgId 的字段：" + mapped);
    }

    @Test
    @DisplayName("定位条件 (conversationId, seq) 保持原名、落在唯一索引上")
    void locateCriteriaKeepTheirNames() {
        Document mapped = MAPPER.getMappedObject(
                Query.query(Criteria.where("conversationId").is("s:alice:bob").and("seq").is(42L)).getQueryObject(),
                CONTEXT.getRequiredPersistentEntity(MessageEntity.class));

        assertEquals("s:alice:bob", mapped.getString("conversationId"));
        assertEquals(42L, mapped.get("seq"));
    }

    @Test
    @DisplayName("普通属性保持同名（落库字段与另两种介质的叫法一致）")
    void plainPropertiesKeepTheirNames() {
        Document mapped = MAPPER.getMappedObject(
                Query.query(Criteria.where("conversationId").is("g:100")
                        .and("seq").gt(10L)
                        .and("recalledAt").ne(null)).getQueryObject(),
                CONTEXT.getRequiredPersistentEntity(MessageEntity.class));

        assertEquals("g:100", mapped.getString("conversationId"));
        assertTrue(mapped.containsKey("seq"));
        assertTrue(mapped.containsKey("recalledAt"));
        // 发送方字段叫 from：不映射成 fromUser/from_user，迁移脚本才不用再对照一遍
        Document from = MAPPER.getMappedObject(
                Query.query(Criteria.where("from").is("alice")).getQueryObject(),
                CONTEXT.getRequiredPersistentEntity(MessageEntity.class));
        assertEquals("alice", from.getString("from"));
    }

    @Test
    @DisplayName("更新语句的字段映射：$set recalledAt 不被改名")
    void updateFieldsAreMapped() {
        Document mapped = MAPPER.getMappedObject(
                new Update().set("recalledAt", 1_700_000_000_000L).getUpdateObject(),
                CONTEXT.getRequiredPersistentEntity(MessageEntity.class));

        assertEquals(1_700_000_000_000L, mapped.get("$set", Document.class).get("recalledAt"));
    }

    @Test
    @DisplayName("$setOnInsert 的 id 也会落到 _id（upsert 才能建出带 _id 的文档）")
    void setUpInsertUsesUnderscoreId() {
        Document mapped = MAPPER.getMappedObject(
                new Update()
                        .setOnInsert("id", MessageDeleteEntity.idOf("alice", "s:alice:bob", 7L))
                        .setOnInsert("userId", "alice")
                        .getUpdateObject(),
                CONTEXT.getRequiredPersistentEntity(MessageDeleteEntity.class));

        Document onInsert = mapped.get("$setOnInsert", Document.class);
        assertEquals("alice:s:alice:bob:7", onInsert.getString("_id"));
        assertEquals("alice", onInsert.getString("userId"));
    }

    @Test
    @DisplayName("群成员的 id 是「群:人」，映射结果仍然只落在 _id 上")
    void memberIdMapsToUnderscoreId() {
        Document mapped = MAPPER.getMappedObject(
                Query.query(Criteria.where("id").is(GroupMemberEntity.idOf("g:100", "alice"))).getQueryObject(),
                CONTEXT.getRequiredPersistentEntity(GroupMemberEntity.class));

        assertEquals("g:100:alice", mapped.getString("_id"));
    }

    private static MongoMappingContext mappingContext() {
        MongoMappingContext context = new MongoMappingContext();
        context.afterPropertiesSet();
        return context;
    }

    private static MappingMongoConverter converter() {
        MappingMongoConverter converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, CONTEXT);
        converter.afterPropertiesSet();
        return converter;
    }
}
