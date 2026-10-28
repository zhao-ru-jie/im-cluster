package com.github.im.storage.mongodb;

import com.github.im.server.group.GroupInfo;
import com.github.im.server.group.GroupRole;
import com.github.im.server.message.StoredMessage;
import com.github.im.storage.mongodb.entity.GroupEntity;
import com.github.im.storage.mongodb.entity.GroupMemberEntity;
import com.github.im.storage.mongodb.entity.MessageDeleteEntity;
import com.github.im.storage.mongodb.entity.MessageEntity;
import com.github.im.storage.mongodb.entity.RelationEntity;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolver.IndexDefinitionHolder;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 实体映射与索引声明的单元测试（不需要 Mongo 实例）。
 *
 * <p>为什么这些值得单独测：
 * <ul>
 *   <li><b>实体 ↔ 模型转换</b>是唯一「错了也不报错」的地方——字段名写错的表现是某个字段永远为 null，
 *       接口照样 200；</li>
 *   <li><b>{@code _id} 拼装规则</b>一旦不一致，重复写入不再被拒、按 id 点查查不到、
 *       删除记录也就失去幂等；</li>
 *   <li><b>索引注解</b>的 {@code def} 是字符串，写错（少个花括号、字段名拼错）不会编译失败，
 *       只会在启动建索引时才炸、或者安静地建成一条没用的索引。这里用 Spring Data 自己的解析器
 *       把注解解成索引定义并把名字/唯一性钉住。</li>
 * </ul>
 */
class MongoEntityMappingTest {

    private static StoredMessage sample() {
        return new StoredMessage("s:alice:bob", 42L, "0190f3c1-7a2b-7c3d-9e4f-5a6b7c8d9e0f",
                "alice", "bob", "SINGLE", "c-1", "hello", 1_700_000_000_000L,
                "IMAGE", "{\"url\":\"https://example.com/a.png\",\"size\":1024}");
    }

    // ==== 实体 ↔ 模型 ====

    @Test
    @DisplayName("消息模型 → 实体 → 模型：所有字段无损往返")
    void messageRoundTrip() {
        StoredMessage original = sample();

        StoredMessage restored = MessageEntity.from(original).toStoredMessage();

        assertEquals(original, restored);
    }

    @Test
    @DisplayName("消息文档的身份是 serverMsgId（_id 由它承载），不是「会话 + seq」")
    void messageIdentityIsServerMsgId() {
        MessageEntity entity = MessageEntity.from(sample());

        assertEquals(sample().serverMsgId(), entity.getServerMsgId());
        // _id 就是 serverMsgId：同一个 ID 只能落一条，这是 UUIDv7 全局唯一的直接体现
        assertEquals(entity.getServerMsgId(),
                MessageEntity.from(sample()).getServerMsgId());
    }

    @Test
    @DisplayName("新消息实体 recalledAt 为 null（撤回才是一次 set，不存在第三种状态）")
    void newMessageHasNoRecallMark() {
        assertNull(MessageEntity.from(sample()).getRecalledAt());
    }

    @Test
    @DisplayName("群资料模型 → 实体 → 模型：所有字段无损往返")
    void groupRoundTrip() {
        GroupInfo info = new GroupInfo("g:100", "研发群", "alice", "本群禁止聊政治", 300,
                1_700_000_000_000L, true, false);

        GroupInfo restored = GroupEntity.from(info).toInfo();

        assertEquals(info, restored);
    }

    @Test
    @DisplayName("缺群名时按 groupId 兜底（与 GroupInfo 的紧凑构造器语义一致）")
    void groupNameFallsBackToId() {
        GroupEntity entity = new GroupEntity();
        entity.setGroupId("g:100");

        GroupInfo info = entity.toInfo();

        assertEquals("g:100", info.name());
        assertEquals("", info.announcement());
        assertEquals(200, info.maxMembers());
    }

    @Test
    @DisplayName("角色缺失或脏数据按 MEMBER 解析，不当成放行")
    void roleFallsBackToMember() {
        GroupMemberEntity entity = new GroupMemberEntity();
        assertEquals(GroupRole.MEMBER, entity.roleOrDefault());

        entity.setRole("not-a-role");
        assertEquals(GroupRole.MEMBER, entity.roleOrDefault());

        entity.setRole("ADMIN");
        assertEquals(GroupRole.ADMIN, entity.roleOrDefault());
    }

    // ==== _id 拼装规则 ====

    @Test
    @DisplayName("其余实体的 _id 是确定性拼接（重复写入天然被挡），且能区分方向/会话")
    void derivedIdsAreDeterministic() {
        assertEquals("alice:s:alice:bob:42", MessageDeleteEntity.idOf("alice", "s:alice:bob", 42L));
        assertEquals("g:100:alice", GroupMemberEntity.idOf("g:100", "alice"));
        assertEquals("FRIEND:alice:bob", RelationEntity.idOf(RelationEntity.TYPE_FRIEND, "alice", "bob"));

        // 同一 (user, conversationId, seq) 必须得到同一个 _id：这是「重复删除被挡」的前提
        assertEquals(MessageDeleteEntity.idOf("alice", "g:100", 7L),
                MessageDeleteEntity.idOf("alice", "g:100", 7L));
        // 有向：反向边必须是另一个 _id，否则好友关系会被写成一条
        assertNotEquals(RelationEntity.idOf(RelationEntity.TYPE_FRIEND, "alice", "bob"),
                RelationEntity.idOf(RelationEntity.TYPE_FRIEND, "bob", "alice"));
    }

    // ==== 索引声明 ====

    @Test
    @DisplayName("im_message：两条唯一索引（保序去重 / 幂等），消息 ID 由 _id 自身承担")
    void messageIndexes() {
        Map<String, Document> indexes = indexOptionsOf(MessageEntity.class);

        assertTrue(indexes.containsKey("uk_msg_conv_seq"), "少了保序与范围扫描的主索引，实际：" + indexes.keySet());
        assertTrue(indexes.containsKey("uk_msg_idem"), "少了幂等最终防线，实际：" + indexes.keySet());
        assertTrue(isUnique(indexes.get("uk_msg_conv_seq")),
                "serverMsgId 当了 _id 之后，重复 (conv, seq) 只能靠这条唯一索引挡住");
        assertTrue(isUnique(indexes.get("uk_msg_idem")));
        assertFalse(indexes.containsKey("uk_msg_server_id"),
                "serverMsgId 已是 _id，再建一条唯一索引纯属重复写放大：" + indexes.keySet());
    }

    @Test
    @DisplayName("其余集合：成员/关系是唯一键，反查走单列索引")
    void otherIndexes() {
        assertTrue(isUnique(indexOptionsOf(GroupMemberEntity.class).get("uk_group_member")));
        assertTrue(indexOptionsOf(GroupMemberEntity.class).containsKey("idx_group_member_user"),
                "少了 userId 反查索引：「我加入了哪些群」会退化成全集合扫描");
        assertTrue(isUnique(indexOptionsOf(RelationEntity.class).get("uk_relation")));
        assertTrue(indexOptionsOf(RelationEntity.class).containsKey("idx_relation_target"));
        assertTrue(indexOptionsOf(GroupEntity.class).containsKey("idx_group_owner"));
        assertEquals(2, indexOptionsOf(MessageDeleteEntity.class).size(),
                "删除表只需要两个查询方向：某人某会话、某会话某条");
    }

    /** 实体声明的索引：名字 → IndexOptions（Spring Data 解析注解的结果，不连数据库） */
    private static Map<String, Document> indexOptionsOf(Class<?> entityType) {
        List<IndexDefinitionHolder> definitions = MongoSchema.resolveIndexes(entityType);
        Map<String, Document> indexes = new LinkedHashMap<>();
        for (IndexDefinitionHolder definition : definitions) {
            Document options = definition.getIndexOptions();
            Object name = options.get("name");
            indexes.put(name == null ? definition.getPath() : name.toString(), options);
        }
        return indexes;
    }

    private static boolean isUnique(Document options) {
        return options != null && Boolean.TRUE.equals(options.get("unique"));
    }
}
