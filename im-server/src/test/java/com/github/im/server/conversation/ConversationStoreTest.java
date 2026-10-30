package com.github.im.server.conversation;

import com.github.im.contract.RedisKeys;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 会话模型的三条硬约定。
 *
 * <ol>
 *   <li><b>未读数是算出来的</b>：{@code lastSeq - readSeq}。存计数在群聊里是
 *       「消息数 × 成员数」的写放大，且一次写失败就永久和真实值不一致；</li>
 *   <li><b>已读是用户级</b>：手机读完，网页的角标就该清零——多端合并在这是免费的，
 *       因为只有一份状态。按设备分的是「同步水位」，不是「已读」；</li>
 *   <li><b>新会话的已读基线</b>：首次出现在列表里时把已读设为当时的 lastSeq，
 *       否则新人进一个历史很长的群，未读数一上来就是全量。</li>
 * </ol>
 *
 * <p>另外锁住两个容易踩的细节：lastSeq 只前进（乱序到达的旧消息不能把会话推回去）、
 * 已读要 cap 到 lastSeq（客户端上报未来的 seq 不能让未读数变负）。
 *
 * <p>需要本机 Redis，不可用时自动跳过。只清理自己造的 key。
 */
class ConversationStoreTest {

    private static final Duration TTL = Duration.ofHours(1);

    private final List<String> conversationIds = new ArrayList<>();
    private LettuceConnectionFactory factory;
    private StringRedisTemplate redis;

    @AfterEach
    void cleanup() {
        if (redis != null) {
            for (String conversationId : conversationIds) {
                redis.delete(RedisKeys.convMeta(conversationId));
            }
        }
        if (factory != null) {
            factory.destroy();
        }
    }

    // ==== 元信息 ====

    @Test
    @DisplayName("lastSeq 只前进：乱序到达的旧消息不会把会话推回去")
    void metaOnlyMovesForward() {
        ConversationStore store = store();
        String conversationId = newConversation();

        assertTrue(store.updateMeta(conversationId, "SINGLE", 5, 1000, "alice", "hi", "m5"));
        assertFalse(store.updateMeta(conversationId, "SINGLE", 3, 900, "bob", "old", "m3"),
                "seq=3 比已知的 5 旧，必须丢弃");

        ConversationStore.ConversationMeta meta = store.meta(conversationId);
        assertEquals(5, meta.lastSeq());
        assertEquals("hi", meta.lastDigest(), "后到的旧消息不能覆盖最后一条的摘要");
        assertEquals("alice", meta.lastFrom());
        assertEquals(1000, meta.lastTime());
    }

    @Test
    @DisplayName("元信息包含会话类型与最后一条消息，会话列表直接可用")
    void metaCarriesSummaryFields() {
        ConversationStore store = store();
        String conversationId = newConversation();

        store.updateMeta(conversationId, "GROUP", 42, 1_700_000_000_000L, "alice", "hello world", "m42");

        ConversationStore.ConversationMeta meta = store.meta(conversationId);
        assertEquals("GROUP", meta.type());
        assertEquals(42, meta.lastSeq());
        assertEquals(1_700_000_000_000L, meta.lastTime());
        assertEquals("m42", meta.lastMsgId());
    }

    @Test
    @DisplayName("不存在的会话返回空元信息，不抛异常")
    void missingMetaIsEmpty() {
        ConversationStore store = store();
        ConversationStore.ConversationMeta meta = store.meta("nope-" + UUID.randomUUID());
        assertEquals(0, meta.lastSeq());
        assertEquals("", meta.type());
    }

    // ==== 会话列表与已读基线 ====

    @Test
    @DisplayName("首次进入会话列表时，已读基线是当时的 lastSeq：只有这条算未读")
    void firstTouchBaselineIsCurrentSeq() {
        ConversationStore store = store();
        String userId = newUser();
        String conversationId = newConversation();
        // 会话里已经有 100 条历史
        store.updateMeta(conversationId, "GROUP", 100, 1000, "alice", "hi", "m100");

        // 第 101 条消息把他带进这个会话的列表（基线 = 100）
        store.touch(userId, conversationId, 1001, 100);

        assertEquals(100, store.readSeq(userId, conversationId));
        long unread = store.meta(conversationId).lastSeq() - store.readSeq(userId, conversationId);
        assertEquals(0, unread, "历史消息不该算成未读——否则新人一进群就是 100 条未读");
    }

    @Test
    @DisplayName("基线只初始化一次：后续 touch 不再改动已读位置")
    void baselineSetOnlyOnce() {
        ConversationStore store = store();
        String userId = newUser();
        String conversationId = newConversation();

        store.touch(userId, conversationId, 1000, 10);
        store.touch(userId, conversationId, 2000, 50);

        assertEquals(10, store.readSeq(userId, conversationId),
                "第二次 touch 若是覆盖基线，用户之前读过的位置会被反复重置");
    }

    @Test
    @DisplayName("会话列表的排序时间只前进，不会被补发的旧消息压下去")
    void touchKeepsLatestTime() {
        ConversationStore store = store();
        String userId = newUser();
        String conversationId = newConversation();

        store.touch(userId, conversationId, 2000, 0);
        store.touch(userId, conversationId, 1000, 0);

        List<String> recent = store.recentConversations(userId, 10);
        assertEquals(List.of(conversationId), recent);
        Double score = redis.opsForZSet().score(ConversationStore.convsKeyOf(userId), conversationId);
        assertEquals(2000.0, score, "ZADD GT 保证只往前排");
    }

    @Test
    @DisplayName("会话列表按最后消息时间倒序")
    void conversationsOrderedByLastTime() {
        ConversationStore store = store();
        String userId = newUser();
        String older = newConversation();
        String newer = newConversation();

        store.touch(userId, older, 1000, 0);
        store.touch(userId, newer, 2000, 0);

        assertEquals(List.of(newer, older), store.recentConversations(userId, 10));
    }

    @Test
    @DisplayName("超过写扩散上限时跳过成员列表，但不影响元信息")
    void fanoutSkippedWhenTooLarge() {
        ConversationStore store = store(Duration.ofHours(1), 2);
        String conversationId = newConversation();
        List<String> members = List.of(newUser(), newUser(), newUser());

        assertEquals(0, store.touchAll(members, conversationId, 1000, 99));

        assertTrue(store.updateMeta(conversationId, "GROUP", 100, 1000, "alice", "hi", "m100"));
        assertEquals(100, store.meta(conversationId).lastSeq(),
                "元信息是权威源，扇出被降级不影响它——大群成员靠群反查表补齐列表");
        assertTrue(store.recentConversations(members.get(0), 10).isEmpty());
    }

    // ==== 已读 ====

    @Test
    @DisplayName("已读只前进，乱序上报不会让它回退")
    void markReadNeverGoesBackwards() {
        ConversationStore store = store();
        String userId = newUser();
        String conversationId = newConversation();
        store.updateMeta(conversationId, "SINGLE", 100, 1000, "alice", "hi", "m100");

        assertEquals(50, store.markRead(userId, conversationId, 50, 100));
        assertEquals(50, store.markRead(userId, conversationId, 30, 100), "较小的值晚到不能把已读拉回去");

        long unread = store.meta(conversationId).lastSeq() - store.readSeq(userId, conversationId);
        assertEquals(50, unread);
    }

    @Test
    @DisplayName("已读 cap 到会话当前 lastSeq：客户端上报未来的 seq 不会让未读数变负")
    void markReadIsCappedByLastSeq() {
        ConversationStore store = store();
        String userId = newUser();
        String conversationId = newConversation();
        store.updateMeta(conversationId, "SINGLE", 100, 1000, "alice", "hi", "m100");

        long readSeq = store.markRead(userId, conversationId, 9999, 100);

        assertEquals(100, readSeq);
        assertEquals(0, store.meta(conversationId).lastSeq() - readSeq, "未读数不能是负数");
    }

    @Test
    @DisplayName("已读是用户级的：一端读完，另一端看到的也是已读")
    void readIsSharedAcrossDevices() {
        ConversationStore store = store();
        String userId = newUser();
        String conversationId = newConversation();
        store.updateMeta(conversationId, "SINGLE", 100, 1000, "alice", "hi", "m100");
        store.touch(userId, conversationId, 1000, 0);

        // 手机上报读了 100
        store.markRead(userId, conversationId, 100, 100);

        // 网页拉列表：已读没有任何 deviceId 维度，直接就是 100
        assertEquals(100, store.readSeq(userId, conversationId));
        assertEquals(0, store.meta(conversationId).lastSeq() - store.readSeq(userId, conversationId),
                "这正是「多端已读合并免费」的含义：只有一份状态，不需要合并");
    }

    @Test
    @DisplayName("批量读已读位置与会话元信息，会话列表一页只走一次 pipeline")
    void batchReadsCoverAllConversations() {
        ConversationStore store = store();
        String userId = newUser();
        String a = newConversation();
        String b = newConversation();
        store.updateMeta(a, "SINGLE", 10, 1000, "alice", "a", "ma");
        store.updateMeta(b, "GROUP", 20, 2000, "bob", "b", "mb");
        store.markRead(userId, a, 10, 10);

        assertEquals(10, store.readSeqs(userId, List.of(a, b)).get(a));
        assertEquals(0, store.readSeqs(userId, List.of(a, b)).get(b));
        assertEquals(20, store.metas(List.of(a, b)).get(b).lastSeq());
        assertEquals("GROUP", store.metas(List.of(a, b)).get(b).type());
    }

    @Test
    @DisplayName("批量读同一会话里多个人的已读位置（群已读聚合的转置查询）")
    void batchReadsCoverAllMembersInOneConversation() {
        ConversationStore store = store();
        String alice = newUser();
        String bob = newUser();
        String carol = newUser();
        // 一个从没读过的人
        String dave = newUser();
        String groupConversation = newConversation();
        store.updateMeta(groupConversation, "GROUP", 100, 1000, "alice", "hi", "m100");
        store.markRead(alice, groupConversation, 100, 100);
        store.markRead(bob, groupConversation, 40, 100);

        Map<String, Long> readSeqs = store.readSeqsOf(groupConversation,
                List.of(alice, bob, carol, dave, alice));

        assertEquals(100, readSeqs.get(alice));
        assertEquals(40, readSeqs.get(bob));
        assertEquals(0, readSeqs.get(carol), "没上报过的成员必须是 0，不能缺席");
        assertEquals(0, readSeqs.get(dave));
        assertEquals(4, readSeqs.size(), "重复的 userId 要去重");
        assertTrue(store.readSeqsOf(groupConversation, List.of()).isEmpty());
    }

    @Test
    @DisplayName("群反查出来的会话会补进列表，即使它从没被写扩散过")
    void groupReverseIndexFillsList() {
        ConversationStore store = store(Duration.ofHours(1), 1);
        String userId = newUser();
        String bigGroup = newConversation();

        // 模拟大群：成员数超过上限，写扩散被跳过
        assertEquals(0, store.touchAll(List.of(newUser(), newUser()), bigGroup, 5000, 0));
        assertTrue(store.recentConversations(userId, 10).isEmpty());

        // 但反查表里有这个群（groupId 就是会话 ID 去掉 g: 前缀）
        List<String> withGroups = store.recentConversations(userId, 10, Set.of(bigGroup));
        assertEquals(List.of("g:" + bigGroup), withGroups,
                "大群成员如果在会话列表里看不到自己的群，等于消息没有任何入口");
    }

    // ==== 辅助 ====

    private ConversationStore store() {
        return store(TTL, 1000);
    }

    private ConversationStore store(Duration ttl, int fanoutLimit) {
        return new ConversationStore(redisOrSkip(), ttl, fanoutLimit);
    }

    private String newConversation() {
        String conversationId = "t:" + UUID.randomUUID();
        conversationIds.add(conversationId);
        return conversationId;
    }

    /** userId 也是随机的，清理时按会话列表/已读的 key 一起删 */
    private String newUser() {
        String userId = "conv-test-" + UUID.randomUUID();
        if (redis != null) {
            redis.delete(ConversationStore.convsKeyOf(userId));
            redis.delete(ConversationStore.readKeyOf(userId));
        }
        testUserIds.add(userId);
        return userId;
    }

    private final List<String> testUserIds = new ArrayList<>();

    @AfterEach
    void cleanupUsers() {
        if (redis != null) {
            for (String userId : testUserIds) {
                redis.delete(ConversationStore.convsKeyOf(userId));
                redis.delete(ConversationStore.readKeyOf(userId));
            }
        }
    }

    private StringRedisTemplate redisOrSkip() {
        try {
            if (redis == null) {
                RedisStandaloneConfiguration config = new RedisStandaloneConfiguration(
                        System.getProperty("im.test.redis.host", "127.0.0.1"),
                        Integer.getInteger("im.test.redis.port", 6379));
                factory = new LettuceConnectionFactory(config);
                factory.afterPropertiesSet();
                redis = new StringRedisTemplate(factory);
                redis.afterPropertiesSet();
            }
            redis.hasKey("im:test:probe");
            return redis;
        } catch (RuntimeException e) {
            Assumptions.assumeTrue(false, "本机 Redis 不可用，跳过：" + e.getMessage());
            throw e;
        }
    }
}
