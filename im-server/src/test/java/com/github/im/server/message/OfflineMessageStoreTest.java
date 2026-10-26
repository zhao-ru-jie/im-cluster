package com.github.im.server.message;

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
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 离线存储的两条通道各自的硬约定。
 *
 * <p><b>单聊帧队列</b>：一人一条 key、大群批量要分片、<b>必须有上限和 TTL</b>（否则长期不上线的
 * 用户会让队列无界增长）；<b>读取不删除</b>，只有按设备游标确认之后才不再返回——这是
 * 「至少一次」的落点，旧实现 {@code LRANGE + LTRIM} 原子移走，网关写失败即丢。
 *
 * <p><b>群聊指针</b>：只保留「最早的未补发 seq」（后面的消息不能把指针往前推，否则中间那段补不回来）、
 * 只前进不回退、确认才推进、确实没数据才清掉。
 *
 * <p>需要本机 Redis，不可用时自动跳过。只清理自己造的 {@code im:offline:* / im:wm:*} key。
 */
class OfflineMessageStoreTest {

    private static final Duration TTL = Duration.ofHours(1);
    private static final String WEB = "web";
    private static final String PHONE = "phone";

    private final List<String> userIds = new ArrayList<>();
    private LettuceConnectionFactory factory;
    private StringRedisTemplate redis;

    @AfterEach
    void cleanup() {
        if (redis != null && !userIds.isEmpty()) {
            for (String id : userIds) {
                redis.delete(OfflineMessageStore.queueKeyOf(id));
                redis.delete(OfflineMessageStore.queueKeyOf(id) + ":pos");
                redis.delete(OfflineMessageStore.cursorKeyOf(id, WEB));
                redis.delete(OfflineMessageStore.cursorKeyOf(id, PHONE));
                redis.delete(RedisKeys.OFFLINE_PREFIX + id);
                redis.delete(RedisKeys.OFFLINE_PTR_PREFIX + id);
            }
        }
        if (factory != null) {
            factory.destroy();
        }
    }

    // ==== 单聊帧队列 ====

    @Test
    @DisplayName("storeAll 把同一个帧写给多个用户，各自独立成队列")
    void storesFrameForEachUser() {
        OfflineMessageStore store = store(500);
        List<String> targets = List.of(newUser(), newUser(), newUser());

        int stored = store.storeAll(targets, "frame-1");

        assertEquals(3, stored);
        for (String userId : targets) {
            assertEquals(List.of("frame-1"), frames(store, userId, WEB));
        }
    }

    @Test
    @DisplayName("重复写入按入队顺序追加")
    void appendsInOrder() {
        OfflineMessageStore store = store(500);
        String userId = newUser();

        store.storeAll(List.of(userId), "first");
        store.storeAll(List.of(userId), "second");

        assertEquals(List.of("first", "second"), frames(store, userId, WEB));
    }

    @Test
    @DisplayName("读不删：没确认之前重复拉取拿到同一批帧")
    void peekDoesNotConsume() {
        OfflineMessageStore store = store(500);
        String userId = newUser();
        store.store(userId, "frame-1");

        OfflineMessageStore.FrameBatch first = store.peek(userId, WEB, 10);
        OfflineMessageStore.FrameBatch second = store.peek(userId, WEB, 10);

        assertEquals(List.of("frame-1"), first.frames());
        assertEquals(List.of("frame-1"), second.frames(), "读一次就消失等于「读即消费」，网关写失败就丢了");
        assertEquals(first.ackSeq(), second.ackSeq(), "两次读到的是同一批位置");
        assertTrue(first.ackSeq() > 0, "必须给出待确认位置，否则客户端永远不知道该确认到哪");
    }

    @Test
    @DisplayName("确认之后游标前进，同一个设备不再读到已确认的帧")
    void cursorAdvancesAfterAck() {
        OfflineMessageStore store = store(500);
        String userId = newUser();
        store.store(userId, "frame-1");
        store.store(userId, "frame-2");

        OfflineMessageStore.FrameBatch batch = store.peek(userId, WEB, 10);
        store.ackFrames(userId, WEB, batch.ackSeq());

        assertTrue(store.peek(userId, WEB, 10).frames().isEmpty(), "已确认的帧不该再补一遍");
        assertEquals(batch.ackSeq(), store.cursorOf(userId, WEB));

        store.store(userId, "frame-3");
        assertEquals(List.of("frame-3"), frames(store, userId, WEB), "新消息仍然要能补到");
    }

    @Test
    @DisplayName("游标按设备隔离：一端确认不影响另一端")
    void cursorIsPerDevice() {
        OfflineMessageStore store = store(500);
        String userId = newUser();
        store.store(userId, "frame-1");

        OfflineMessageStore.FrameBatch forWeb = store.peek(userId, WEB, 10);
        store.ackFrames(userId, WEB, forWeb.ackSeq());

        assertTrue(store.peek(userId, WEB, 10).frames().isEmpty());
        assertEquals(List.of("frame-1"), frames(store, userId, PHONE),
                "手机还没确认过，必须还能读到——用户级单一游标会让这台设备永久丢消息");
    }

    @Test
    @DisplayName("游标只前进：迟到的旧确认不会把它拉回去")
    void cursorNeverGoesBackwards() {
        OfflineMessageStore store = store(500);
        String userId = newUser();
        store.store(userId, "frame-1");
        store.store(userId, "frame-2");

        long high = store.peek(userId, WEB, 10).ackSeq();
        store.ackFrames(userId, WEB, high);
        long afterStaleAck = store.ackFrames(userId, WEB, 1L);

        assertEquals(high, afterStaleAck, "回退会造成重复补发死循环");
        assertEquals(high, store.cursorOf(userId, WEB));
    }

    @Test
    @DisplayName("未确认条数按游标算，不是队列总条数")
    void pendingFramesExcludesAcknowledged() {
        OfflineMessageStore store = store(500);
        String userId = newUser();
        store.store(userId, "frame-1");
        store.store(userId, "frame-2");

        assertEquals(2, store.pendingFrames(userId, WEB));

        store.ackFrames(userId, WEB, store.peek(userId, WEB, 10).ackSeq());

        assertEquals(0, store.pendingFrames(userId, WEB),
                "已确认但还没被裁剪的帧不能计入 remaining，否则客户端会无限重拉");
    }

    @Test
    @DisplayName("超过分片阈值（600 人）时全部写入，不漏人")
    void handlesMoreThanOneShard() {
        OfflineMessageStore store = store(500);
        List<String> targets = new ArrayList<>();
        for (int i = 0; i < 600; i++) {
            targets.add(newUser());
        }

        int stored = store.storeAll(targets, "big-frame");

        assertEquals(600, stored);
        long missing = targets.stream().filter(id -> store.pendingFrames(id, WEB) != 1).count();
        assertEquals(0, missing, "600 人里任何一个没收到离线帧都算丢消息");
    }

    @Test
    @DisplayName("空输入直接返回 0，不发任何命令")
    void emptyInputIsNoop() {
        OfflineMessageStore store = store(500);
        assertEquals(0, store.storeAll(List.of(), "x"));
        assertEquals(0L, store.pendingFrames("never-existed-" + UUID.randomUUID(), WEB));
    }

    @Test
    @DisplayName("帧队列有长度上限：超出上限时丢最旧的、保留最新的")
    void frameQueueIsBounded() {
        OfflineMessageStore store = store(3);
        String userId = newUser();

        for (int i = 0; i < 5; i++) {
            store.store(userId, "frame-" + i);
        }

        assertEquals(List.of("frame-2", "frame-3", "frame-4"), frames(store, userId, WEB),
                "上限是防无界增长的兜底，代价是丢最旧的；保留最新才对客户端有意义");
    }

    @Test
    @DisplayName("帧队列必须带 TTL，否则长期不上线的用户永远占着内存")
    void frameQueueHasTtl() {
        OfflineMessageStore store = store(500);
        String userId = newUser();

        store.store(userId, "frame");
        store.storeAll(List.of(userId), "frame-2");

        Long queueTtl = redis.getExpire(OfflineMessageStore.queueKeyOf(userId));
        assertNotNull(queueTtl);
        assertTrue(queueTtl > 0, "写入后必须续期，实际 ttl=" + queueTtl);
        Long positionTtl = redis.getExpire(OfflineMessageStore.queueKeyOf(userId) + ":pos");
        assertNotNull(positionTtl);
        assertTrue(positionTtl > 0, "位置计数器也要 TTL，否则每个离线过的用户都留一个永久 key");
    }

    @Test
    @DisplayName("位置计数器丢失时从队列最大位置续上，新帧不会排到旧帧前面")
    void positionCounterRecoversFromQueue() {
        OfflineMessageStore store = store(500);
        String userId = newUser();
        store.store(userId, "frame-1");
        store.store(userId, "frame-2");

        // 模拟计数器过期/丢失（队列还在）
        redis.delete(OfflineMessageStore.queueKeyOf(userId) + ":pos");
        store.store(userId, "frame-3");

        assertEquals(List.of("frame-1", "frame-2", "frame-3"), frames(store, userId, WEB),
                "计数器从 1 重来会让新帧的 score 小于旧帧，补发顺序直接错乱");
    }

    // ==== 群聊补发指针 ====

    @Test
    @DisplayName("同一会话的后续消息只把指针往前推，不覆盖成最新 seq")
    void pendingKeepsEarliestSeq() {
        OfflineMessageStore store = store(500);
        String userId = newUser();
        String conversation = TestRedis.conversationId();

        store.recordGroupPending(List.of(userId), conversation, 10);
        store.recordGroupPending(List.of(userId), conversation, 20);
        store.recordGroupPending(List.of(userId), conversation, 30);

        assertEquals(10L, store.pendingConversations(userId, 10).get(conversation),
                "若覆盖成 30，中间 10~29 就永远补不回来了");
    }

    @Test
    @DisplayName("指针只为「用户 × 会话」占一个 field，不随消息数增长")
    void pendingDoesNotGrowWithMessageCount() {
        OfflineMessageStore store = store(500);
        String userId = newUser();
        String conversation = TestRedis.conversationId();

        for (int seq = 1; seq <= 50; seq++) {
            store.recordGroupPending(List.of(userId), conversation, seq);
        }

        assertEquals(1L, store.pendingConversationCount(userId), "50 条群消息只应留下 1 个 field");
    }

    @Test
    @DisplayName("指针带 TTL")
    void pendingHasTtl() {
        OfflineMessageStore store = store(500);
        String userId = newUser();

        store.recordGroupPending(List.of(userId), TestRedis.conversationId(), 1L);

        Long ttlMillis = redis.getExpire(RedisKeys.OFFLINE_PTR_PREFIX + userId);
        assertNotNull(ttlMillis);
        assertTrue(ttlMillis > 0, "写入后必须续期，实际 ttl=" + ttlMillis);
    }

    @Test
    @DisplayName("确认后指针前进；确实没数据时才能清掉")
    void pendingAdvancesOnAckAndClearsWhenEmpty() {
        OfflineMessageStore store = store(500);
        String userId = newUser();
        String conversation = TestRedis.conversationId();
        store.recordGroupPending(List.of(userId), conversation, 10);

        assertTrue(store.ackGroupPending(userId, conversation, 11));
        assertEquals(11L, store.pendingConversations(userId, 10).get(conversation));

        store.clearPending(userId, conversation);
        assertTrue(store.pendingConversations(userId, 10).isEmpty(),
                "残留指针会让每轮 pull 都白查一次历史");
    }

    @Test
    @DisplayName("指针只前进不回退（旧连接的确认晚到不能把它推回去）")
    void pendingNeverGoesBackwards() {
        OfflineMessageStore store = store(500);
        String userId = newUser();
        String conversation = TestRedis.conversationId();
        store.recordGroupPending(List.of(userId), conversation, 10);
        store.ackGroupPending(userId, conversation, 15);

        assertEquals(false, store.ackGroupPending(userId, conversation, 3));

        assertEquals(15L, store.pendingConversations(userId, 10).get(conversation), "回退会造成重复补发甚至死循环");
    }

    @Test
    @DisplayName("待补会话按 ID 字典序返回，保证多轮 pull 能够推进")
    void pendingConversationsAreOrdered() {
        OfflineMessageStore store = store(500);
        String userId = newUser();
        store.recordGroupPending(List.of(userId), "g:zzz", 1);
        store.recordGroupPending(List.of(userId), "g:aaa", 2);
        store.recordGroupPending(List.of(userId), "g:mmm", 3);

        assertEquals(List.of("g:aaa", "g:mmm"),
                new ArrayList<>(store.pendingConversations(userId, 2).keySet()),
                "只取前 2 个时必须是最小的一批，否则每轮都处理同一批、后面的会话永远排不上");
        assertEquals(3, store.pendingConversations(userId, 10).size());
    }

    // ==== 辅助 ====

    private static List<String> frames(OfflineMessageStore store, String userId, String deviceId) {
        return store.peek(userId, deviceId, 10).frames();
    }

    private OfflineMessageStore store(int maxFrames) {
        return new OfflineMessageStore(redisOrSkip(), TTL, maxFrames, TTL);
    }

    private String newUser() {
        String userId = "offline-test-" + UUID.randomUUID();
        userIds.add(userId);
        return userId;
    }

    private StringRedisTemplate redisOrSkip() {
        try {
            if (redis == null) {
                RedisStandaloneConfiguration config = new RedisStandaloneConfiguration("127.0.0.1", 6379);
                factory = new LettuceConnectionFactory(config);
                factory.afterPropertiesSet();
                redis = new StringRedisTemplate(factory);
                redis.afterPropertiesSet();
            }
            redis.hasKey("offline-test-probe");
            return redis;
        } catch (RuntimeException e) {
            Assumptions.assumeTrue(false, "本机 Redis 不可用，跳过：" + e.getMessage());
            throw e;
        }
    }
}
