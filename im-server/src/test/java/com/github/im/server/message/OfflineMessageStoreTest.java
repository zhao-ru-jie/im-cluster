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
 * 用户会让队列无界增长）。
 *
 * <p><b>群聊指针</b>：只保留「最早的未补发 seq」（后面的消息不能把指针往前推，否则中间那段补不回来）、
 * 只前进不回退、追平后能清掉。
 *
 * <p>需要本机 Redis，不可用时自动跳过。只清理自己造的 {@code im:offline:*} key。
 */
class OfflineMessageStoreTest {

    private static final Duration TTL = Duration.ofHours(1);

    private final List<String> userIds = new ArrayList<>();
    private LettuceConnectionFactory factory;
    private StringRedisTemplate redis;

    @AfterEach
    void cleanup() {
        if (redis != null && !userIds.isEmpty()) {
            for (String id : userIds) {
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
            assertEquals(List.of("frame-1"), store.drain(userId, 10));
        }
    }

    @Test
    @DisplayName("重复写入按 FIFO 追加")
    void appendsInOrder() {
        OfflineMessageStore store = store(500);
        String userId = newUser();

        store.storeAll(List.of(userId), "first");
        store.storeAll(List.of(userId), "second");

        assertEquals(List.of("first", "second"), store.drain(userId, 10));
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
        long missing = targets.stream().filter(id -> store.size(id) != 1).count();
        assertEquals(0, missing, "600 人里任何一个没收到离线帧都算丢消息");
    }

    @Test
    @DisplayName("空输入直接返回 0，不发任何命令")
    void emptyInputIsNoop() {
        OfflineMessageStore store = store(500);
        assertEquals(0, store.storeAll(List.of(), "x"));
        assertEquals(0L, store.size("never-existed-" + UUID.randomUUID()));
    }

    @Test
    @DisplayName("帧队列有长度上限：超出上限时丢最旧的、保留最新的")
    void frameQueueIsBounded() {
        OfflineMessageStore store = store(3);
        String userId = newUser();

        for (int i = 0; i < 5; i++) {
            store.store(userId, "frame-" + i);
        }

        assertEquals(List.of("frame-2", "frame-3", "frame-4"), store.drain(userId, 10),
                "上限是防无界增长的兜底，代价是丢最旧的；保留最新才对客户端有意义");
    }

    @Test
    @DisplayName("帧队列必须带 TTL，否则长期不上线的用户永远占着内存")
    void frameQueueHasTtl() {
        OfflineMessageStore store = store(500);
        String userId = newUser();

        store.store(userId, "frame");
        store.storeAll(List.of(userId), "frame-2");

        Long ttlMillis = redis.getExpire(RedisKeys.OFFLINE_PREFIX + userId);
        assertNotNull(ttlMillis);
        assertTrue(ttlMillis > 0, "写入后必须续期，实际 ttl=" + ttlMillis);
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
    @DisplayName("追平且期间没有新消息时清掉指针")
    void pendingIsClearedWhenCaughtUp() {
        OfflineMessageStore store = store(500);
        String userId = newUser();
        String conversation = TestRedis.conversationId();
        store.recordGroupPending(List.of(userId), conversation, 10);

        store.advancePending(userId, conversation, 10, 11, true);

        assertTrue(store.pendingConversations(userId, 10).isEmpty(), "追平后残留指针会让每轮 pull 都白查一次历史");
    }

    @Test
    @DisplayName("没追平时指针前进但不删除")
    void pendingAdvancesWhenNotCaughtUp() {
        OfflineMessageStore store = store(500);
        String userId = newUser();
        String conversation = TestRedis.conversationId();
        store.recordGroupPending(List.of(userId), conversation, 10);

        store.advancePending(userId, conversation, 10, 15, false);

        assertEquals(15L, store.pendingConversations(userId, 10).get(conversation));
    }

    @Test
    @DisplayName("指针只前进不回退（并发 pull 读到旧指针时不能把它推回去）")
    void pendingNeverGoesBackwards() {
        OfflineMessageStore store = store(500);
        String userId = newUser();
        String conversation = TestRedis.conversationId();
        store.recordGroupPending(List.of(userId), conversation, 10);
        store.advancePending(userId, conversation, 10, 15, false);

        store.advancePending(userId, conversation, 10, 3, false);

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
