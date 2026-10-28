package com.github.im.storage.redis;

import com.github.im.server.message.StoredMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link RedisMessageRepository#maxSeq} 的回归测试。
 *
 * <p>要点：它是 im-server 里 seq 发号器（{@code SeqAllocator}）的下界来源，<b>不能假设 List 尾部
 * 就是最大 seq</b>。多实例下 seq 在 Redis 里串行发号，但 {@code append} 的到达顺序可能不同
 * （实例 A 拿到 5、实例 B 拿到 6，B 先落库），尾部会是 5。取错会让下界偏小、削弱防回绕保护。
 *
 * <p>随实现一起从 im-server 迁入本模块：介质相关的行为由介质模块自己守。
 */
class RedisMessageRepositoryTest {

    private final List<String> created = new ArrayList<>();

    @AfterEach
    void deleteOnlyOwnKeys() {
        if (created.isEmpty()) {
            return;
        }
        try {
            created.forEach(conversationId -> TestRedis.template().delete(TestRedis.messagesKey(conversationId)));
        } catch (RuntimeException ignored) {
            // Redis 不可用时用例本身已被标记 skipped，清理失败不该把 skip 变成 failure
        } finally {
            created.clear();
        }
    }

    @Test
    @DisplayName("空会话：maxSeq = 0（= 无历史，发号器按 1 起）")
    void emptyConversationHasZeroMaxSeq() {
        RedisMessageRepository repository = repository();

        assertEquals(0L, repository.maxSeq(conversation()));
    }

    @Test
    @DisplayName("落库顺序与 seq 顺序不一致时，maxSeq 仍取真正的最大 seq")
    void maxSeqIgnoresAppendOrder() {
        RedisMessageRepository repository = repository();
        String conversationId = conversation();

        repository.append(message(conversationId, 6));
        repository.append(message(conversationId, 5));
        repository.append(message(conversationId, 7));
        repository.append(message(conversationId, 4));

        assertEquals(7L, repository.maxSeq(conversationId));
    }

    // ==== 辅助 ====

    private static RedisMessageRepository repository() {
        return new RedisMessageRepository(TestRedis.template(), JsonMapper.builder().build());
    }

    private String conversation() {
        String conversationId = TestRedis.conversationId();
        created.add(conversationId);
        return conversationId;
    }

    private static StoredMessage message(String conversationId, long seq) {
        return new StoredMessage(conversationId, seq, "msg-" + seq, "alice", "bob",
                "SINGLE", "c-" + seq, "hi-" + seq, 1_750_000_000_000L + seq);
    }
}
