package com.github.im.server.message;

import com.github.im.contract.RedisKeys;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * Redis 版消息存储（默认实现）：{@code im:conv:msgs:{conversationId}} 存 {@link StoredMessage} 的 JSON。
 *
 * <pre>im:conv:msgs:{conversationId}   List，右进左出，只保留最近 N 条</pre>
 *
 * <p>定位：本地开发 / 联调 / 小规模。生产用 {@code com.github.im.storage.jdbc.JdbcMessageRepository}
 * （`im.storage=jdbc`，位于 im-storage-jdbc 模块）——
 * Redis List 无法做按时间分区、无法跨会话检索、容量受内存限制。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
@ConditionalOnProperty(name = "im.storage", havingValue = "redis", matchIfMissing = true)
public class RedisMessageRepository implements MessageRepository {

    /** 单会话保留条数上限，防止联调阶段 Redis 无限膨胀 */
    private static final int MAX_LENGTH = 1000;

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;

    public RedisMessageRepository(StringRedisTemplate redis, ObjectMapper objectMapper) {
        this.redis = redis;
        this.objectMapper = objectMapper;
    }

    @Override
    public void append(StoredMessage message) {
        String key = RedisKeys.CONV_MESSAGES_PREFIX + message.conversationId();
        redis.opsForList().rightPush(key, objectMapper.writeValueAsString(message));
        redis.opsForList().trim(key, -MAX_LENGTH, -1);
    }

    /**
     * 扫描本会话保留的全部条目取最大 seq。
     *
     * <p>不能只取 List 尾部：多实例下 seq 在 Redis 里串行发号，但 {@code append} 的到达顺序
     * 可能与 seq 顺序不一致（实例 A 拿到 5、实例 B 拿到 6，B 先落库），此时尾部是 5 而真实最大是 6，
     * 会把发号下界算小、削弱防回绕保护。{@code trim} 只保留最近 {@value #MAX_LENGTH} 条，
     * 扫描成本有上限，且本实现只用于本地/小规模（生产走 {@code JdbcMessageRepository} 的
     * {@code MAX(seq)} 索引查找）。
     */
    @Override
    public long maxSeq(String conversationId) {
        List<String> rows = redis.opsForList().range(RedisKeys.CONV_MESSAGES_PREFIX + conversationId, 0, -1);
        if (rows == null || rows.isEmpty()) {
            return 0L;
        }
        long max = 0L;
        for (String row : rows) {
            if (row == null || row.isBlank()) {
                continue;
            }
            max = Math.max(max, objectMapper.readValue(row, StoredMessage.class).seq());
        }
        return max;
    }

    @Override
    public List<StoredMessage> recent(String conversationId, int limit) {
        List<String> rows = redis.opsForList().range(RedisKeys.CONV_MESSAGES_PREFIX + conversationId, -limit, -1);
        if (rows == null || rows.isEmpty()) {
            return List.of();
        }
        List<StoredMessage> messages = new ArrayList<>(rows.size());
        for (String row : rows) {
            messages.add(objectMapper.readValue(row, StoredMessage.class));
        }
        return messages;
    }

    /**
     * 扫描本会话保留的全部条目，按 seq 过滤后取前 {@code limit} 条。
     *
     * <p>同样是「多实例下 append 顺序 ≠ seq 顺序」的原因：List 上的下标区间无法对应 seq 区间，
     * 只能先全取再筛。{@code trim} 把保留量压在 {@value #MAX_LENGTH} 条，成本有上限；
     * 生产用 {@code JdbcMessageRepository} 的 {@code WHERE seq > ? ORDER BY seq LIMIT ?}。
     */
    @Override
    public List<StoredMessage> since(String conversationId, long afterSeq, int limit) {
        if (limit <= 0) {
            return List.of();
        }
        List<String> rows = redis.opsForList().range(RedisKeys.CONV_MESSAGES_PREFIX + conversationId, 0, -1);
        if (rows == null || rows.isEmpty()) {
            return List.of();
        }
        List<StoredMessage> matched = new ArrayList<>(rows.size());
        for (String row : rows) {
            if (row == null || row.isBlank()) {
                continue;
            }
            StoredMessage message = objectMapper.readValue(row, StoredMessage.class);
            if (message.seq() > afterSeq) {
                matched.add(message);
            }
        }
        matched.sort((left, right) -> Long.compare(left.seq(), right.seq()));
        return matched.size() <= limit ? matched : matched.subList(0, limit);
    }
}
