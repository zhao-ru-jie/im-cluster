package com.github.im.storage.redis;

import com.github.im.contract.RedisKeys;
import com.github.im.server.message.MessageRepository;
import com.github.im.server.message.StoredMessage;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Redis 版消息存储（{@code im.storage=redis}，缺省形态）：
 * {@code im:conv:msgs:{conversationId}} 存 {@link StoredMessage} 的 JSON。
 *
 * <pre>im:conv:msgs:{conversationId}   List，右进左出，只保留最近 N 条
 * im:conv:recalled:{conversationId}  Set，已撤回的 seq
 * im:user:deleted:{userId}:{conv}    Set，该用户在这个会话里本地删掉的 seq</pre>
 *
 * <p>定位：本地开发 / 联调 / 小规模。生产用
 * {@code com.github.im.storage.jdbc.JdbcMessageRepository}（{@code im.storage=jdbc}，位于
 * im-storage-jdbc 模块）——Redis List 无法做按时间分区、无法跨会话检索、容量受内存限制。
 *
 * <p>放在 im-storage-redis 而不是 im-server：与 im-storage-jdbc 对称，介质相关的代码
 * （Redis 键结构、List/Set 语义、pipeline 用法）不进入业务模块；切换只改 {@code im.storage}。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
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
        String key = RedisKeys.convMessages(message.conversationId());
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
        List<String> rows = redis.opsForList().range(RedisKeys.convMessages(conversationId), 0, -1);
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

    /**
     * 向上翻页：{@code seq < beforeSeq} 的升序区间（{@code beforeSeq <= 0} = 取最新一页）。
     *
     * <p>不能像「取最新 N 条」那样用下标区间 {@code LRANGE -limit -1}：List 的物理顺序不等于
     * seq 顺序（多实例下 {@code append} 的到达顺序可能乱，理由同 {@link #maxSeq}），
     * 下标区间无法对应 seq 区间。所以先全取（{@code trim} 已把保留量压在
     * {@value #MAX_LENGTH} 条，成本有上限），再按 seq 筛、排序、取尾部 limit 条。
     *
     * <p>生产形态不背这个成本：{@code JdbcMessageRepository} / {@code MongoMessageRepository}
     * 都是「按 seq 倒序 + LIMIT」，命中索引区间。
     */
    @Override
    public List<StoredMessage> before(String conversationId, long beforeSeq, int limit) {
        if (limit <= 0) {
            return List.of();
        }
        List<String> rows = redis.opsForList().range(RedisKeys.convMessages(conversationId), 0, -1);
        if (rows == null || rows.isEmpty()) {
            return List.of();
        }
        List<StoredMessage> matched = new ArrayList<>(rows.size());
        for (String row : rows) {
            if (row == null || row.isBlank()) {
                continue;
            }
            StoredMessage message = objectMapper.readValue(row, StoredMessage.class);
            if (beforeSeq <= 0 || message.seq() < beforeSeq) {
                matched.add(message);
            }
        }
        matched.sort(Comparator.comparingLong(StoredMessage::seq));
        if (matched.size() <= limit) {
            return matched;
        }
        // 只要最新的一页：游标翻页要的是「紧挨着 beforeSeq 往前的那 limit 条」
        return new ArrayList<>(matched.subList(matched.size() - limit, matched.size()));
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
        List<String> rows = redis.opsForList().range(RedisKeys.convMessages(conversationId), 0, -1);
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

    // ==== 消息生命周期（撤回 / 删除） ====

    @Override
    public Optional<StoredMessage> find(String conversationId, long seq) {
        List<String> rows = redis.opsForList().range(RedisKeys.convMessages(conversationId), 0, -1);
        if (rows == null || rows.isEmpty()) {
            return Optional.empty();
        }
        for (String row : rows) {
            if (row == null || row.isBlank()) {
                continue;
            }
            StoredMessage message = objectMapper.readValue(row, StoredMessage.class);
            if (message.seq() == seq) {
                return Optional.of(message);
            }
        }
        return Optional.empty();
    }

    /**
     * {@code SADD} 的返回值就是「是否首次」：重复撤回返回 0，据此避免重复广播变更帧。
     *
     * <p>撤回集合不设 TTL：消息本身（List）也没有过期，集合先过期就等于把撤回记录弄丢、
     * 让原文重新可见。这条实现只用于本地/小规模，集合规模由 {@value #MAX_LENGTH} 的保留量兜住。
     */
    @Override
    public boolean recall(String conversationId, long seq) {
        if (seq <= 0) {
            return false;
        }
        Long added = redis.opsForSet().add(recalledKey(conversationId), String.valueOf(seq));
        return added != null && added > 0;
    }

    /**
     * 批量判定是否已撤回。
     *
     * <p>用 pipeline 逐个 {@code SISMEMBER} 而不是 {@code SMISMEMBER}：本机联调的 Redis 是 3.2，
     * 后者 6.2 才有。一次补发最多两三百帧，pipeline 的往返开销只有一次。
     */
    @Override
    public Set<Long> recalledSeqs(String conversationId, Collection<Long> seqs) {
        List<Long> ordered = List.copyOf(new LinkedHashSet<>(seqs));
        if (ordered.isEmpty()) {
            return Set.of();
        }
        String key = recalledKey(conversationId);
        List<Object> raw = redis.executePipelined((RedisCallback<Object>) connection -> {
            for (Long seq : ordered) {
                connection.setCommands().sIsMember(key.getBytes(StandardCharsets.UTF_8),
                        String.valueOf(seq).getBytes(StandardCharsets.UTF_8));
            }
            return null;
        });
        Set<Long> recalled = new LinkedHashSet<>();
        for (int i = 0; i < ordered.size() && i < raw.size(); i++) {
            if (Boolean.TRUE.equals(raw.get(i))) {
                recalled.add(ordered.get(i));
            }
        }
        return recalled;
    }

    @Override
    public void markDeleted(String userId, String conversationId, long seq) {
        if (seq <= 0 || userId == null || userId.isBlank()) {
            return;
        }
        redis.opsForSet().add(deletedKey(userId, conversationId), String.valueOf(seq));
    }

    @Override
    public Set<Long> deletedSeqs(String userId, String conversationId) {
        if (userId == null || userId.isBlank()) {
            return Set.of();
        }
        Set<String> members = redis.opsForSet().members(deletedKey(userId, conversationId));
        if (members == null || members.isEmpty()) {
            return Set.of();
        }
        Set<Long> seqs = new LinkedHashSet<>(members.size());
        for (String member : members) {
            try {
                seqs.add(Long.parseLong(member));
            } catch (NumberFormatException ignored) {
                // 脏数据不该让整页历史失败
            }
        }
        return seqs;
    }

    private static String recalledKey(String conversationId) {
        return RedisKeys.convRecalled(conversationId);
    }

    private static String deletedKey(String userId, String conversationId) {
        return RedisKeys.userDeleted(userId, conversationId);
    }
}
