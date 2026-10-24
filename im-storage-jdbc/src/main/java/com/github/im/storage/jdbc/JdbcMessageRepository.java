package com.github.im.storage.jdbc;

import com.github.im.server.message.MessageRepository;
import com.github.im.server.message.StoredMessage;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * MySQL 版消息存储（生产形态，`im.storage=jdbc`）。
 *
 * <p>表结构见 {@code db/schema.sql}，两条关键索引：
 * <ul>
 *   <li>{@code uk_msg_idem (from_user, client_msg_id)} —— 幂等的<b>最终防线</b>：
 *       Redis 幂等表 TTL 到期或丢数据时，靠唯一索引仍能拒绝重复落库；</li>
 *   <li>{@code uk_msg_conv_seq (conversation_id, seq)} —— 保序与漫游拉取的主索引。</li>
 * </ul>
 *
 * <p>注意：{@code seq} 由 {@code SeqAllocator} 逐条 Redis 原子递增分配，并周期性用
 * {@link #maxSeq(String)} 读到的「已落库最大 seq」当下界（见 {@code SeqAllocator} 的 Lua）。
 * 因此即使 Redis 里的序号计数器被清空，新号也从历史之后接着发，不会撞
 * {@code uk_msg_conv_seq}；唯一索引退化为「兜底断言」而非「常态错误源」。
 * {@code server_msg_id} 由 {@code MessageIds}（UUIDv7）独立生成，与 seq 无关，不受回绕影响。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class JdbcMessageRepository implements MessageRepository, AutoCloseable {

    /** 包内可见：{@link JdbcBatchAppender} 复用同一条 SQL */
    static final String INSERT_SQL = """
            INSERT INTO im_message
                (conversation_id, seq, server_msg_id, from_user, to_user, conv_type, client_msg_id, body, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    /** 先按 seq 倒序取最近 limit 条，再反转成升序返回 */
    private static final String RECENT_SQL = """
            SELECT conversation_id, seq, server_msg_id, from_user, to_user, conv_type, client_msg_id, body, created_at
              FROM im_message
             WHERE conversation_id = ?
             ORDER BY seq DESC
             LIMIT ?
            """;

    /** seq 发号器的下界：已落库的最大 seq（走 uk_msg_conv_seq 索引尾部） */
    private static final String MAX_SEQ_SQL = """
            SELECT COALESCE(MAX(seq), 0) FROM im_message WHERE conversation_id = ?
            """;

    private final JdbcTemplate jdbc;
    private final JdbcBatchAppender appender;

    /**
     * @param batchEnabled 是否启用 group commit（{@code im.db.batch-enabled}）。
     *                     关掉后每条消息单独自动提交，便于对照压测与排障。
     */
    public JdbcMessageRepository(JdbcTemplate jdbc, boolean batchEnabled) {
        this.jdbc = jdbc;
        this.appender = new JdbcBatchAppender(jdbc, batchEnabled);
    }

    /** 落库并在返回前确认已提交（批处理时等本批 flush） */
    @Override
    public void append(StoredMessage message) {
        appender.append(message);
    }

    /** 关闭时把队列里还没提交的消息刷完，避免「已回 ack 但没落库」 */
    @Override
    public void close() {
        appender.close();
    }

    /** 走 {@code uk_msg_conv_seq} 的最后一段（索引下推取 MAX），一条索引查找即得 */
    @Override
    public long maxSeq(String conversationId) {
        Long max = jdbc.queryForObject(MAX_SEQ_SQL, Long.class, conversationId);
        return max == null ? 0L : max;
    }

    @Override
    public List<StoredMessage> recent(String conversationId, int limit) {
        List<StoredMessage> messages = new ArrayList<>(jdbc.query(RECENT_SQL,
                (rs, rowNum) -> new StoredMessage(
                        rs.getString("conversation_id"),
                        rs.getLong("seq"),
                        rs.getString("server_msg_id"),
                        rs.getString("from_user"),
                        rs.getString("to_user"),
                        rs.getString("conv_type"),
                        rs.getString("client_msg_id"),
                        rs.getString("body"),
                        rs.getLong("created_at")),
                conversationId, limit));
        Collections.reverse(messages);
        return messages;
    }
}
