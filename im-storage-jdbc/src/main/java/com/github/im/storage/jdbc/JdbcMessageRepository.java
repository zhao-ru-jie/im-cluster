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
 */
public class JdbcMessageRepository implements MessageRepository {

    private static final String INSERT_SQL = """
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

    public JdbcMessageRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void append(StoredMessage message) {
        jdbc.update(INSERT_SQL,
                message.conversationId(),
                message.seq(),
                message.serverMsgId(),
                message.from(),
                message.to(),
                message.convType(),
                message.clientMsgId(),
                message.body(),
                message.createdAt());
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
