package com.github.im.client;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * 基于 JDBC 的本地消息表（SQLite / H2 / MySQL 等均可，由宿主提供 {@link DataSource} 与驱动）。
 *
 * <h2>为什么 SDK 不自带驱动</h2>
 *
 * <p>驱动的选择是<b>宿主应用的部署决定</b>：Android 用系统 SQLite API、桌面用 xerial、
 * 服务端可能直接复用已有的连接池。SDK 若钉死一个驱动，要么把宿主的连接池顶掉，
 * 要么在 Android 上打不进 JNI 库。这里只依赖 JDK 自带的 {@code javax.sql}，
 * 宿主给什么用什么——测试里才引入 sqlite-jdbc（{@code test} 作用域）。
 *
 * <h2>SQL 只用可移植语法</h2>
 *
 * <p>没有用 {@code ON CONFLICT}/{@code ON DUPLICATE KEY}/{@code MERGE}：这三个在 SQLite、MySQL、H2
 * 之间互不通用。改用「先 UPDATE，影响 0 行再 INSERT」，一个事务内完成，所有库都支持。
 * 并发写不用考虑：同一个 SDK 实例只有一条连接在写（宿主多实例各写各的文件/库）。
 *
 * <h2>与追加日志实现的差别</h2>
 *
 * <p>这里<b>没有内存索引兜底</b>：每次查询都走 SQL。好处是数据量与内存无关（聊天记录能到几十万条），
 * 代价是落库失败会抛 {@link IllegalStateException}——静默吞掉就等于放弃了「先落表再上网」这个前提。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public final class JdbcLocalMessageTable implements LocalMessageTable {

    private static final String TABLE = "im_local_message";
    private static final String COLUMNS =
            "client_msg_id, direction, status, conversation_id, conv_type, from_user, to_user, body, "
                    + "content_type, attrs, trace_id, client_ts, server_msg_id, seq, attempts, "
                    + "next_attempt_at, last_error, created_at, updated_at";

    private final DataSource dataSource;
    private volatile boolean closed;

    public JdbcLocalMessageTable(DataSource dataSource) {
        this.dataSource = dataSource;
        initSchema();
    }

    /** 建表 + 建索引（幂等）。宿主也可以自己按同样的 DDL 预建，便于统一迁移脚本。 */
    private void initSchema() {
        execute("""
                CREATE TABLE IF NOT EXISTS im_local_message (
                  client_msg_id   VARCHAR(128) PRIMARY KEY,
                  direction       VARCHAR(8)  NOT NULL,
                  status          VARCHAR(16) NOT NULL,
                  conversation_id VARCHAR(128),
                  conv_type       VARCHAR(16),
                  from_user       VARCHAR(128),
                  to_user         VARCHAR(128),
                  body            TEXT,
                  content_type    VARCHAR(32),
                  attrs           TEXT,
                  trace_id        VARCHAR(64),
                  client_ts       BIGINT,
                  server_msg_id   VARCHAR(128),
                  seq             BIGINT,
                  attempts        INTEGER,
                  next_attempt_at BIGINT,
                  last_error      VARCHAR(255),
                  created_at      BIGINT,
                  updated_at      BIGINT
                )""");
        // 待发扫描是高频查询（每 outboxScanIntervalMs 一次），必须走索引
        execute("CREATE INDEX IF NOT EXISTS idx_im_local_outbox ON im_local_message "
                + "(direction, status, next_attempt_at)");
        execute("CREATE INDEX IF NOT EXISTS idx_im_local_conv ON im_local_message "
                + "(conversation_id, seq)");
    }

    private void execute(String sql) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.execute();
        } catch (SQLException e) {
            throw new IllegalStateException("初始化本地消息表失败：" + e.getMessage(), e);
        }
    }

    @Override
    public synchronized void upsert(LocalMessage message) {
        try (Connection conn = dataSource.getConnection()) {
            boolean autoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                if (update(conn, message) == 0) {
                    insert(conn, message);
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(autoCommit);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("写入本地消息表失败：" + e.getMessage(), e);
        }
    }

    @Override
    public synchronized Optional<LocalMessage> find(String clientMsgId) {
        if (clientMsgId == null) {
            return Optional.empty();
        }
        return queryOne("SELECT " + COLUMNS + " FROM " + TABLE + " WHERE client_msg_id = ?", clientMsgId);
    }

    @Override
    public synchronized List<LocalMessage> pendingDue(long nowMs, int limit) {
        String sql = "SELECT " + COLUMNS + " FROM " + TABLE
                + " WHERE direction = 'OUT' AND status = 'PENDING' AND next_attempt_at <= ?"
                + " ORDER BY next_attempt_at, created_at LIMIT ?";
        return queryList(sql, nowMs, Math.max(1, limit));
    }

    @Override
    public synchronized List<LocalMessage> outbound(int limit) {
        String sql = "SELECT " + COLUMNS + " FROM " + TABLE
                + " WHERE direction = 'OUT' ORDER BY created_at LIMIT ?";
        return queryList(sql, Math.max(1, limit));
    }

    /**
     * 落一条已收消息并去重。
     *
     * <p>去重靠主键（{@code 会话#seq} 就是 {@code client_msg_id}）：先 SELECT 判存在再 INSERT，
     * 而不是靠捕获唯一约束异常——各驱动抛的异常类型不统一，而这里的并发写只来自一条连接。
     */
    @Override
    public synchronized boolean recordInbound(LocalMessage message) {
        if (message.seq() <= 0) {
            return false;
        }
        if (find(message.clientMsgId()).isPresent()) {
            return false;
        }
        upsert(message);
        return true;
    }

    @Override
    public synchronized OptionalLong lastInboundSeq(String conversationId) {
        String sql = "SELECT MAX(seq) AS max_seq FROM " + TABLE
                + " WHERE direction = 'IN' AND conversation_id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, conversationId);
            try (ResultSet rows = statement.executeQuery()) {
                if (rows.next()) {
                    long value = rows.getLong("max_seq");
                    if (!rows.wasNull()) {
                        return OptionalLong.of(value);
                    }
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("查询会话水位失败：" + e.getMessage(), e);
        }
        return OptionalLong.empty();
    }

    @Override
    public synchronized Map<String, Long> inboundWatermarks() {
        String sql = "SELECT conversation_id, MAX(seq) AS max_seq FROM " + TABLE
                + " WHERE direction = 'IN' AND conversation_id IS NOT NULL GROUP BY conversation_id";
        Map<String, Long> watermarks = new LinkedHashMap<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql);
             ResultSet rows = statement.executeQuery()) {
            while (rows.next()) {
                watermarks.put(rows.getString("conversation_id"), rows.getLong("max_seq"));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("查询水位失败：" + e.getMessage(), e);
        }
        return watermarks;
    }

    @Override
    public synchronized List<LocalMessage> inbound(String conversationId, int limit) {
        // 先取最近 limit 条（DESC），再按 seq 升序返回：调用方要的是「最后几条」
        String sql = "SELECT " + COLUMNS + " FROM " + TABLE
                + " WHERE direction = 'IN' AND conversation_id = ? ORDER BY seq DESC LIMIT ?";
        List<LocalMessage> rows = queryList(sql, conversationId, Math.max(1, limit));
        Collections.reverse(rows);
        return rows;
    }

    @Override
    public synchronized int size() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement("SELECT COUNT(*) AS total FROM " + TABLE);
             ResultSet rows = statement.executeQuery()) {
            return rows.next() ? rows.getInt("total") : 0;
        } catch (SQLException e) {
            throw new IllegalStateException("统计本地消息表失败：" + e.getMessage(), e);
        }
    }

    /** 只标记关闭：{@link DataSource} 由宿主拥有，SDK 不负责关连接池 */
    @Override
    public void close() {
        closed = true;
    }

    // ==== SQL 细节 ====

    private int update(Connection conn, LocalMessage message) throws SQLException {
        String sql = "UPDATE " + TABLE + " SET direction = ?, status = ?, conversation_id = ?, "
                + "conv_type = ?, from_user = ?, to_user = ?, body = ?, content_type = ?, attrs = ?, "
                + "trace_id = ?, client_ts = ?, server_msg_id = ?, seq = ?, attempts = ?, "
                + "next_attempt_at = ?, last_error = ?, created_at = ?, updated_at = ? "
                + "WHERE client_msg_id = ?";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int index = bindAll(statement, message);
            statement.setString(index, message.clientMsgId());
            return statement.executeUpdate();
        }
    }

    private void insert(Connection conn, LocalMessage message) throws SQLException {
        String sql = "INSERT INTO " + TABLE + " (" + COLUMNS + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, message.clientMsgId());
            bindTail(statement, message, 2);
            statement.executeUpdate();
        }
    }

    private int bindAll(PreparedStatement statement, LocalMessage message) throws SQLException {
        bindTail(statement, message, 1);
        return 19;
    }

    private void bindTail(PreparedStatement statement, LocalMessage message, int start) throws SQLException {
        statement.setString(start, message.direction().name());
        statement.setString(start + 1, message.status().name());
        statement.setString(start + 2, message.conversationId());
        statement.setString(start + 3, message.convType());
        statement.setString(start + 4, message.from());
        statement.setString(start + 5, message.to());
        statement.setString(start + 6, message.body());
        statement.setString(start + 7, message.contentType());
        statement.setString(start + 8, message.attrs());
        statement.setString(start + 9, message.traceId());
        statement.setLong(start + 10, message.clientTs());
        statement.setString(start + 11, message.serverMsgId());
        statement.setLong(start + 12, message.seq());
        statement.setInt(start + 13, message.attempts());
        statement.setLong(start + 14, message.nextAttemptAt());
        statement.setString(start + 15, message.lastError());
        statement.setLong(start + 16, message.createdAt());
        statement.setLong(start + 17, message.updatedAt());
    }

    private Optional<LocalMessage> queryOne(String sql, String parameter) {
        List<LocalMessage> rows = queryList(sql, parameter);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    private List<LocalMessage> queryList(String sql, Object... parameters) {
        ensureOpen();
        List<LocalMessage> rows = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.length; i++) {
                statement.setObject(i + 1, parameters[i]);
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    rows.add(map(resultSet));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("查询本地消息表失败：" + e.getMessage(), e);
        }
        return rows;
    }

    private static LocalMessage map(ResultSet rows) throws SQLException {
        return new LocalMessage(
                rows.getString("client_msg_id"),
                LocalMessage.Direction.valueOf(rows.getString("direction")),
                LocalMessage.Status.valueOf(rows.getString("status")),
                rows.getString("conversation_id"),
                rows.getString("conv_type"),
                rows.getString("from_user"),
                rows.getString("to_user"),
                rows.getString("body"),
                rows.getString("content_type"),
                rows.getString("attrs"),
                rows.getString("trace_id"),
                rows.getLong("client_ts"),
                rows.getString("server_msg_id"),
                rows.getLong("seq"),
                rows.getInt("attempts"),
                rows.getLong("next_attempt_at"),
                rows.getString("last_error"),
                rows.getLong("created_at"),
                rows.getLong("updated_at"));
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("本地消息表已关闭");
        }
    }
}
