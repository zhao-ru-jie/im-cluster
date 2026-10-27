package com.github.im.client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.sqlite.SQLiteDataSource;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * JDBC 后端的真实回归：跑在 SQLite 文件库上（不是 mock 的 Connection）。
 *
 * <p>为什么坚持用真库：这层要验证的是「SQL 写得对不对」——拼错的列名、绑错的参数序号、
 * UPDATE 影响 0 行才 INSERT 的分支，全都只有真正执行才能暴露；mock 一个 Connection
 * 只能验证「我们调用了某个方法」，那是最没价值的一类测试。
 *
 * <p>语义断言与 {@link FileLocalMessageTableTest} 对齐：换持久化后端不该改变行为。
 */
class JdbcLocalMessageTableTest {

    private static final String CONV = "s:alice:bob";

    @TempDir
    Path dir;

    private String url;
    private JdbcLocalMessageTable table;

    @BeforeEach
    void setUp() {
        url = "jdbc:sqlite:" + dir.resolve("im-client.db").toAbsolutePath();
        table = new JdbcLocalMessageTable(dataSource(url));
    }

    @AfterEach
    void tearDown() {
        table.close();
    }

    @Test
    @DisplayName("重开连接后待发消息、已收消息、水位都还在")
    void survivesReopen() {
        long now = 1_700_000_000_000L;
        table.upsert(LocalMessage.outbound(OutgoingMessage.builder()
                .clientMsgId("c-1").to("bob").body("hi").build(), now));
        assertTrue(table.recordInbound(LocalMessage.inbound(push(3), now)));
        assertTrue(table.recordInbound(LocalMessage.inbound(push(5), now)));
        assertFalse(table.recordInbound(LocalMessage.inbound(push(5), now)), "同一 (会话, seq) 是重复");
        table.close();

        JdbcLocalMessageTable reopened = new JdbcLocalMessageTable(dataSource(url));
        try {
            LocalMessage row = reopened.find("c-1").orElseThrow();
            assertEquals(LocalMessage.Status.PENDING, row.status());
            assertEquals("hi", row.body());
            assertEquals("bob", row.to());
            assertEquals(5, reopened.lastInboundSeq(CONV).orElseThrow());
            assertEquals(Map.of(CONV, 5L), reopened.inboundWatermarks());
            assertEquals(1, reopened.pendingDue(now + 1_000, 10).size());
            assertEquals(3, reopened.size());
        } finally {
            reopened.close();
        }
    }

    @Test
    @DisplayName("重投时间与状态机：PENDING 到点才捞得出来，SENT 之后不再出现")
    void pendingDueAndMarkSent() {
        long now = 1_700_000_000_000L;
        table.upsert(LocalMessage.outbound(OutgoingMessage.builder()
                .clientMsgId("c-1").to("bob").body("hi").build(), now));

        assertEquals(1, table.pendingDue(now, 10).size());
        table.upsert(table.find("c-1").orElseThrow().withAttempt(now + 5_000, "write failed", now));
        assertEquals(0, table.pendingDue(now, 10).size(), "推到未来后不该被立刻重投");
        assertEquals(1, table.pendingDue(now + 5_001, 10).size());

        table.upsert(table.find("c-1").orElseThrow().withSent("srv-1", 7, now));
        LocalMessage sent = table.find("c-1").orElseThrow();
        assertEquals(LocalMessage.Status.SENT, sent.status());
        assertEquals("srv-1", sent.serverMsgId());
        assertEquals(7, sent.seq());
        assertEquals(0, table.pendingDue(now + 60_000, 10).size());
    }

    @Test
    @DisplayName("历史按最近 N 条取，返回时按 seq 升序")
    void inboundReturnsLatestAscending() {
        long now = 1_700_000_000_000L;
        for (int seq = 1; seq <= 10; seq++) {
            assertTrue(table.recordInbound(LocalMessage.inbound(push(seq), now)));
        }

        List<LocalMessage> latest = table.inbound(CONV, 3);

        assertEquals(List.of(8L, 9L, 10L), latest.stream().map(LocalMessage::seq).toList());
        assertEquals(10, table.lastInboundSeq(CONV).orElseThrow());
    }

    @Test
    @DisplayName("多会话水位互不影响")
    void watermarksPerConversation() {
        long now = 1_700_000_000_000L;
        table.recordInbound(LocalMessage.inbound(push(4), now));
        table.recordInbound(LocalMessage.inbound(new DownlinkFrame.Push("g:g1", "GROUP", 9,
                "srv-9", "c-9", "alice", null, "hi", "TEXT", null, false, now), now));

        assertEquals(Map.of(CONV, 4L, "g:g1", 9L), table.inboundWatermarks());
    }

    private static DataSource dataSource(String url) {
        SQLiteDataSource dataSource = new SQLiteDataSource();
        dataSource.setUrl(url);
        return dataSource;
    }

    private static DownlinkFrame.Push push(long seq) {
        return new DownlinkFrame.Push(CONV, "SINGLE", seq, "srv-" + seq, "c-" + seq,
                "bob", "alice", "hi", "TEXT", null, false, 1_700_000_000_000L);
    }
}
