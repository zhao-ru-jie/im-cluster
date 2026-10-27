package com.github.im.client;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 本地消息表的语义：<b>重启不丢</b>、<b>补发去重</b>、<b>水位可从表恢复</b>、<b>日志不无限膨胀</b>。
 *
 * <p>前三条任何一条破了，用户看到的现象都很具体：
 * 重启后「发送中的消息消失」、同一条消息显示两遍、或者 Redis 归零后新消息被当历史丢弃。
 */
class FileLocalMessageTableTest {

    private static final String CONV = "s:alice:bob";

    @Test
    @DisplayName("重启后待发消息与已收水位都还在（表就是客户端的持久化真相）")
    void survivesRestart(@TempDir Path dir) throws IOException {
        Path journal = dir.resolve("messages.jsonl");
        OutgoingMessage message = OutgoingMessage.builder()
                .clientMsgId("c-1").to("bob").body("hi").build();
        long now = 1_700_000_000_000L;

        try (FileLocalMessageTable table = new FileLocalMessageTable(journal)) {
            table.upsert(LocalMessage.outbound(message, now));
            assertTrue(table.recordInbound(LocalMessage.inbound(push(3, "srv-3"), now)));
            assertTrue(table.recordInbound(LocalMessage.inbound(push(5, "srv-5"), now)));
            // 同一 seq 再来一次：补发重复，必须是「丢弃」而不是「覆盖」或异常
            assertFalse(table.recordInbound(LocalMessage.inbound(push(5, "srv-5-dup"), now)));
        }

        try (FileLocalMessageTable reopened = new FileLocalMessageTable(journal)) {
            LocalMessage row = reopened.find("c-1").orElseThrow();
            assertEquals(LocalMessage.Status.PENDING, row.status());
            assertEquals("hi", row.body());
            assertEquals("bob", row.to());

            assertEquals(5, reopened.lastInboundSeq(CONV).orElseThrow());
            Map<String, Long> watermarks = reopened.inboundWatermarks();
            assertEquals(Map.of(CONV, 5L), watermarks, "水位是 sync 帧的内容，重启后必须还在");

            assertEquals(1, reopened.pendingDue(now + 1_000, 10).size());
            assertEquals(3, reopened.size(), "1 条待发 + 2 条已收");
        }
    }

    @Test
    @DisplayName("已收消息按 (会话, seq) 精确去重，乱序补发的新 seq 仍要入库")
    void inboundDedupesAndKeepsOrder() {
        InMemoryLocalMessageTable table = new InMemoryLocalMessageTable();
        long now = 1_700_000_000_000L;

        assertTrue(table.recordInbound(LocalMessage.inbound(push(2, "srv-2"), now)));
        assertTrue(table.recordInbound(LocalMessage.inbound(push(9, "srv-9"), now)));
        // 乱序补发：seq 5 比高水位小，但它确实是没收到过的消息，必须入库
        assertTrue(table.recordInbound(LocalMessage.inbound(push(5, "srv-5"), now)),
                "用高水位一刀切会把「没收到过、只是比高水位小」的消息误删");
        // 精确重复（至少一次补发的正常现象）才丢
        assertFalse(table.recordInbound(LocalMessage.inbound(push(5, "srv-5-again"), now)));
        assertFalse(table.recordInbound(LocalMessage.inbound(push(9, "srv-9-again"), now)));

        List<LocalMessage> rows = table.inbound(CONV, 10);
        assertEquals(List.of(2L, 5L, 9L), rows.stream().map(LocalMessage::seq).toList());
        assertEquals(9, table.lastInboundSeq(CONV).orElseThrow());
    }

    @Test
    @DisplayName("未到点的消息不会被 pendingDue 捞出来（至少一次 ≠ 立刻重复发）")
    void pendingDueHonoursNextAttemptAt() {
        InMemoryLocalMessageTable table = new InMemoryLocalMessageTable();
        long now = 1_700_000_000_000L;
        OutgoingMessage message = OutgoingMessage.builder().clientMsgId("c-1").to("bob").body("hi").build();

        table.upsert(LocalMessage.outbound(message, now));
        assertEquals(1, table.pendingDue(now, 10).size());

        table.upsert(table.find("c-1").orElseThrow().withAttempt(now + 5_000, now));
        assertEquals(0, table.pendingDue(now, 10).size(), "推到未来之后不该再被立刻重投");
        assertEquals(1, table.pendingDue(now + 5_001, 10).size());

        table.upsert(table.find("c-1").orElseThrow().withSent("srv-1", 7, now));
        assertEquals(0, table.pendingDue(now + 60_000, 10).size(), "已受理的消息不该再重投");
    }

    @Test
    @DisplayName("日志会被 compaction 回收：上千次状态变更后文件不跟着无限膨胀")
    void compactsJournal(@TempDir Path dir) throws IOException {
        Path journal = dir.resolve("messages.jsonl");
        OutgoingMessage message = OutgoingMessage.builder()
                .clientMsgId("c-1").to("bob").body("hi").build();
        long base = 1_700_000_000_000L;

        try (FileLocalMessageTable table = new FileLocalMessageTable(journal)) {
            // 模拟「反复重投/重试」：同一条消息被写 1200 次
            for (int i = 0; i < 1200; i++) {
                table.upsert(LocalMessage.outbound(message, base + i));
            }
            assertEquals(1, table.size());
        }

        long lines;
        try (var stream = Files.lines(journal, StandardCharsets.UTF_8)) {
            lines = stream.filter(line -> !line.isBlank()).count();
        }
        assertTrue(lines < 300, "1200 次变更后日志仍有 " + lines + " 行，说明 compaction 没生效");

        try (FileLocalMessageTable reopened = new FileLocalMessageTable(journal)) {
            // compaction 之后剩下的必须是「最新状态」，而不是被清空
            assertEquals(base + 1199, reopened.find("c-1").orElseThrow().createdAt());
        }
    }

    @Test
    @DisplayName("坏行只跳过自身，不影响其它记录（一行损坏不该毁掉整张表）")
    void skipsBrokenLines(@TempDir Path dir) throws IOException {
        Path journal = dir.resolve("messages.jsonl");
        long now = 1_700_000_000_000L;
        try (FileLocalMessageTable table = new FileLocalMessageTable(journal)) {
            table.upsert(LocalMessage.outbound(
                    OutgoingMessage.builder().clientMsgId("c-1").to("bob").body("hi").build(), now));
        }
        Files.writeString(journal, "{ this is not json }\n", StandardCharsets.UTF_8,
                java.nio.file.StandardOpenOption.APPEND);

        try (FileLocalMessageTable reopened = new FileLocalMessageTable(journal)) {
            assertTrue(reopened.find("c-1").isPresent());
        }
    }

    private static DownlinkFrame.Push push(long seq, String serverMsgId) {
        return new DownlinkFrame.Push(CONV, "SINGLE", seq, serverMsgId, "c-" + seq,
                "alice", "bob", "hi", "TEXT", null, false, 1_700_000_000_000L);
    }
}
