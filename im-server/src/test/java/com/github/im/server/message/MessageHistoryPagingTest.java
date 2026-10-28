package com.github.im.server.message;

import com.github.im.server.metrics.MessageMetrics;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 历史消息游标翻页的约定（纯单元测试，不需要 Redis / 数据库）。
 *
 * <p>锁住四件事，每一件错了都会变成「用户上滑看到重复、漏消息，或永远卡在同一页」：
 *
 * <ol>
 *   <li><b>游标排他且按页前进</b>：{@code seq < beforeSeq}，翻页不重不漏；</li>
 *   <li><b>hasMore 用多要一条探测</b>：不额外做 count 查询，也不因为过滤而失真；</li>
 *   <li><b>整页被本地删除时游标仍然前进</b>：跟存储走，不跟可见消息走，否则客户端死循环；</li>
 *   <li><b>limit 由服务端夹取</b>：0 → 默认 50，超限 → 200（一次拉 1 万条会拖垮客户端）。</li>
 * </ol>
 */
class MessageHistoryPagingTest {

    private static final String CONV = "s:alice:bob";

    private final MessageRepository repository = mock(MessageRepository.class);

    @Test
    @DisplayName("翻页不重不漏：三页拼起来正好是全部消息，最后一页 hasMore=false")
    void pagesCoverEverythingExactlyOnce() {
        // 存储里有 seq 1..8；service 每次多要一条（limit+1）来探测 hasMore
        when(repository.before(CONV, 0L, 4)).thenReturn(messages(5, 6, 7, 8));
        when(repository.before(CONV, 6L, 4)).thenReturn(messages(2, 3, 4, 5));
        when(repository.before(CONV, 3L, 4)).thenReturn(messages(1, 2));
        when(repository.recalledSeqs(eq(CONV), anyCollection())).thenReturn(Set.of());

        MessageService.HistoryPage first = service().history("alice", CONV, 0L, 3);
        assertEquals(List.of(6L, 7L, 8L), seqs(first), "第一页给最新的 3 条，按 seq 升序");
        assertTrue(first.hasMore(), "更早的还有，客户端应继续上滑");
        assertEquals(6L, first.nextBeforeSeq(), "游标 = 本页最老一条，取 seq < 它 才是下一页");

        MessageService.HistoryPage second = service().history("alice", CONV, first.nextBeforeSeq(), 3);
        assertEquals(List.of(3L, 4L, 5L), seqs(second));
        assertTrue(second.hasMore());

        MessageService.HistoryPage third = service().history("alice", CONV, second.nextBeforeSeq(), 3);
        assertEquals(List.of(1L, 2L), seqs(third));
        assertFalse(third.hasMore(), "已经到会话开头了");
    }

    @Test
    @DisplayName("整页都被本地删除时，游标仍按存储前进（否则客户端永远卡在同一页）")
    void cursorAdvancesEvenWhenWholePageIsDeleted() {
        // limit=2 时服务端要 3 条探测 hasMore：最老的 7 只是探针，不消费
        when(repository.before(CONV, 0L, 3)).thenReturn(messages(7, 8, 9));
        when(repository.deletedSeqs("alice", CONV)).thenReturn(Set.of(8L, 9L));

        MessageService.HistoryPage page = service().history("alice", CONV, 0L, 2);

        assertTrue(page.messages().isEmpty(), "全被自己删了，可见消息为空");
        assertEquals(8L, page.nextBeforeSeq(),
                "游标 = 本页真正消费掉的最老一条（8，不是探针 7）：取 7 会把 7 永远跳过");
        assertTrue(page.hasMore(), "存储里还有更早的，hasMore 不受删除过滤影响");
    }

    @Test
    @DisplayName("空会话：没有消息时游标为 0、hasMore=false")
    void emptyConversation() {
        when(repository.before(CONV, 0L, 51)).thenReturn(List.of());

        MessageService.HistoryPage page = service().history("alice", CONV, 0L, 0);

        assertTrue(page.messages().isEmpty());
        assertEquals(0L, page.nextBeforeSeq(), "0 表示「没有更早的了」，客户端不必再请求");
        assertFalse(page.hasMore());
        assertEquals(50, page.limit(), "limit 不传时按默认 50 生效");
    }

    @Test
    @DisplayName("limit 由服务端夹取：0 → 50，超限 → 200")
    void limitIsClampedByServer() {
        when(repository.before(eq(CONV), anyLong(), org.mockito.ArgumentMatchers.anyInt())).thenReturn(List.of());

        assertEquals(50, service().history("alice", CONV, 0L, 0).limit());
        assertEquals(200, service().history("alice", CONV, 0L, 10_000).limit());
        assertEquals(200, service().history("alice", CONV, 0L, 200).limit());

        // 探测条数也要跟着夹取后的值走（201 = 200 + 1），否则 hasMore 会误判
        verify(repository, times(2)).before(CONV, 0L, 201);
        verify(repository).before(CONV, 0L, 51);
    }

    // ==== 辅助 ====

    private MessageService service() {
        // history 只用到 repository 与 frames；其余依赖给 null 即可（构造器只做赋值）
        return new MessageService(null, repository, null, new ClientFrames(new ObjectMapper()), null,
                null, null, null, null, null, null, null, null, Duration.ofMinutes(2),
                new MessageMetrics(null));
    }

    /** 升序构造 seq 列表对应的消息 */
    private static List<StoredMessage> messages(long... seqs) {
        List<StoredMessage> messages = new ArrayList<>(seqs.length);
        for (long seq : seqs) {
            messages.add(new StoredMessage(CONV, seq, "srv-" + seq, "alice", "bob", "SINGLE",
                    "c-" + seq, "hi-" + seq, 1_750_000_000_000L + seq));
        }
        return messages;
    }

    private static List<Long> seqs(MessageService.HistoryPage page) {
        List<Long> seqs = new ArrayList<>(page.messages().size());
        for (Map<String, Object> frame : page.messages()) {
            Object seq = frame.get("seq");
            seqs.add(seq instanceof Number n ? n.longValue() : Long.parseLong(String.valueOf(seq)));
        }
        return seqs;
    }
}
