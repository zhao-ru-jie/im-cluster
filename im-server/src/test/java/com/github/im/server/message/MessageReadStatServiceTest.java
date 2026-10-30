package com.github.im.server.message;

import com.github.im.contract.MessageEnvelope;
import com.github.im.server.conversation.ConversationStore;
import com.github.im.server.group.GroupMemberRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 群聊已读聚合的四条硬约定。
 *
 * <ol>
 *   <li><b>分母不含发送者</b>：他自己那条当然读过，算进分母会让「已读 3/3」永远差一个；</li>
 *   <li><b>计数全量、名单可截断</b>：{@code limit} 只影响 {@code members}，
 *       {@code readCount + unreadCount == total} 在任何 limit 下都成立；</li>
 *   <li><b>不是会话成员就查不到</b>：已读名单等于「谁在潜水」，属于隐私；</li>
 *   <li><b>调用者自己的已读位置取实时值</b>：快照缓存最多滞后 2 秒，但「我刚读完却显示未读」不行。</li>
 * </ol>
 *
 * <p>不需要真实存储：这里验证的是聚合口径，pipeline 的正确性由
 * {@code ConversationStoreTest#batchReadsCoverAllMembersInOneConversation} 用真 Redis 锁住。
 */
class MessageReadStatServiceTest {

    private static final String GROUP_CONV = "g:g1";
    private static final String SINGLE_CONV = "s:alice:bob";

    private final ConversationStore conversations = mock(ConversationStore.class);
    private final GroupMemberRepository groupMembers = mock(GroupMemberRepository.class);
    private final MessageRepository messageRepository = mock(MessageRepository.class);

    private MessageReadStatService service(Duration cacheTtl) {
        return new MessageReadStatService(conversations, groupMembers, messageRepository, cacheTtl);
    }

    @Test
    @DisplayName("已读人数不含发送者：3 人读过就是 3/3，不是 4/4")
    void readCountExcludesSender() {
        when(groupMembers.isMember("g1", "bob")).thenReturn(true);
        when(groupMembers.membersOf("g1")).thenReturn(List.of("alice", "bob", "carol", "dave"));
        when(conversations.readSeqsOf(eq(GROUP_CONV), anyCollection()))
                .thenReturn(Map.of("alice", 12L, "bob", 10L, "carol", 12L, "dave", 3L));
        when(conversations.readSeq("bob", GROUP_CONV)).thenReturn(10L);
        // alice 发了 seq=10 这条消息
        when(messageRepository.find(GROUP_CONV, 10))
                .thenReturn(Optional.of(message(GROUP_CONV, 10, "alice")));

        MessageReadStatService.ReadStatResult result = service(Duration.ZERO).stat(GROUP_CONV, "bob", 10, 0);

        assertEquals(0, result.code(), result.message());
        MessageReadStatService.ReadStat stat = result.stat();
        assertEquals(3, stat.total(), "alice 是发送者，不进分母");
        assertEquals(2, stat.readCount(), "bob(10) 与 carol(12) 读到了 10");
        assertEquals(1, stat.unreadCount(), "dave 只读到 3");
        assertEquals(10, stat.viewerReadSeq());
        assertFalse(stat.truncated());
        assertEquals(MessageEnvelope.CONV_GROUP, stat.convType());
        assertEquals(List.of("carol", "bob", "dave"), stat.members().stream()
                .map(MessageReadStatService.ReadStat.Member::userId).toList(),
                "已读在前、按 readSeq 倒序，未读排最后");
        assertTrue(stat.members().get(0).read());
        assertFalse(stat.members().get(2).read());
    }

    @Test
    @DisplayName("明细被 limit 截断时，计数仍是全量")
    void truncatedDetailsKeepFullCounts() {
        List<String> members = new ArrayList<>();
        members.add("alice");
        Map<String, Long> readSeqs = new LinkedHashMap<>();
        readSeqs.put("alice", 9L);
        for (int i = 0; i < 9; i++) {
            members.add("u" + i);
            readSeqs.put("u" + i, i < 5 ? 9L : 4L);
        }
        when(groupMembers.isMember("g1", "alice")).thenReturn(true);
        when(groupMembers.membersOf("g1")).thenReturn(members);
        when(conversations.readSeqsOf(eq(GROUP_CONV), anyCollection())).thenReturn(readSeqs);
        when(conversations.readSeq("alice", GROUP_CONV)).thenReturn(9L);
        when(messageRepository.find(GROUP_CONV, 9))
                .thenReturn(Optional.of(message(GROUP_CONV, 9, "alice")));

        MessageReadStatService.ReadStat stat =
                service(Duration.ZERO).stat(GROUP_CONV, "alice", 9, 2).stat();

        assertEquals(9, stat.total());
        assertEquals(5, stat.readCount());
        assertEquals(4, stat.unreadCount());
        assertEquals(2, stat.members().size(), "名单按 limit 截断");
        assertTrue(stat.truncated());
        assertEquals(stat.total(), stat.readCount() + stat.unreadCount());
    }

    @Test
    @DisplayName("不是群成员查不到已读名单")
    void nonMemberIsForbidden() {
        when(groupMembers.isMember("g1", "stranger")).thenReturn(false);

        MessageReadStatService.ReadStatResult result =
                service(Duration.ZERO).stat(GROUP_CONV, "stranger", 10, 0);

        assertEquals(403, result.code());
        assertNull(result.stat());
        verify(conversations, times(0)).readSeqsOf(anyString(), anyCollection());
    }

    @Test
    @DisplayName("单聊：分母只剩对端，发送者自己不参与")
    void singleChatOnlyCountsPeer() {
        when(conversations.readSeqsOf(eq(SINGLE_CONV), anyCollection()))
                .thenReturn(Map.of("alice", 7L, "bob", 7L));
        when(conversations.readSeq("alice", SINGLE_CONV)).thenReturn(7L);
        when(messageRepository.find(SINGLE_CONV, 7))
                .thenReturn(Optional.of(message(SINGLE_CONV, 7, "alice")));

        MessageReadStatService.ReadStat stat =
                service(Duration.ZERO).stat(SINGLE_CONV, "alice", 7, 0).stat();

        assertEquals(MessageEnvelope.CONV_SINGLE, stat.convType());
        assertEquals(1, stat.total(), "只有 bob 进分母");
        assertEquals(1, stat.readCount());
        assertEquals("bob", stat.members().get(0).userId());
    }

    @Test
    @DisplayName("快照缓存：同一会话连续查只打一次存储，同一份快照服务不同 seq")
    void snapshotCacheCollapsesRepeatedReads() {
        when(groupMembers.isMember("g1", "bob")).thenReturn(true);
        when(groupMembers.membersOf("g1")).thenReturn(List.of("bob", "carol"));
        when(conversations.readSeqsOf(eq(GROUP_CONV), anyCollection()))
                .thenReturn(Map.of("bob", 20L, "carol", 5L));
        when(conversations.readSeq("bob", GROUP_CONV)).thenReturn(20L);

        MessageReadStatService service = service(Duration.ofSeconds(5));
        MessageReadStatService.ReadStat first = service.stat(GROUP_CONV, "bob", 10, 0).stat();
        MessageReadStatService.ReadStat second = service.stat(GROUP_CONV, "bob", 20, 0).stat();

        assertEquals(2, first.total());
        assertEquals(1, first.readCount());
        assertEquals(1, second.readCount(), "同一份快照就能回答不同 seq 的问题");
        verify(conversations, times(1)).readSeqsOf(eq(GROUP_CONV), anyCollection());
    }

    @Test
    @DisplayName("seq 缺省时按会话当前 lastSeq 统计")
    void seqDefaultsToConversationLastSeq() {
        when(groupMembers.isMember("g1", "bob")).thenReturn(true);
        when(groupMembers.membersOf("g1")).thenReturn(List.of("bob", "carol"));
        when(conversations.readSeqsOf(eq(GROUP_CONV), anyCollection()))
                .thenReturn(Map.of("bob", 42L, "carol", 41L));
        when(conversations.readSeq("bob", GROUP_CONV)).thenReturn(42L);
        when(conversations.meta(GROUP_CONV)).thenReturn(new ConversationStore.ConversationMeta(
                GROUP_CONV, "GROUP", 42L, 1000L, "carol", "hi", "m42"));
        // 该 seq 的消息已被裁剪：查不到发送者，于是不排除任何人
        when(messageRepository.find(GROUP_CONV, 42)).thenReturn(Optional.empty());

        MessageReadStatService.ReadStat stat = service(Duration.ZERO).stat(GROUP_CONV, "bob", 0, 0).stat();

        assertEquals(42, stat.seq());
        assertEquals(1, stat.readCount());
        assertEquals(1, stat.unreadCount());
    }

    private static StoredMessage message(String conversationId, long seq, String from) {
        return new StoredMessage(conversationId, seq, "srv-" + seq, from, "", "GROUP", "c-" + seq,
                "hi", 1000L, "TEXT", null);
    }
}
