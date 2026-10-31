package com.github.im.server.message;

import com.github.im.server.conversation.ConversationStore;
import com.github.im.server.group.GroupMemberRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 新端回放的六条约定。
 *
 * <ol>
 *   <li><b>看的是「最近」</b>：最近活跃的 M 个会话、每个取最新 N 条，会话内按 seq 升序；</li>
 *   <li><b>与补发同源的脱敏</b>：本地删除的不回放，撤回的把正文抹掉；</li>
 *   <li><b>预算兜底</b>：总帧数有上限，超了要如实报 {@code truncated}，不能假装完整；</li>
 *   <li><b>单会话失败不拖垮整批</b>：一个会话读历史失败，其余会话照常回放；</li>
 *   <li><b>标记才是不回放的理由</b>：{@code build} 本身不落标记——落标记的时机在网关确认写成功之后
 *       （由 {@code MessageService.ackPending} 调 {@link NewDeviceReplayService#markReplayed}），
 *       否则写失败会让这台设备永远停在「服务端以为它有了」；</li>
 *   <li><b>显式请求绕过标记</b>：本地库被清空但 deviceId 没变时，只有这条路能再要一次。</li>
 * </ol>
 *
 * <p>不需要真实 Redis：标记在这里退化为进程内集合（构造器传 {@code redis = null}）。
 */
class NewDeviceReplayServiceTest {

    private static final String USER = "u-1";
    private static final String DEVICE = "phone";

    private final ConversationStore conversations = mock(ConversationStore.class);
    private final GroupMemberRepository groupMembers = mock(GroupMemberRepository.class);
    private final MessageRepository repository = mock(MessageRepository.class);

    @Test
    @DisplayName("最近 M 个会话 × 每个最近 N 条，按会话分组、会话内 seq 升序")
    void buildsRecentMessagesPerConversation() {
        when(groupMembers.groupsOf(USER)).thenReturn(List.of("g1"));
        when(conversations.recentConversations(eq(USER), eq(20), anySet()))
                .thenReturn(List.of("g:g1", "s:a:b"));
        when(repository.before(eq("g:g1"), eq(0L), eq(20)))
                .thenReturn(List.of(message("g:g1", 7), message("g:g1", 8)));
        when(repository.before(eq("s:a:b"), eq(0L), eq(20)))
                .thenReturn(List.of(message("s:a:b", 3)));

        NewDeviceReplayService.Replay replay = service(20, 20, 300).build(USER, DEVICE, false);

        assertEquals(3, replay.frames().size());
        assertEquals(2, replay.conversations());
        assertFalse(replay.truncated());
        assertTrue(replay.included());
        assertTrue(replay.frames().get(0).contains("\"seq\":7"), "会话内必须按 seq 升序");
        assertTrue(replay.frames().get(1).contains("\"seq\":8"));
        assertTrue(replay.frames().get(2).contains("\"seq\":3"));
    }

    @Test
    @DisplayName("本地删除的不回放，撤回的抹掉正文")
    void filtersDeletedAndRedactsRecalled() {
        when(conversations.recentConversations(eq(USER), anyInt(), anySet())).thenReturn(List.of("g:g1"));
        when(repository.before(eq("g:g1"), eq(0L), eq(20))).thenReturn(List.of(
                message("g:g1", 1), message("g:g1", 2), message("g:g1", 3)));
        when(repository.deletedSeqs(USER, "g:g1")).thenReturn(Set.of(1L));
        when(repository.recalledSeqs(eq("g:g1"), anyCollection())).thenReturn(Set.of(2L));

        NewDeviceReplayService.Replay replay = service(20, 20, 300).build(USER, DEVICE, false);

        assertEquals(2, replay.frames().size(), "seq=1 已被这个用户本地删除，不该出现在回放里");
        assertTrue(replay.frames().get(0).contains("\"recalled\":true"));
        assertTrue(replay.frames().get(0).contains("\"body\":\"\""), "撤回的消息不能把原文带出来");
        assertTrue(replay.frames().get(1).contains("\"seq\":3"));
    }

    @Test
    @DisplayName("总帧预算用尽即截断，并如实报告 truncated")
    void budgetTruncates() {
        when(conversations.recentConversations(eq(USER), eq(20), anySet()))
                .thenReturn(List.of("g:g1", "g:g2", "g:g3"));
        // 第一轮预算 3 → 第一会话拿 3 条上限（给 2 条），第二会话只剩 1 条预算
        when(repository.before(eq("g:g1"), eq(0L), eq(3)))
                .thenReturn(List.of(message("g:g1", 1), message("g:g1", 2)));
        when(repository.before(eq("g:g2"), eq(0L), eq(1)))
                .thenReturn(List.of(message("g:g2", 5)));

        NewDeviceReplayService.Replay replay = service(20, 20, 3).build(USER, DEVICE, false);

        assertEquals(3, replay.frames().size(), "不能超过总帧预算");
        assertEquals(2, replay.conversations());
        assertTrue(replay.truncated(), "第三个会话没被覆盖，必须如实报告");
    }

    @Test
    @DisplayName("单个会话读历史失败不影响其它会话，但整批标记为不完整")
    void singleConversationFailureDoesNotAbortTheBatch() {
        when(conversations.recentConversations(eq(USER), anyInt(), anySet()))
                .thenReturn(List.of("g:bad", "g:g2"));
        when(repository.before(eq("g:bad"), anyLong(), anyInt()))
                .thenThrow(new IllegalStateException("storage down"));
        when(repository.before(eq("g:g2"), anyLong(), anyInt()))
                .thenReturn(List.of(message("g:g2", 9)));

        NewDeviceReplayService.Replay replay = service(20, 20, 300).build(USER, DEVICE, false);

        assertEquals(1, replay.frames().size());
        assertEquals(1, replay.conversations());
        assertTrue(replay.truncated());
    }

    @Test
    @DisplayName("标记落下之后不再自动回放，但显式请求仍然给")
    void markerStopsAutomaticReplay() {
        when(conversations.recentConversations(eq(USER), anyInt(), anySet())).thenReturn(List.of("g:g1"));
        when(repository.before(eq("g:g1"), anyLong(), anyInt())).thenReturn(List.of(message("g:g1", 1)));
        NewDeviceReplayService service = service(20, 20, 300);

        assertTrue(service.build(USER, DEVICE, false).included());
        assertFalse(service.replayed(USER, DEVICE), "build 不许自己落标记（要等网关确认写成功）");

        service.markReplayed(USER, DEVICE);
        assertTrue(service.replayed(USER, DEVICE));
        assertFalse(service.build(USER, DEVICE, false).included(), "自动路径不再回放");
        assertTrue(service.build(USER, DEVICE, true).included(), "显式请求绕过标记");
    }

    @Test
    @DisplayName("用户没有任何会话时不回放（新注册用户不该收到一个空动作）")
    void noConversationsMeansNoReplay() {
        when(conversations.recentConversations(eq(USER), anyInt(), anySet())).thenReturn(List.of());

        NewDeviceReplayService.Replay replay = service(20, 20, 300).build(USER, DEVICE, false);

        assertFalse(replay.included());
        assertEquals(0, replay.conversations());
    }

    // ==== 辅助 ====

    private NewDeviceReplayService service(int maxConversations, int perConversation, int maxFrames) {
        // redis = null：标记退化为进程内集合，正好用来验证「build 不落标记」
        return new NewDeviceReplayService(conversations, groupMembers, repository, new ClientFrames(),
                null, true, maxConversations, perConversation, maxFrames, Duration.ofDays(30));
    }

    private static StoredMessage message(String conversationId, long seq) {
        return new StoredMessage(conversationId, seq, "srv-" + seq, "alice", "bob", "GROUP",
                "c-" + seq, "hi", 1_700_000_000_000L + seq, "TEXT", "");
    }
}
