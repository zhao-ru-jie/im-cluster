package com.github.im.server.presence;

import com.github.im.proto.presence.v1.GetPresenceRequest;
import com.github.im.proto.presence.v1.GetPresenceResponse;
import com.github.im.proto.presence.v1.PresenceReport;
import com.github.im.proto.presence.v1.PresenceReportAck;
import com.github.im.proto.presence.v1.PresenceState;
import com.github.im.proto.presence.v1.TypingRequest;
import com.github.im.proto.presence.v1.TypingResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * presence / typing 的业务判定。
 *
 * <p>这里不碰 Redis：{@link PresenceStore} 用替身，因为要锁的是<b>判定规则</b>
 * （什么时候算上线、什么时候不能广播下线），而不是 key 长什么样——后者属于
 * {@code PresenceStore} 自己的关注点。
 *
 * <p>最要紧的一条是 {@link #offlineReportIsIgnoredWhileStillOnline()}：
 * 「错的下线」比「漏报的上线」严重得多。漏报只影响一次更新，错报会让好友头像一直灰着，
 * 直到下一次状态变化才自愈。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
class PresenceServiceTest {

    private final PresenceStore store = mock(PresenceStore.class);
    private final TypingNotifier typing = mock(TypingNotifier.class);
    private final PresenceNotifier notifier = mock(PresenceNotifier.class);
    private final PresenceService service = new PresenceService(store, typing, notifier);

    // ==== typing：会话解析 ====

    @Test
    @DisplayName("typing：只给对端时按消息路径同一套规则生成会话 ID")
    void typingResolvesSingleConversationFromReceiver() {
        when(typing.notifyTyping(anyString(), any(), anyString(), any(), anyBoolean())).thenReturn(1);

        TypingResponse response = service.reportTyping(TypingRequest.newBuilder()
                .setUserId("bob")
                .setReceiverId("alice")
                .setTyping(true)
                .build());

        assertEquals(0, response.getCode());
        assertEquals(1, response.getDelivered());
        // alice < bob，所以会话 ID 是 s:alice:bob —— 与消息路径完全一致（两个方向落在同一会话）
        verify(typing).notifyTyping(eq("s:alice:bob"), eq(""), eq("bob"), eq("alice"), eq(true));
    }

    @Test
    @DisplayName("typing：群聊只给群号时生成 g: 前缀的会话 ID")
    void typingResolvesGroupConversation() {
        TypingResponse response = service.reportTyping(TypingRequest.newBuilder()
                .setUserId("alice")
                .setConvType("GROUP")
                .setReceiverId("group1")
                .build());

        assertEquals(0, response.getCode());
        verify(typing).notifyTyping(eq("g:group1"), eq("GROUP"), eq("alice"), eq("group1"), eq(false));
    }

    @Test
    @DisplayName("typing：既没有会话也没有对端时回 400，且不去打扰扇出层")
    void typingRejectsFrameWithoutTarget() {
        TypingResponse response = service.reportTyping(TypingRequest.newBuilder()
                .setUserId("alice")
                .build());

        assertEquals(400, response.getCode());
        verifyNoInteractions(typing);
    }

    @Test
    @DisplayName("typing：没有 user_id 时回 400")
    void typingRejectsFrameWithoutUser() {
        assertEquals(400, service.reportTyping(TypingRequest.newBuilder()
                .setConversationId("s:alice:bob")
                .build()).getCode());
        verifyNoInteractions(typing);
    }

    // ==== presence：查询 ====

    @Test
    @DisplayName("presence：按入参顺序返回，重复 userId 只出现一次")
    void presenceKeepsOrderAndDeduplicates() {
        Map<String, PresenceStore.Snapshot> snapshots = new HashMap<>();
        snapshots.put("bob", new PresenceStore.Snapshot(2, 3, 1_700_000_000_000L));
        snapshots.put("carol", PresenceStore.Snapshot.offline());
        when(store.snapshotsOf(any())).thenReturn(snapshots);

        GetPresenceResponse response = service.presence(GetPresenceRequest.newBuilder()
                .addUserIds("bob")
                .addUserIds("carol")
                .addUserIds("bob")
                .build());

        assertEquals(2, response.getStatesCount(), "重复的人只该回一条");
        PresenceState bob = response.getStates(0);
        assertEquals("bob", bob.getUserId());
        assertTrue(bob.getOnline());
        assertEquals(2, bob.getGateways());
        assertEquals(3, bob.getDevices());
        assertEquals(1_700_000_000_000L, bob.getLastSeen());

        PresenceState carol = response.getStates(1);
        assertEquals("carol", carol.getUserId());
        assertFalse(carol.getOnline());
        assertEquals(0, carol.getLastSeen(), "离线用户没有 lastSeen 时必须是 0，而不是编一个时间");
    }

    @Test
    @DisplayName("presence：单次查询夹取到上限，不把几千人塞进一次 pipeline")
    void presenceCapsQuerySize() {
        when(store.snapshotsOf(any())).thenReturn(Map.of());

        List<String> userIds = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            userIds.add("u" + i);
        }
        GetPresenceResponse response = service.presence(GetPresenceRequest.newBuilder()
                .addAllUserIds(userIds)
                .build());

        assertEquals(200, response.getStatesCount(), "上限 200：再多就该分批查");
    }

    @Test
    @DisplayName("presence：空请求不查存储")
    void presenceWithNoUsersSkipsStore() {
        assertEquals(0, service.presence(GetPresenceRequest.getDefaultInstance()).getStatesCount());
        verifyNoInteractions(store);
    }

    // ==== presence：边沿上报 ====

    @Test
    @DisplayName("上线：直接广播，broadcast 反映实际送达人数")
    void onlineReportBroadcasts() {
        when(notifier.notifyChange(eq("alice"), eq(true), anyLong())).thenReturn(3);

        PresenceReportAck ack = service.reportPresence(PresenceReport.newBuilder()
                .setUserId("alice")
                .setOnline(true)
                .setTs(100L)
                .build());

        assertEquals(0, ack.getCode());
        assertTrue(ack.getBroadcast(), "有人收到才算广播成功");
        verify(notifier).notifyChange("alice", true, 100L);
        verify(store, never()).touchLastSeen(anyString(), anyLong());
    }

    @Test
    @DisplayName("上线：没有好友在线时 broadcast=false，但仍然是成功")
    void onlineReportWithoutRecipients() {
        when(notifier.notifyChange(anyString(), anyBoolean(), anyLong())).thenReturn(0);

        PresenceReportAck ack = service.reportPresence(PresenceReport.newBuilder()
                .setUserId("alice")
                .setOnline(true)
                .build());

        assertEquals(0, ack.getCode());
        assertFalse(ack.getBroadcast());
    }

    @Test
    @DisplayName("下线：其它实例还连着时<不广播>，也不写 lastSeen")
    void offlineReportIsIgnoredWhileStillOnline() {
        when(store.online("alice")).thenReturn(true);

        PresenceReportAck ack = service.reportPresence(PresenceReport.newBuilder()
                .setUserId("alice")
                .setOnline(false)
                .setGatewayInstance("10.0.0.2:7001")
                .setTs(200L)
                .build());

        assertEquals(0, ack.getCode());
        assertFalse(ack.getBroadcast(), "手机下线但网页还连着，广播「下线」会让好友头像一直灰着");
        verify(notifier, never()).notifyChange(anyString(), anyBoolean(), anyLong());
        verify(store, never()).touchLastSeen(anyString(), anyLong());
    }

    @Test
    @DisplayName("下线：确认没人连着才广播，并记下 lastSeen")
    void offlineReportBroadcastsWhenTrulyOffline() {
        when(store.online("alice")).thenReturn(false);
        when(notifier.notifyChange(eq("alice"), eq(false), anyLong())).thenReturn(2);

        PresenceReportAck ack = service.reportPresence(PresenceReport.newBuilder()
                .setUserId("alice")
                .setOnline(false)
                .setTs(300L)
                .build());

        assertEquals(0, ack.getCode());
        assertTrue(ack.getBroadcast());
        verify(store).touchLastSeen("alice", 300L);
        verify(notifier).notifyChange("alice", false, 300L);
    }

    @Test
    @DisplayName("上报没有 user_id 时回 400")
    void reportWithoutUserIsRejected() {
        assertEquals(400, service.reportPresence(PresenceReport.getDefaultInstance()).getCode());
        verifyNoInteractions(notifier, store);
    }
}
