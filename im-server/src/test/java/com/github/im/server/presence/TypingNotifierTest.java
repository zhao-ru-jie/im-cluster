package com.github.im.server.presence;

import com.github.im.contract.PushPort;
import com.github.im.proto.push.v1.PushResult;
import com.github.im.proto.push.v1.PushTask;
import com.github.im.server.group.GroupMemberRepository;
import com.github.im.server.group.GroupRole;
import com.github.im.server.message.ClientFrames;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 「正在输入」扇出的约定。
 *
 * <p>推送是同步的（调用方在虚拟线程上），所以这里用 {@link BlockingQueue} 收帧只是为了让
 * 断言与生产路径保持同一形状，不依赖 sleep。
 *
 * <p>锁住的五件事：
 * <ol>
 *   <li><b>单聊只推对端</b>：会话 ID {@code s:a:b} 里的另一个人，解不出来就不推；</li>
 *   <li><b>群聊推「除自己以外的成员」，空成员表不推</b>：绝不能退化成「投给群 ID」；</li>
 *   <li><b>去抖只对 typing=true 生效</b>：{@code typing=false} 必须立刻转发，
 *       否则对方那三个点会一直亮着；</li>
 *   <li><b>群里超上限直接不推</b>：万人群里扇出一次的代价远大于收益；</li>
 *   <li><b>开关关掉 / 没装 PushPort 时静默</b>：输入状态是装饰品，不能反过来影响连接。</li>
 * </ol>
 *
 * @author zhaorujie
 * @since 1.0.0
 */
class TypingNotifierTest {

    private static final long WAIT_MS = 3_000;
    private static final long QUIET_MS = 300;

    private final BlockingQueue<PushTask> pushed = new LinkedBlockingQueue<>();
    private final FakeGroupMembers members = new FakeGroupMembers();

    @Test
    @DisplayName("单聊：只推给会话对端，帧里带 userId 与 typing")
    void singleChatPushesToPeer() throws Exception {
        TypingNotifier notifier = notifier(true, 200, 3_000);

        int delivered = notifier.notifyTyping("s:alice:bob", "SINGLE", "alice", null, true);

        PushTask task = pushed.poll(WAIT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(task, "对方在线时输入状态必须扇出");
        assertEquals(List.of("bob"), task.getTargetUserIdsList(), "只推对端，不能推给自己");
        assertEquals("s:alice:bob", task.getConversationId());
        assertEquals(1, delivered, "返回值要能反映「推给了几个人」");
        assertTrue(task.getFrame().contains("\"type\":\"typing.push\""), task.getFrame());
        assertTrue(task.getFrame().contains("\"userId\":\"alice\""), task.getFrame());
        assertTrue(task.getFrame().contains("\"typing\":true"), task.getFrame());
        assertTrue(task.getTtlMs() > 0 && task.getTtlMs() <= 10_000,
                "输入状态必须带短 TTL，迟到送达等于误导：" + task.getTtlMs());
    }

    @Test
    @DisplayName("单聊：conversationId 解不出对端（群格式/脏 ID）时不推")
    void unparsableConversationIsDropped() throws Exception {
        TypingNotifier notifier = notifier(true, 200, 3_000);

        assertEquals(0, notifier.notifyTyping("g:group1", "SINGLE", "alice", null, true),
                "群 ID 当单聊用属于脏帧，丢掉");
        assertEquals(0, notifier.notifyTyping("s:alice", "SINGLE", "alice", null, true),
                "缺少另一半的会话 ID 解不出对端");
        assertNull(pushed.poll(QUIET_MS, TimeUnit.MILLISECONDS));
    }

    @Test
    @DisplayName("群聊：推给除自己以外的成员")
    void groupChatPushesToOtherMembers() throws Exception {
        members.put("group1", List.of("alice", "bob", "carol"));
        TypingNotifier notifier = notifier(true, 200, 3_000);

        notifier.notifyTyping("g:group1", "GROUP", "alice", null, true);

        PushTask task = pushed.poll(WAIT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(task);
        assertEquals(Set.of("bob", "carol"), Set.copyOf(task.getTargetUserIdsList()),
                "自己在输入，自己不该收到「我在输入」");
    }

    @Test
    @DisplayName("群聊：成员表为空（群不存在）时不推，不退化成投给群 ID")
    void emptyGroupPushesNothing() throws Exception {
        TypingNotifier notifier = notifier(true, 200, 3_000);

        assertEquals(0, notifier.notifyTyping("g:missing", "GROUP", "alice", null, true));
        assertNull(pushed.poll(QUIET_MS, TimeUnit.MILLISECONDS),
                "空成员表说明群不存在，推给群 ID 只会塞进永远不会被拉取的地方");
    }

    @Test
    @DisplayName("群聊：成员数超过扇出上限时整条不推")
    void oversizedGroupIsSkipped() throws Exception {
        members.put("big", List.of("alice", "bob", "carol", "dave"));
        TypingNotifier notifier = notifier(true, 3, 3_000);

        assertEquals(0, notifier.notifyTyping("g:big", "GROUP", "alice", null, true));
        assertNull(pushed.poll(QUIET_MS, TimeUnit.MILLISECONDS),
                "超限时退化成「只推前 N 个人」会让一部分人看到三个点、另一部分看不到，比都不看到更像 bug");
    }

    @Test
    @DisplayName("去抖：窗口内重复的 typing=true 只扇出一次")
    void repeatedTypingIsCoalesced() throws Exception {
        TypingNotifier notifier = notifier(true, 200, 3_000);

        notifier.notifyTyping("s:alice:bob", "SINGLE", "alice", null, true);
        assertNotNull(pushed.poll(WAIT_MS, TimeUnit.MILLISECONDS), "第一次必须推");

        notifier.notifyTyping("s:alice:bob", "SINGLE", "alice", null, true);
        notifier.notifyTyping("s:alice:bob", "SINGLE", "alice", null, true);
        assertNull(pushed.poll(QUIET_MS, TimeUnit.MILLISECONDS),
                "客户端打字期间会持续上报，每帧都扇出等于给对端刷屏");
    }

    @Test
    @DisplayName("去抖不吞 typing=false：停止输入必须立刻下发")
    void typingStopsAlwaysForwarded() throws Exception {
        TypingNotifier notifier = notifier(true, 200, 3_000);

        notifier.notifyTyping("s:alice:bob", "SINGLE", "alice", null, true);
        assertNotNull(pushed.poll(WAIT_MS, TimeUnit.MILLISECONDS));

        notifier.notifyTyping("s:alice:bob", "SINGLE", "alice", null, false);
        PushTask stop = pushed.poll(WAIT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(stop, "停止输入被去抖吞掉的话，对方那三个点会一直亮着");
        assertTrue(stop.getFrame().contains("\"typing\":false"), stop.getFrame());
    }

    @Test
    @DisplayName("停止输入之后立刻再打字：不能被上一次去抖误吞")
    void typingRestartsAfterStop() throws Exception {
        TypingNotifier notifier = notifier(true, 200, 3_000);

        notifier.notifyTyping("s:alice:bob", "SINGLE", "alice", null, true);
        assertNotNull(pushed.poll(WAIT_MS, TimeUnit.MILLISECONDS));
        notifier.notifyTyping("s:alice:bob", "SINGLE", "alice", null, false);
        assertNotNull(pushed.poll(WAIT_MS, TimeUnit.MILLISECONDS));

        notifier.notifyTyping("s:alice:bob", "SINGLE", "alice", null, true);
        assertNotNull(pushed.poll(WAIT_MS, TimeUnit.MILLISECONDS),
                "「停止 → 又打字」是新一轮输入，必须重新亮灯");
    }

    @Test
    @DisplayName("出了去抖窗口，同样的输入状态算新的一次")
    void dedupWindowExpires() throws Exception {
        TypingNotifier notifier = notifier(true, 200, 80);

        notifier.notifyTyping("s:alice:bob", "SINGLE", "alice", null, true);
        assertNotNull(pushed.poll(WAIT_MS, TimeUnit.MILLISECONDS));

        TimeUnit.MILLISECONDS.sleep(150);

        notifier.notifyTyping("s:alice:bob", "SINGLE", "alice", null, true);
        assertNotNull(pushed.poll(WAIT_MS, TimeUnit.MILLISECONDS),
                "窗口外还算「重复」的话，聊了十分钟的人只会点亮一次");
    }

    @Test
    @DisplayName("开关关闭：不推也不记账")
    void disabledDoesNothing() throws Exception {
        TypingNotifier notifier = notifier(false, 200, 3_000);

        assertEquals(0, notifier.notifyTyping("s:alice:bob", "SINGLE", "alice", null, true));
        assertNull(pushed.poll(QUIET_MS, TimeUnit.MILLISECONDS));
        assertEquals(0, notifier.tracked());
    }

    @Test
    @DisplayName("没装配 PushPort：静默返回 0，不抛异常")
    void missingPushPortIsSilent() {
        TypingNotifier notifier = notifier(true, 200, 3_000, null);

        assertEquals(0, notifier.notifyTyping("s:alice:bob", "SINGLE", "alice", null, true),
                "输入状态推不出去不是错误，调用方不该因此失败");
        assertTrue(pushed.isEmpty());
    }

    @Test
    @DisplayName("能被 Spring 容器实例化：多构造器时容器用的那个必须标 @Autowired")
    void springCanInstantiate() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.registerBean(ClientFrames.class, ClientFrames::new);
            context.registerBean(GroupMemberRepository.class, FakeGroupMembers::new);
            context.register(TypingNotifier.class);
            context.refresh();

            assertNotNull(context.getBean(TypingNotifier.class),
                    "两个构造器同时存在时，没标 @Autowired 会让 Spring 找不到可用的那个");
        }
    }

    // ==== 辅助 ====

    private TypingNotifier notifier(boolean enabled, int maxGroupMembers, long dedupWindowMs) {
        return notifier(enabled, maxGroupMembers, dedupWindowMs, task -> {
            pushed.add(task);
            return PushResult.newBuilder()
                    .addAllDeliveredUserIds(task.getTargetUserIdsList()).build();
        });
    }

    @SuppressWarnings("unchecked")
    private TypingNotifier notifier(boolean enabled, int maxGroupMembers, long dedupWindowMs,
                                    PushPort pushPort) {
        ObjectProvider<PushPort> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(pushPort);
        return new TypingNotifier(new ClientFrames(), provider, members,
                enabled, maxGroupMembers, dedupWindowMs);
    }

    /** 群成员端口的最小替身：这些用例只关心「谁在群里」，不关心存储介质 */
    static final class FakeGroupMembers implements GroupMemberRepository {

        private final Map<String, List<String>> members = new HashMap<>();

        void put(String groupId, List<String> userIds) {
            members.put(groupId, new ArrayList<>(userIds));
        }

        @Override
        public void addMembers(String groupId, Collection<String> userIds) {
            members.computeIfAbsent(groupId, key -> new ArrayList<>()).addAll(userIds);
        }

        @Override
        public void removeMember(String groupId, String userId) {
            members.getOrDefault(groupId, new ArrayList<>()).remove(userId);
        }

        @Override
        public List<String> membersOf(String groupId) {
            return List.copyOf(members.getOrDefault(groupId, List.of()));
        }

        @Override
        public List<String> groupsOf(String userId) {
            return members.entrySet().stream()
                    .filter(entry -> entry.getValue().contains(userId))
                    .map(Map.Entry::getKey)
                    .toList();
        }

        @Override
        public GroupRole roleOf(String groupId, String userId) {
            return GroupRole.MEMBER;
        }

        @Override
        public void setRole(String groupId, String userId, GroupRole role) {
            // 输入状态与角色无关
        }

        @Override
        public Map<String, GroupRole> rolesOf(String groupId) {
            return Map.of();
        }

        @Override
        public long mutedUntil(String groupId, String userId) {
            return 0;
        }

        @Override
        public void mute(String groupId, String userId, long untilMillis) {
            // 输入状态与禁言无关
        }

        @Override
        public boolean isMember(String groupId, String userId) {
            return members.getOrDefault(groupId, List.of()).contains(userId);
        }
    }
}
