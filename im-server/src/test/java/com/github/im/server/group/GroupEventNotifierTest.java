package com.github.im.server.group;

import com.github.im.contract.PushPort;
import com.github.im.proto.push.v1.PushResult;
import com.github.im.proto.push.v1.PushTask;
import com.github.im.server.message.ClientFrames;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
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
 * 群变更下行（{@code group.update}）的约定。
 *
 * <p>推送是异步的（virtual thread + fire-and-forget），所以用 {@link BlockingQueue} 等结果而不是 sleep：
 * 既不会因为机器慢而偶发失败，也不会为了等一条「不该来」的帧而白等整秒。
 *
 * <p>锁住的四件事：
 * <ol>
 *   <li><b>收件人是「成员 ∪ 被操作者」</b>：被踢的人此刻已经不是成员，只按成员推的话
 *       他最该收到的那条「你已被移出群」反而收不到；</li>
 *   <li><b>去重按动作，不按收件人</b>：按收件人去重时，同一个群第二次踢人会被整条跳过——
 *       全群都不会知道有人被踢了；</li>
 *   <li><b>没开开关 / 没装配 PushPort 时静默</b>：变更已经落库，通知失败不该反过来让治理动作失败；</li>
 *   <li><b>收件人为空不推</b>：否则会退化成一次没有对象的广播。</li>
 * </ol>
 *
 * @author zhaorujie
 * @since 1.0.0
 */
class GroupEventNotifierTest {

    /** 等一条「应该来」的帧：够宽松，避免慢机器上偶发失败 */
    private static final long WAIT_MS = 3_000;
    /** 等一条「不该来」的帧：只等一小会儿，不为它拖慢整个套件 */
    private static final long QUIET_MS = 300;

    private final BlockingQueue<PushTask> pushed = new LinkedBlockingQueue<>();

    @Test
    @DisplayName("群变更推给全体成员，帧里带 groupId / change / 操作双方")
    void pushesFrameToAllRecipients() throws Exception {
        GroupEventNotifier notifier = notifier(true);

        notifier.notifyChange("g1", ClientFrames.GROUP_MEMBER_REMOVED, "alice", "dave",
                List.of("alice", "bob", "dave"));

        PushTask task = pushed.poll(WAIT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(task, "治理动作发生了却不下行，客户端只能等下次拉群资料才知道");
        assertEquals(Set.of("alice", "bob", "dave"), Set.copyOf(task.getTargetUserIdsList()),
                "被踢的 dave 也要收到「你已被移出群」——他此刻已经不是成员了");
        assertEquals("g:g1", task.getConversationId(), "客户端按会话维度归并这条事件");
        assertTrue(task.getFrame().contains("\"type\":\"group.update\""), task.getFrame());
        assertTrue(task.getFrame().contains("\"change\":\"MEMBER_REMOVED\""), task.getFrame());
        assertTrue(task.getFrame().contains("\"groupId\":\"g1\""), task.getFrame());
        assertTrue(task.getFrame().contains("\"actorId\":\"alice\""), task.getFrame());
        assertTrue(task.getFrame().contains("\"targetId\":\"dave\""), task.getFrame());
    }

    @Test
    @DisplayName("同一动作重复触发（客户端重试）只提示一次")
    void duplicateActionNotifiesOnce() throws Exception {
        GroupEventNotifier notifier = notifier(true);

        notifier.notifyChange("g1", ClientFrames.GROUP_MUTED, "alice", "bob", List.of("alice", "bob"));
        notifier.notifyChange("g1", ClientFrames.GROUP_MUTED, "alice", "bob", List.of("alice", "bob"));

        assertNotNull(pushed.poll(WAIT_MS, TimeUnit.MILLISECONDS), "第一次必须推");
        assertNull(pushed.poll(QUIET_MS, TimeUnit.MILLISECONDS),
                "同一次动作被重试时二次提示就是打扰——去重窗口内必须只推一条");
    }

    @Test
    @DisplayName("同一个群的另一次动作要照常提示（去重不能按收件人做）")
    void differentTargetStillNotifies() throws Exception {
        GroupEventNotifier notifier = notifier(true);

        notifier.notifyChange("g1", ClientFrames.GROUP_MEMBER_REMOVED, "alice", "dave",
                List.of("alice", "bob", "dave"));
        assertNotNull(pushed.poll(WAIT_MS, TimeUnit.MILLISECONDS), "第一次踢人要推");

        // 第二次踢人：alice/bob 在上一次动作里已经当过收件人，但这是另一件事
        notifier.notifyChange("g1", ClientFrames.GROUP_MEMBER_REMOVED, "alice", "bob",
                List.of("alice", "carol", "bob"));

        PushTask second = pushed.poll(WAIT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(second,
                "去重键若只含收件人，第二次踢人时全群都已被标记过，整条事件会被静默吞掉");
        assertEquals(Set.of("alice", "carol", "bob"), Set.copyOf(second.getTargetUserIdsList()));
        assertTrue(second.getFrame().contains("\"targetId\":\"bob\""), second.getFrame());
    }

    @Test
    @DisplayName("群资料变更（无被操作者）同样按一次动作去重")
    void groupLevelChangeIsDeduplicated() throws Exception {
        GroupEventNotifier notifier = notifier(true);

        notifier.notifyChange("g1", ClientFrames.GROUP_UPDATED, "alice", null, List.of("alice", "bob"));
        notifier.notifyChange("g1", ClientFrames.GROUP_UPDATED, "alice", null, List.of("alice", "bob"));

        PushTask task = pushed.poll(WAIT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(task, "改名/公告这类变更也要下行");
        assertTrue(task.getFrame().contains("\"targetId\":\"\""), "没有具体被操作者时用空串，" + task.getFrame());
        assertNull(pushed.poll(QUIET_MS, TimeUnit.MILLISECONDS), "重试改名不该二次提示");
    }

    @Test
    @DisplayName("开关关闭时不推，也不记账")
    void disabledNotifierDoesNothing() throws Exception {
        GroupEventNotifier notifier = notifier(false);

        notifier.notifyChange("g1", ClientFrames.GROUP_UPDATED, "alice", null, List.of("alice"));

        assertNull(pushed.poll(QUIET_MS, TimeUnit.MILLISECONDS));
        assertEquals(0, notifier.tracked(), "关掉之后连去重表都不该增长");
    }

    @Test
    @DisplayName("没装配 PushPort 时静默：变更已落库，通知失败不能反过来报错")
    void missingPushPortIsSilent() throws Exception {
        GroupEventNotifier notifier = notifier(true, null);

        notifier.notifyChange("g1", ClientFrames.GROUP_DISMISSED, "alice", null, List.of("alice", "bob"));

        assertEquals(1, notifier.tracked(), "动作本身被记录，只是没有下游可推");
        assertNull(pushed.poll(QUIET_MS, TimeUnit.MILLISECONDS));
    }

    @Test
    @DisplayName("没有收件人时不推，不退化成广播")
    void emptyRecipientsPushesNothing() throws Exception {
        GroupEventNotifier notifier = notifier(true);

        notifier.notifyChange("g1", ClientFrames.GROUP_UPDATED, "alice", null, List.of());
        notifier.notifyChange("g1", ClientFrames.GROUP_UPDATED, "alice", null, null);
        notifier.notifyChange(null, ClientFrames.GROUP_UPDATED, "alice", null, List.of("alice"));

        assertNull(pushed.poll(QUIET_MS, TimeUnit.MILLISECONDS),
                "空收件人 / 空群 ID 时推送出去就是一条没有对象的事件");
        assertEquals(0, notifier.tracked());
    }

    // ==== 去重表的生命周期 ====

    @Test
    @DisplayName("出了去重窗口，同样的动作算新事件（要再次提示）")
    void dedupWindowExpires() throws Exception {
        GroupEventNotifier notifier = notifier(50, 8);

        notifier.notifyChange("g1", ClientFrames.GROUP_UPDATED, "alice", null, List.of("alice"));
        assertNotNull(pushed.poll(WAIT_MS, TimeUnit.MILLISECONDS), "第一次必须推");

        TimeUnit.MILLISECONDS.sleep(200);

        notifier.notifyChange("g1", ClientFrames.GROUP_UPDATED, "alice", null, List.of("alice"));
        assertNotNull(pushed.poll(WAIT_MS, TimeUnit.MILLISECONDS),
                "窗口外还算「重试」的话，同一个群第二次改名就永远推不出去了");
    }

    @Test
    @DisplayName("过期键会被回收：去重表规模只跟窗口内的动作数相关")
    void staleKeysArePruned() throws Exception {
        // 窗口放大到 200ms，造数据期间不会有键过期，计数才是确定的
        GroupEventNotifier notifier = notifier(200, 8);

        for (int i = 0; i < 10; i++) {
            notifier.notifyChange("g1", ClientFrames.GROUP_MUTED, "alice", "u" + i, List.of("alice"));
        }
        assertEquals(10, notifier.tracked(), "没过期的键一条都不能丢，否则重试会重复提示");

        TimeUnit.MILLISECONDS.sleep(300);

        // 这一条顶过水位、且距上次清理已超过窗口 → 触发回收
        notifier.notifyChange("g1", ClientFrames.GROUP_MUTED, "alice", "fresh", List.of("alice"));

        assertEquals(1, notifier.tracked(),
                "不回收的话这张表会随「群 × 被操作者」的历史一直涨，最终只能整表丢弃");
    }

    @Test
    @DisplayName("能被 Spring 容器实例化：多构造器不能落到「找不到默认构造器」")
    void springCanInstantiate() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.registerBean(ClientFrames.class, ClientFrames::new);
            context.register(GroupEventNotifier.class);
            context.refresh();

            assertNotNull(context.getBean(GroupEventNotifier.class),
                    "这个类有第二个（给单测注入窗口的）构造器，容器用的那个必须标 @Autowired；"
                            + "否则 Spring 会退回无参构造器，启动直接失败");
        }
    }

    // ==== 辅助 ====

    private GroupEventNotifier notifier(boolean enabled) {
        return notifier(enabled, task -> {
            pushed.add(task);
            return PushResult.newBuilder()
                    .addAllDeliveredUserIds(task.getTargetUserIdsList()).build();
        });
    }

    @SuppressWarnings("unchecked")
    private GroupEventNotifier notifier(boolean enabled, PushPort pushPort) {
        ObjectProvider<PushPort> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(pushPort);
        return new GroupEventNotifier(new ClientFrames(), provider, enabled);
    }

    /** 缩短窗口与水位的实例：用来验证去重的过期与回收，生产走默认常量 */
    @SuppressWarnings("unchecked")
    private GroupEventNotifier notifier(long dedupWindowMs, int pruneThreshold) {
        ObjectProvider<PushPort> provider = mock(ObjectProvider.class);
        PushPort pushPort = task -> {
            pushed.add(task);
            return PushResult.newBuilder().addAllDeliveredUserIds(task.getTargetUserIdsList()).build();
        };
        when(provider.getIfAvailable()).thenReturn(pushPort);
        return new GroupEventNotifier(new ClientFrames(), provider, true,
                dedupWindowMs, pruneThreshold);
    }
}
