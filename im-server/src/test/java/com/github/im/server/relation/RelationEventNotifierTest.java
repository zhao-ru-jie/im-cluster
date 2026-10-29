package com.github.im.server.relation;

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
 * 好友申请下行帧的形状与降级行为。
 *
 * <p>申请通知是「一条都不该丢」的典型：丢了对方就不知道有人在等他同意。
 * 但它也绝不能反过来影响主流程——申请已经落库，推不出去只是慢一点。
 * 这两面都在这里钉住：帧字段对不对、开关关掉/没装配 PushPort 时是否安静。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
class RelationEventNotifierTest {

    private static final long WAIT_MS = 3_000;
    private static final long QUIET_MS = 300;

    private final BlockingQueue<PushTask> pushed = new LinkedBlockingQueue<>();

    @Test
    @DisplayName("申请帧推给被申请人，带申请人 + 验证消息")
    void requestFrameCarriesApplicantAndMessage() throws Exception {
        notifier(true).notifyRequest("alice", "bob", "我是小李");

        PushTask task = pushed.poll(WAIT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(task, "申请到达却不推，红点只能等下次冷启动");
        assertEquals(List.of("bob"), task.getTargetUserIdsList());
        String frame = task.getFrame();
        assertTrue(frame.contains("\"type\":\"relation.request\""), frame);
        assertTrue(frame.contains("\"fromId\":\"alice\""), frame);
        assertTrue(frame.contains("\"message\":\"我是小李\""), frame);
        assertTrue(frame.contains("\"ts\":"), "所有业务帧都带时间戳，客户端据此做「晚到的提示」判断：" + frame);
    }

    @Test
    @DisplayName("空验证消息也照常下发（用空串而不是省略字段）")
    void emptyMessageIsStillEmitted() throws Exception {
        notifier(true).notifyRequest("alice", "bob", null);

        PushTask task = pushed.poll(WAIT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(task);
        assertTrue(task.getFrame().contains("\"message\":\"\""), task.getFrame());
    }

    @Test
    @DisplayName("结果帧带 peerId + change，按 change 区分谁该刷新什么")
    void updateFrameCarriesChange() throws Exception {
        RelationEventNotifier notifier = notifier(true);

        notifier.notifyUpdate("alice", "bob", ClientFrames.RELATION_ACCEPTED);
        PushTask accepted = pushed.poll(WAIT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(accepted);
        assertEquals(List.of("alice"), accepted.getTargetUserIdsList(), "结果推给申请人");
        assertTrue(accepted.getFrame().contains("\"type\":\"relation.update\""), accepted.getFrame());
        assertTrue(accepted.getFrame().contains("\"peerId\":\"bob\""), accepted.getFrame());
        assertTrue(accepted.getFrame().contains("\"change\":\"ACCEPTED\""), accepted.getFrame());

        notifier.notifyUpdate("bob", "alice", ClientFrames.RELATION_CANCELLED);
        PushTask cancelled = pushed.poll(WAIT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(cancelled);
        assertEquals(List.of("bob"), cancelled.getTargetUserIdsList());
        assertTrue(cancelled.getFrame().contains("\"change\":\"CANCELLED\""), cancelled.getFrame());
    }

    @Test
    @DisplayName("开关关闭时一条都不推（但申请仍会落库）")
    void disabledNotifierIsSilent() throws Exception {
        notifier(false).notifyRequest("alice", "bob", "");

        assertNull(pushed.poll(QUIET_MS, TimeUnit.MILLISECONDS));
    }

    @Test
    @DisplayName("没装配 PushPort 时静默：通知失败不能反过来让申请失败")
    void missingPushPortIsSilent() throws Exception {
        notifier(true, null).notifyRequest("alice", "bob", "");

        assertNull(pushed.poll(QUIET_MS, TimeUnit.MILLISECONDS));
    }

    @Test
    @DisplayName("没有收件人时不推，不退化成广播")
    void blankRecipientPushesNothing() throws Exception {
        RelationEventNotifier notifier = notifier(true);

        notifier.notifyRequest("alice", "", "");
        notifier.notifyRequest("alice", null, "");
        notifier.notifyUpdate("  ", "bob", ClientFrames.RELATION_ACCEPTED);

        assertNull(pushed.poll(QUIET_MS, TimeUnit.MILLISECONDS));
    }

    @Test
    @DisplayName("能被 Spring 容器实例化")
    void springCanInstantiate() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.registerBean(ClientFrames.class, () -> new ClientFrames(new ObjectMapper()));
            context.register(RelationEventNotifier.class);
            context.refresh();

            assertNotNull(context.getBean(RelationEventNotifier.class));
        }
    }

    // ==== 辅助 ====

    private RelationEventNotifier notifier(boolean enabled) {
        return notifier(enabled, task -> {
            pushed.add(task);
            return PushResult.newBuilder().addAllDeliveredUserIds(task.getTargetUserIdsList()).build();
        });
    }

    @SuppressWarnings("unchecked")
    private RelationEventNotifier notifier(boolean enabled, PushPort pushPort) {
        ObjectProvider<PushPort> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(pushPort);
        return new RelationEventNotifier(new ClientFrames(new ObjectMapper()), provider, enabled);
    }
}
