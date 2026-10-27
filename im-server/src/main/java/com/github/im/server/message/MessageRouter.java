package com.github.im.server.message;

import com.github.im.contract.MessageEnvelope;
import com.github.im.contract.PushPort;
import com.github.im.proto.push.v1.PushTask;
import com.github.im.server.group.GroupMemberRepository;
import com.github.im.server.metrics.MessageMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 消息路由（IM 服务的职责，不是连接层的）。
 *
 * <p>拆分前 {@code MessageRouter} 同时做了三件事：查 Redis 找节点、本机推送、跨节点 gRPC 转发。
 * 现在只剩第一件「决策」：算出这条消息该投给哪些 userId，然后交给 {@link PushPort}。
 * 「目标连在哪个网关、本机有没有会话、要不要 gRPC」全部下沉到网关模块。
 *
 * <p>返回值是投递失败的用户列表——IM 服务据此决定谁需要落离线存储。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class MessageRouter {

    private static final Logger log = LoggerFactory.getLogger(MessageRouter.class);

    private static final int DEFAULT_TTL_MS = 30_000;

    private final ObjectProvider<PushPort> pushPortProvider;
    private final GroupMemberRepository groupMemberRepository;
    private final MessageMetrics metrics;

    public MessageRouter(ObjectProvider<PushPort> pushPortProvider,
                         GroupMemberRepository groupMemberRepository,
                         MessageMetrics metrics) {
        this.pushPortProvider = pushPortProvider;
        this.groupMemberRepository = groupMemberRepository;
        this.metrics = metrics;
    }

    /**
     * 投递消息
     * @param envelope 消息内容
     * @param frame 消息帧
     * @param traceId 消息追踪ID
     * @return 投递失败的用户列表
     */
    public List<String> deliver(MessageEnvelope envelope, String frame, String traceId) {
        List<String> targets = targetsOf(envelope);
        if (targets.isEmpty()) {
            return List.of();
        }
        // 只有这里同时知道「该投几个人」和「几个人没投到」，送达率只能在这里算：
        // 返回给上层的只有失败列表，人数信息一旦离开这个方法就丢了
        long startedAt = System.nanoTime();
        List<String> failed = doDeliver(envelope, frame, traceId, targets);
        metrics.delivery(System.nanoTime() - startedAt, targets.size() - failed.size(), failed.size());
        return failed;
    }

    private List<String> doDeliver(MessageEnvelope envelope, String frame, String traceId,
                                   List<String> targets) {
        PushPort pushPort = pushPortProvider.getIfAvailable();
        if (pushPort == null) {
            // 没有任何推送通道（例如纯离线部署）：全部按失败处理，落离线等客户端上线拉取
            log.debug("no PushPort available, treat {} targets as offline", targets.size());
            return targets;
        }
        PushTask task = PushTask.newBuilder()
                .setTraceId(traceId == null ? "" : traceId)
                .addAllTargetUserIds(targets)
                .setFrame(frame)
                .setTtlMs(DEFAULT_TTL_MS)
                .setConversationId(envelope.conversationId())
                .build();
        try {
            return pushPort.push(task).getFailedUserIdsList();
        } catch (RuntimeException e) {
            log.warn("push failed, fallback to offline: {}", e.getMessage());
            return targets;
        }
    }

    /**
     * 计算投递目标——这里只做「写扩散」的目标计算，不需要知道谁在线。
     *
     * <p>单聊：接收方一人。群聊：群成员列表（{@link GroupMemberRepository}）去掉发送者；
     * 「在线与否」由网关按 {@code PushTask.targetUserIds} 逐个判定，离线的由 IM 服务落离线队列，
     * 因此本方法不查在线状态，避免把连接层的状态耦合进业务决策。
     */
    private List<String> targetsOf(MessageEnvelope envelope) {
        if (!envelope.isGroup()) {
            return List.of(envelope.to());
        }
        List<String> members = groupMemberRepository.membersOf(envelope.to());
        if (members.isEmpty()) {
            // 群不存在/无成员：没有可投目标，也不能退化成「投给群 ID」——那会往离线队列塞进永不拉取的数据
            log.warn("group {} has no members, message {} dropped from fanout",
                    envelope.to(), envelope.idempotencyKey());
            return List.of();
        }
        List<String> targets = new ArrayList<>(members.size());
        for (String member : members) {
            // 发送者本身不需要下发（发送端已本地回显），否则同一条消息会闪两次
            if (!member.equals(envelope.from())) {
                targets.add(member);
            }
        }
        return targets;
    }
}
