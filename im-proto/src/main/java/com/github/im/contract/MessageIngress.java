package com.github.im.contract;

import com.github.im.proto.ingress.v1.*;

/**
 * 上行端口：长连接服务(im-gateway) → IM 业务服务(im-server)。
 *
 * <p>这是两个服务之间「唯一的写入口」。端口定在契约层（im-proto），两侧各自提供实现：
 * <ul>
 *   <li>阶段 1（单体进程）：{@code im-server} 内的本地实现，直接调用业务服务，零序列化开销；</li>
 *   <li>阶段 2（IM 服务独立部署）：{@code im-server} 内的 gRPC 实现
 *       （{@code MessageIngressGrpcService}）+ 网关内的 gRPC 客户端实现。</li>
 * </ul>
 * 网关侧只依赖本接口，因此「换成本地调用还是远程 gRPC」对网关代码不可见。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public interface MessageIngress {

    /**
     * 提交一条上行消息。实现必须幂等：同一 {@code (from_user_id, client_msg_id)}
     * 重复提交返回相同的 {@code server_msg_id} / {@code seq}。
     */
    SubmitMessageResponse submit(SubmitMessageRequest request);

    /** 批量提交；默认逐条提交，远程实现可覆盖为一次 RPC。 */
    default BatchSubmitResponse submitBatch(BatchSubmitRequest request) {
        BatchSubmitResponse.Builder builder = BatchSubmitResponse.newBuilder();
        for (SubmitMessageRequest message : request.getMessagesList()) {
            builder.addResults(submit(message));
        }
        return builder.build();
    }

    /**
     * 连接建立时拉取该用户待补发的下行帧（离线消息 / 多端缺口）。
     *
     * <p>同时承担第二个职责：接收客户端上报的消费水位
     * （{@link PullPendingRequest#getWatermarksMap()}，会话 → 已见最大 seq）。
     * 实现侧必须把它并入 seq 发号器的下界——当消息存储与发号器共用同一个 Redis 且整体丢失时，
     * 客户端水位是唯一还活着的「这些 seq 已被消费过」的证据；缺了它，新消息会被客户端
     * 当历史重复丢弃，表现为「消息永久不显示」。
     *
     * <p><b>本方法只读不移除</b>：返回的帧不会因为被读走而从离线存储里消失，必须由调用方
     * 在真的写进连接之后再调 {@link #ackPending}。这样「网关写到一半失败 / 进程崩溃」不会丢消息，
     * 只是下次重连重复补发一遍（客户端按 seq 去重）。
     */
    PullPendingResponse pullPending(PullPendingRequest request);

    /**
     * 确认一批补发帧已经写进客户端连接。
     *
     * <p>默认实现是空操作（返回成功）：让只需要读路径的实现（如测试替身、或暂时不关心
     * at-least-once 的本地实现）不用被迫实现它。真实实现必须覆盖，否则离线游标永不前进，
     * 表现为「每次重连都把同一批消息重发一遍」。
     */
    default AckPendingResponse ackPending(AckPendingRequest request) {
        return AckPendingResponse.newBuilder().setCode(0).setMessage("ok").build();
    }

    /**
     * 客户端收到下行帧之后的端到端确认（delivered 的真正来源）。
     *
     * <p>与 {@link #ackPending} 的区别是这次只说了一句话：
     * <b>前者是「网关把它写进 socket 了」，后者是「客户端说它收到了」</b>。
     * 可靠性（至少一次）靠前者，可观测性（到底送到没有）靠后者，两者都要，不能互相替代。
     *
     * <p>默认实现是空操作：ACK 丢了是有害但可控的——客户端下次 {@code sync} 时会重新上报水位，
     * 而水位本来就按 max 推进，重复上报不会出错。
     */
    default DeliveredAckResponse ackDelivered(DeliveredAckRequest request) {
        return DeliveredAckResponse.newBuilder().setCode(0).setMessage("ok").build();
    }

    /**
     * 用户的会话列表（按最后消息时间倒序），每条带未读数。
     *
     * <p>默认返回空列表：会话模型是后加的，让只需要收发消息的实现不必被迫实现它。
     */
    default ListConversationsResponse listConversations(ListConversationsRequest request) {
        return ListConversationsResponse.getDefaultInstance();
    }

    /**
     * 上报已读位置。
     *
     * <p>默认实现是<b>直接回显</b>而不是什么都不做：调用方会拿返回值更新角标，
     * 回 0 会让客户端的未读数卡在错误的位置上。
     */
    default MarkReadResponse markRead(MarkReadRequest request) {
        return MarkReadResponse.newBuilder()
                .setCode(0)
                .setMessage("ok")
                .setReadSeq(request.getSeq())
                .build();
    }
}
