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
     */
    PullPendingResponse pullPending(PullPendingRequest request);
}
