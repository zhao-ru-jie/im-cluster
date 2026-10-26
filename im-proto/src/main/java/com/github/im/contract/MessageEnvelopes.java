package com.github.im.contract;

import com.github.im.proto.ingress.v1.SubmitMessageRequest;
import com.github.im.proto.ingress.v1.SubmitMessageResponse;

/**
 * {@link MessageEnvelope} 与上行 RPC 报文之间的转换。
 *
 * <p>放在契约层的原因：网关要用它「信封 → 请求」，IM 服务要用它「请求 → 信封」，
 * 两侧必须用同一套转换规则，否则 conversationId / 幂等键的语义会在跨服务边界处漂移。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public final class MessageEnvelopes {

    private MessageEnvelopes() {
    }

    /**
     * @param traceId 链路追踪 ID：网关优先复用客户端带来的（{@code traceId} 字段，便于端到端对齐），
     *                没有则由网关生成一个，贯穿上行 RPC、消息受理日志与下行 {@code PushTask}。
     *                <b>不要</b>用 {@code serverMsgId} 冒充它——那是消息标识，不是追踪标识。
     */
    public static SubmitMessageRequest toRequest(MessageEnvelope envelope,
                                                 String deviceId,
                                                 String gatewayInstance,
                                                 String traceId) {
        SubmitMessageRequest.Builder builder = SubmitMessageRequest.newBuilder()
                .setTraceId(traceId)
                .setClientMsgId(envelope.clientMsgId())
                .setFromUserId(envelope.from())
                .setDeviceId(deviceId)
                .setConversationId(envelope.conversationId())
                .setConvType(envelope.convType())
                .setReceiverId(envelope.to())
                .setPayload(envelope.body() == null ? "" : envelope.body())
                .setClientTs(envelope.clientTs())
                .setGatewayInstance(gatewayInstance);
        return builder.build();
    }

    public static MessageEnvelope toEnvelope(SubmitMessageRequest request) {
        return new MessageEnvelope(
                request.getFromUserId(),
                request.getReceiverId(),
                request.getPayload(),
                request.getClientMsgId(),
                request.getConversationId(),
                request.getConvType(),
                request.getClientTs());
    }

    public static boolean isOk(SubmitMessageResponse response) {
        return response != null && response.getCode() == 0;
    }
}
