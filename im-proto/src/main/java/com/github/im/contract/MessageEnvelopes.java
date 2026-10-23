package com.github.im.contract;

import com.github.im.proto.ingress.v1.SubmitMessageRequest;
import com.github.im.proto.ingress.v1.SubmitMessageResponse;

/**
 * {@link MessageEnvelope} 与上行 RPC 报文之间的转换。
 *
 * <p>放在契约层的原因：网关要用它「信封 → 请求」，IM 服务要用它「请求 → 信封」，
 * 两侧必须用同一套转换规则，否则 conversationId / 幂等键的语义会在跨服务边界处漂移。
 */
public final class MessageEnvelopes {

    private MessageEnvelopes() {
    }

    public static SubmitMessageRequest toRequest(MessageEnvelope envelope,
                                                 String deviceId,
                                                 String gatewayInstance) {
        SubmitMessageRequest.Builder builder = SubmitMessageRequest.newBuilder()
                .setClientMsgId(envelope.clientMsgId())
                .setFromUserId(envelope.from())
                .setConversationId(envelope.conversationId())
                .setConvType(envelope.convType())
                .setReceiverId(envelope.to())
                .setPayload(envelope.body() == null ? "" : envelope.body())
                .setClientTs(envelope.clientTs());
        if (deviceId != null) {
            builder.setDeviceId(deviceId);
        }
        if (gatewayInstance != null) {
            builder.setGatewayInstance(gatewayInstance);
        }
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
