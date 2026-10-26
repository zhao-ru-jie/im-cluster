package com.github.im.gateway.transport;

import com.github.im.contract.MessageEnvelope;
import com.github.im.contract.MessageEnvelopes;
import com.github.im.contract.MessageIngress;
import com.github.im.gateway.metrics.GatewayMetrics;
import com.github.im.gateway.protocol.ClientFrames;
import com.github.im.gateway.session.ClientSession;
import com.github.im.gateway.session.ConnectionManager;
import com.github.im.gateway.session.GatewayRegistry;
import com.github.im.proto.ingress.v1.PullPendingRequest;
import com.github.im.proto.ingress.v1.PullPendingResponse;
import com.github.im.proto.ingress.v1.SubmitMessageRequest;
import com.github.im.proto.ingress.v1.SubmitMessageResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 连接生命周期与上行帧处理的<b>唯一实现</b>，被所有传输复用。
 *
 * <p>为什么不让每种传输各写一份：WebSocket 与 TCP 只有「怎么收发字节」不同，
 * 「连上要登记会话、心跳要应答、上行要转发给 IM 服务、断开要清路由」这四件事必须完全一致。
 * 一旦两套实现分叉，就会出现「TCP 用户掉线不清路由」这种只在某一种传输上复现的幽灵 bug。
 *
 * <p>拆分后的关键变化：本类<b>不持有任何业务路由/存储逻辑</b>。收到消息只做
 * 「解帧 → 组装信封 → 交给 {@link MessageIngress}」，至于是本地调用还是远程 gRPC，
 * 由容器里的 {@link MessageIngress} 实现决定。
 *
 * <p>降级约定：IM 服务不可用时（{@link ObjectProvider} 取不到实现或调用异常），
 * <b>连接本身必须保住</b>——只回一个可重试的 ACK 错误帧，等 IM 服务恢复后客户端重发。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class SessionLifecycle {

    private static final Logger log = LoggerFactory.getLogger(SessionLifecycle.class);

    private static final int DEFAULT_PULL_MAX = 200;
    /** 4xx = 可重试（客户端指数退避重发），5xx = 拒收 */
    private static final int CODE_UNAVAILABLE = 503;

    private final ConnectionManager connectionManager;
    private final GatewayRegistry registry;
    private final ClientFrames frames;
    private final ObjectProvider<MessageIngress> ingressProvider;
    private final GatewayMetrics metrics;

    public SessionLifecycle(ConnectionManager connectionManager,
                            GatewayRegistry registry,
                            ClientFrames frames,
                            ObjectProvider<MessageIngress> ingressProvider,
                            GatewayMetrics metrics) {
        this.connectionManager = connectionManager;
        this.registry = registry;
        this.frames = frames;
        this.ingressProvider = ingressProvider;
        this.metrics = metrics;
    }

    /** 会话已认证并建立：登记本机会话表 + 写 Redis 在线路由 + 补发离线消息 */
    public void onOpen(ClientSession session) {
        String userId = session.userId();
        if (userId == null || userId.isBlank()) {
            session.close("missing-user-id");
            return;
        }
        // 多设备上限在这里兜住：超限就拒绝这条新连接，不要登记路由（否则会往一个死连接推消息）
        if (!connectionManager.add(userId, session)) {
            session.close("too-many-devices");
            return;
        }
        metrics.sessionOpened();
        // 连接状态落 Redis：IM 服务据此决定下行帧推到哪些网关实例（多端时是集合）
        registry.bindUser(userId);
        log.info("user {} connected on gateway {} via {} device={} devices={}", userId,
                registry.localInstanceId(), session.transport(), session.deviceId(),
                connectionManager.deviceCount(userId));
        pullPending(userId, Map.of());
    }

    /** 收到一帧上行报文（WS 的 TextMessage / TCP 的长度帧解码后的 String） */
    public void onFrame(ClientSession session, String payload) {
        String userId = session.userId();
        if (userId == null) {
            return;
        }
        JsonNode node;
        try {
            node = frames.parse(payload);
        } catch (RuntimeException e) {
            session.send(frames.error(null, 400, "invalid frame: " + e.getMessage()));
            return;
        }

        // 「这条连接还活着」由 Netty 的 IdleStateHandler 在 Channel 上判定（收到任何字节都会重置计时），
        // 业务层不再维护活跃时间戳。
        String type = text(node, "type");
        if (ClientFrames.TYPE_PING.equals(type)) {
            session.send(frames.pong());
            return;
        }
        if (ClientFrames.TYPE_PONG.equals(type)) {
            // 客户端对服务端 ping 的应答：仅用于刷新活跃时间，无需回包
            return;
        }
        if (ClientFrames.TYPE_SYNC.equals(type)) {
            // 上报消费水位 + 顺带补拉缺口。放在 send 之前处理：它不改消息流，只更新服务端视图。
            pullPending(userId, watermarksOf(node));
            return;
        }
        if (type != null && !ClientFrames.TYPE_SEND.equals(type)) {
            // 未知帧类型显式报错，而不是当消息投递出去（否则脏帧会被存进消息表）
            session.send(frames.error(text(node, "clientMsgId"), 400, "unsupported frame type: " + type));
            return;
        }
        // 不带 type 的旧报文（{"to":...,"body":...}）继续按发消息处理，保持客户端兼容
        submit(userId, session, node);
    }

    /** 会话断开：只摘掉这一条会话，本地已无该用户会话时才清 Redis 路由 */
    public void onClose(ClientSession session) {
        String userId = session.userId();
        if (userId == null) {
            return;
        }
        // 只摘掉「这一条」会话：快速重连时新会话可能已经登记，不能被旧会话顺手删掉
        connectionManager.remove(userId, session);
        // 本地已无该用户会话时才清路由，避免把新连接刚写好的路由删掉（那会让消息被误判离线）
        if (!connectionManager.isLocal(userId)) {
            registry.unbindUser(userId);
        }
        metrics.sessionClosed();
        log.info("user {} disconnected from gateway {} via {}", userId, registry.localInstanceId(), session.transport());
    }

    private void submit(String userId, ClientSession session, JsonNode node) {
        String clientMsgId = text(node, "clientMsgId");
        String to = text(node, "to");
        String body = text(node, "body");
        if (to == null || to.isBlank()) {
            session.send(frames.error(clientMsgId, 400, "receiver 'to' is required"));
            return;
        }

        MessageEnvelope envelope;
        try {
            envelope = new MessageEnvelope(userId, to, body, clientMsgId,
                    text(node, "conversationId"), text(node, "convType"), longValue(node, "clientTs"));
        } catch (IllegalArgumentException e) {
            session.send(frames.error(clientMsgId, 400, e.getMessage()));
            return;
        }

        MessageIngress ingress = ingressProvider.getIfAvailable();
        if (ingress == null) {
            // IM 服务整体不可用：保连接、拒投递，让客户端稍后重试
            session.send(frames.error(envelope.clientMsgId(), CODE_UNAVAILABLE, "im service unavailable"));
            return;
        }

        // 链路追踪 ID：优先复用客户端带来的（它自己的 traceId，便于端到端对齐），否则这里生成。
        // 之前这条链路上塞的是 serverMsgId，那是消息标识，拿它当时序/追踪 ID 会误导排障。
        String traceId = traceIdOf(node);
        SubmitMessageRequest request = MessageEnvelopes.toRequest(envelope,
                session.deviceId(), registry.localInstanceId(), traceId);
        try {
            // 调用grpc客户端服务RemoteMessageIngress -> MessageIngressGrpcService -> MessageService进行业务处理
            SubmitMessageResponse response = ingress.submit(request);
            if (MessageEnvelopes.isOk(response)) {
                session.send(frames.ack(response.getClientMsgId(), response.getServerMsgId(),
                        response.getSeq(), traceId));
            } else {
                session.send(frames.error(envelope.clientMsgId(), response.getCode(), response.getMessage()));
            }
        } catch (RuntimeException e) {
            metrics.ingressFailed("submit", e);
            log.warn("submit message from {} failed: traceId={} err={}", userId, traceId, e.getMessage());
            session.send(frames.error(envelope.clientMsgId(), CODE_UNAVAILABLE, "im service unavailable"));
        }
    }

    /**
     * 拉取离线/漫游缺口，逐帧推给客户端；同时把客户端上报的水位透传给 IM 服务。
     *
     * <p>补发和「登记水位」共用一次 RPC：两者都是「连接建立/客户端主动 sync」时的必然动作，
     * 分成两个接口只会多一次往返，且容易出现「只调了补发、忘了登记水位」的漏调。
     */
    private void pullPending(String userId, Map<String, Long> watermarks) {
        MessageIngress ingress = ingressProvider.getIfAvailable();
        if (ingress == null) {
            return;
        }
        try {
            PullPendingRequest.Builder request = PullPendingRequest.newBuilder()
                    .setUserId(userId)
                    .setMax(DEFAULT_PULL_MAX);
            if (!watermarks.isEmpty()) {
                request.putAllWatermarks(watermarks);
            }
            // 通过grpc调用RemoteMessageIngress这个类，grpc服务端MessageIngressGrpcService接收到请求，会调用MessageService这个类进行业务处理
            PullPendingResponse pending = ingress.pullPending(request.build());
            for (String frame : pending.getFramesList()) {
                if (!connectionManager.send(userId, frame)) {
                    break;
                }
            }
            if (pending.getRemaining() > 0) {
                log.info("user {} still has {} pending messages", userId, pending.getRemaining());
            }
        } catch (RuntimeException e) {
            metrics.ingressFailed("pull", e);
            log.warn("pull pending for {} failed: {}", userId, e.getMessage());
        }
    }

    /**
     * 解析 {@code {"watermarks":{"s:alice:bob":100,"g:group1":42}}}。
     *
     * <p>宽容解析：缺字段、类型不对、负数一律跳过——水位只是「锦上添花的兜底下界」，
     * 不应该因为客户端报了一个脏字段就拒绝整条连接或整帧。
     */
    private static Map<String, Long> watermarksOf(JsonNode node) {
        JsonNode watermarks = node.get("watermarks");
        if (watermarks == null || !watermarks.isObject()) {
            return Map.of();
        }
        Map<String, Long> parsed = new LinkedHashMap<>();
        for (Map.Entry<String, JsonNode> entry : watermarks.properties()) {
            JsonNode value = entry.getValue();
            if (entry.getKey() == null || entry.getKey().isBlank() || value == null || !value.isNumber()) {
                continue;
            }
            long seq = value.asLong();
            if (seq > 0) {
                parsed.put(entry.getKey(), seq);
            }
        }
        return parsed;
    }

    /**
     * 取本帧的 traceId：客户端带了就用它（客户端自己的链路标识，便于和服务端日志对齐），
     * 没带就现场生成一个 32 位十六进制（UUID 去掉分隔符，日志里好搜也够短）。
     *
     * <p>为什么不强制要求客户端带：traceId 是排障辅助，缺了不该拒消息；
     * 但服务端必须保证「这条消息在整条链路上有同一个 ID」，所以不能留空。
     */
    private static String traceIdOf(JsonNode node) {
        String provided = text(node, "traceId");
        if (provided != null && !provided.isBlank()) {
            return provided;
        }
        return UUID.randomUUID().toString().replace("-", "");
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asString();
    }

    private static long longValue(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? 0L : value.asLong();
    }
}
