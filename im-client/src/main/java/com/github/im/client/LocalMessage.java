package com.github.im.client;

/**
 * 本地消息表的一行。
 *
 * <h2>为什么发和收放在同一张表</h2>
 *
 * <p>两个方向解决的是<b>同一类问题</b>：进程可能随时被杀，内存里的状态不可信。
 * <ul>
 *   <li>{@link Direction#OUT}：用户点发送后，消息必须先落盘再上网。否则「发出去了但没收到 ack，
 *       进程被杀」就会丢消息——而用户视角里那条消息是「已发送」的；</li>
 *   <li>{@link Direction#IN}：收到的消息按 (会话, seq) 落盘。它同时承担三件事：
 *       展示用的历史、离线补发的去重依据、{@code sync} 的会话水位来源。</li>
 * </ul>
 * 水位尤其关键：seq 发号器的下界取「服务端已落库的最大 seq」，当存储整体丢失时
 * 这个下界会一起消失、seq 从 1 重来。此时客户端表里的 {@code max(seq)} 是唯一还活着的证据。
 *
 * <h2>为什么状态是四个而不是「成功/失败」</h2>
 *
 * <p>{@code PENDING}（待发/待重投）与 {@code FAILED}（确定性失败，别再把电网费花在它身上）
 * 必须区分开：{@code 503} 是「现在不行，等会再试」，{@code 400} 是「这条永远不行」。
 * 混成一个布尔值的结果是：要么把永久失败的消息无限重投，要么把可重试的消息丢掉。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public record LocalMessage(
        String clientMsgId,
        Direction direction,
        Status status,
        String conversationId,
        String convType,
        String from,
        String to,
        String body,
        String contentType,
        String attrs,
        String traceId,
        long clientTs,
        String serverMsgId,
        long seq,
        int attempts,
        long nextAttemptAt,
        String lastError,
        long createdAt,
        long updatedAt) {

    public enum Direction {
        /** 待发/已发：本地发起，需要重投 */
        OUT,
        /** 已收：下行消息，用于去重、历史与水位 */
        IN
    }

    public enum Status {
        /** 待发或等待重投（{@code nextAttemptAt} 之后才会动） */
        PENDING,
        /** 服务端已受理（拿到 serverMsgId/seq） */
        SENT,
        /** 确定性失败（4xx）：重投没有意义，需要用户干预或丢弃 */
        FAILED,
        /** 已收到的下行消息 */
        RECEIVED
    }

    public boolean outbound() {
        return direction == Direction.OUT;
    }

    public boolean retryable() {
        return direction == Direction.OUT && status == Status.PENDING;
    }

    /** 到点了能重投吗 */
    public boolean dueAt(long nowMs) {
        return retryable() && nextAttemptAt <= nowMs;
    }

    /** 由一条待发消息生成表行（首次落盘） */
    public static LocalMessage outbound(OutgoingMessage message, long nowMs) {
        return new LocalMessage(message.clientMsgId(), Direction.OUT, Status.PENDING,
                message.conversationId(), message.convType(), null, message.to(),
                message.body(), message.contentType(), message.attrs(), message.traceId(),
                message.clientTs(), null, 0, 0, nowMs, null, nowMs, nowMs);
    }

    /** 由一条下行推送生成表行 */
    public static LocalMessage inbound(DownlinkFrame.Push push, long nowMs) {
        return new LocalMessage(
                // 收到的消息用「会话 + seq」当本地主键：服务端保证这个组合唯一，客户端不必另外发号
                key(push.conversationId(), push.seq()),
                Direction.IN, Status.RECEIVED,
                push.conversationId(), push.convType(), push.from(), push.to(),
                push.body(), push.contentType(), push.attrs(), null,
                push.ts(), push.serverMsgId(), push.seq(), 1, 0, null, nowMs, nowMs);
    }

    public static String key(String conversationId, long seq) {
        return (conversationId == null ? "" : conversationId) + '#' + seq;
    }

    /** 每次尝试发送后：记一次 attempt，并把下次可重投时间往后推 */
    public LocalMessage withAttempt(long nextAttemptAtMs, long nowMs) {
        return withAttempt(nextAttemptAtMs, lastError, nowMs);
    }

    /** 同上，但顺带记下这次失败的原因（写失败 / 服务端 503） */
    public LocalMessage withAttempt(long nextAttemptAtMs, String error, long nowMs) {
        return new LocalMessage(clientMsgId, direction, Status.PENDING, conversationId, convType,
                from, to, body, contentType, attrs, traceId, clientTs, serverMsgId, seq,
                attempts + 1, nextAttemptAtMs, error, createdAt, nowMs);
    }

    /** 受理成功：状态不可逆地前进到 SENT */
    public LocalMessage withSent(String serverMsgId, long seq, long nowMs) {
        return new LocalMessage(clientMsgId, direction, Status.SENT, conversationId, convType,
                from, to, body, contentType, attrs, traceId, clientTs,
                serverMsgId == null ? this.serverMsgId : serverMsgId,
                seq > 0 ? seq : this.seq,
                attempts, 0, null, createdAt, nowMs);
    }

    /** 确定性失败：保留 lastError，便于上层展示「为什么这条发不出去」 */
    public LocalMessage withFailed(String error, long nowMs) {
        return new LocalMessage(clientMsgId, direction, Status.FAILED, conversationId, convType,
                from, to, body, contentType, attrs, traceId, clientTs, serverMsgId, seq,
                attempts, 0, error, createdAt, nowMs);
    }
}
