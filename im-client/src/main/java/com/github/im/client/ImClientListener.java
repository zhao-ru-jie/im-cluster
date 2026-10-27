package com.github.im.client;

/**
 * SDK 对宿主应用的事件回调。
 *
 * <p>全部方法都是 {@code default} 空实现：宿主通常只关心其中两三个（收消息、连接状态），
 * 强迫它写一堆空方法只会让「升级 SDK 加了新事件」变成一次编译失败。
 *
 * <h2>线程约定</h2>
 *
 * <p>回调在 SDK 内部线程上执行（读循环的虚拟线程、或定时器线程），
 * <b>不要在回调里做重活或阻塞等待</b>：读循环被堵住 = 后续所有帧（含心跳 pong）都延迟，
 * 表现为「客户端莫名其妙被服务端判定心跳超时」。
 * 需要耗时处理时请自己丢到业务线程池。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public interface ImClientListener {

    /** 连接已认证成功（TCP：收到 {@code connected} 帧）。此刻 SDK 已自动补报水位并重投待发消息。 */
    default void onConnected() {
    }

    /**
     * 连接断开。
     *
     * @param cause      断开原因（可能为 null，例如被服务端摘流后主动断开）
     * @param retryInMs  SDK 计划在多少毫秒后自动重连；{@code < 0} 表示<b>不会</b>再重连
     *                   （例如 401 认证失败，重连只会把自己打进风控）
     */
    default void onDisconnected(Throwable cause, long retryInMs) {
    }

    /**
     * 收到一条业务消息（实时推送、离线补发、历史拉取同一结构）。
     *
     * <p>SDK 已经按 (会话, seq) 做过去重，所以这里<b>每条只会回调一次</b>。
     * 消息同时已写入本地消息表，进程重启后仍可从表里读出。
     */
    default void onMessage(DownlinkFrame.Push push) {
    }

    /**
     * 服务端受理了一条上行消息（拿到 serverMsgId / seq）。
     *
     * @param local 本地消息表里对应的行（已被更新为 {@code SENT}）；找不到时为 null
     */
    default void onAck(DownlinkFrame.Ack ack, LocalMessage local) {
    }

    /** 对端状态回执：DELIVERED / READ（只有<b>发送方</b>会收到） */
    default void onState(DownlinkFrame.State state) {
    }

    /** 消息变更：撤回（会话级广播）/ 本地删除（只给自己的端） */
    default void onUpdate(DownlinkFrame.Update update) {
    }

    /** 群治理事件：被踢、群解散、被禁言…… */
    default void onGroupUpdate(DownlinkFrame.GroupUpdate update) {
    }

    /**
     * 服务端错误帧（上行被拒、协议错误、服务暂不可用）。
     *
     * <p>SDK 已按 {@code code} 处理过本地消息表（503 安排重投，4xx 标记失败），
     * 这里主要用于 UI 提示与埋点。
     */
    default void onError(DownlinkFrame.Error error) {
    }

    /**
     * 发现 seq 空洞：收到的 seq 比上一条大不止 1，中间确实有消息没到。
     *
     * <p>补发是「至少一次」但不保证「连续」：服务端可能只补了部分区间（上限、
     * 群聊按会话计数）。宿主应当去拉一次历史（HTTP {@code /conv/{id}/messages}）补齐。
     *
     * <p><b>本端自己发出的消息不算空洞</b>：会话的 seq 是双方共用的发号器，
     * 「3, 5, 7」这种间隔通常只是因为你把 4、6 发出去了。SDK 会把本端 ack 过的 seq 排除掉，
     * 所以这里回调意味着缺口里确实有既没收到、也不是你发的号。
     *
     * @param expectedFrom 缺口里第一个未知的 seq
     * @param actualSeq    实际收到的 seq
     */
    default void onGap(String conversationId, long expectedFrom, long actualSeq) {
    }

    /** 认证被拒（401）：token 无效/过期。SDK 不会自动重连，宿主需要重新拿 token 再 {@code connect()} */
    default void onAuthRejected(String message) {
    }
}
