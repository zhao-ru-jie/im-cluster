package com.github.im.client;

/**
 * 会话列表的一行（{@code GET /conv} 的元素）。
 *
 * <p>字段与 proto {@code ConversationSummary} 一一对应（JSON 侧是 camelCase）。
 * 未读数是<b>算出来的</b>（{@code unread = lastSeq - readSeq}），不是计数器：
 * 任何一端漏写都会自愈，这也是服务端不存未读计数的原因。
 *
 * @param conversationId 会话 ID：单聊 {@code s:{a}:{b}}，群聊 {@code g:{groupId}}
 * @param type           SINGLE / GROUP
 * @param lastSeq        会话当前最大 seq（0 = 还没有消息）
 * @param lastTime       最后一条消息的服务端时间（毫秒）
 * @param lastFrom       最后一条消息的发送方
 * @param lastDigest     最后一条消息摘要（已截断，只用于列表展示）
 * @param lastMsgId      最后一条消息的服务端 ID
 * @param readSeq        我读到哪了（用户级，多端共用）
 * @param unread         lastSeq - readSeq，客户端自行按「99+」截断
 * @param pinned         置顶（只影响服务端排序，不影响拉取顺序）
 * @param muted          免打扰（未读照算，只是不响铃）
 * @author zhaorujie
 * @since 1.0.0
 */
public record ConversationSummary(
        String conversationId,
        String type,
        long lastSeq,
        long lastTime,
        String lastFrom,
        String lastDigest,
        String lastMsgId,
        long readSeq,
        long unread,
        boolean pinned,
        boolean muted) {
}
