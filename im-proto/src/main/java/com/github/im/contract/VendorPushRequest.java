package com.github.im.contract;

/**
 * 一次<b>厂商推送</b>（APNs / FCM / 华为 / 小米 / OPPO / vivo …）的请求。
 *
 * <h2>它和 {@link PushPort} 的推送有什么区别</h2>
 *
 * <p>本项目的 {@code PushPort} 指的是「长连接下发」——只有 App 在前台、连接活着时才有效。
 * 而用户真正收不到消息的场景恰好是<b>连接不在</b>：进程被杀、后台被冻结、手机休眠。
 * 这时唯一的通道是手机厂商的推送服务，它由系统进程常驻接管，能唤醒 App 或至少显示通知。
 *
 * <p>所以两者的触发条件是互补的：{@code PushPort} 返回的 failed 用户，
 * 正是厂商推送的目标用户。
 *
 * <p>这里只描述「推给谁、推什么」，不描述「怎么推」：各家鉴权差异极大
 * （APNs 是 HTTP/2 + 每次签发的 JWT，FCM 是 OAuth2，国内厂商各有一套签名），
 * 把这些塞进同一个实现里只会得到一堆 if。适配由 {@link VendorPushPort} 的实现承担。
 *
 * @param userId         目标用户
 * @param title          通知标题（单聊一般是发送方，群聊是群 ID）
 * @param body           通知正文（消息摘要，非文本消息是 {@code [图片]} 这类占位）
 * @param conversationId 会话 ID，客户端点击通知时据此直达会话
 * @param seq            会话内序号：客户端点开通知后从它往后拉，比从头拉整个会话省
 * @param contentType    消息类型，客户端可据此决定通知样式（是否带图）
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public record VendorPushRequest(String userId,
                                String title,
                                String body,
                                String conversationId,
                                long seq,
                                String contentType) {
}
