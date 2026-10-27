package com.github.im.server.message;

/**
 * 已受理的一条消息（落库行的内存表示）。
 *
 * <p>为什么不直接存「下行帧字符串」：Redis 阶段怎么存都行，但迁到关系库必须拆成列
 * （要建唯一索引做幂等、要按 {@code conversation_id + seq} 建索引做漫游拉取）。
 * 因此这里定义结构化模型，帧由 {@link ClientFrames} 在读路径上按需重建——
 * 这样「存储格式」和「客户端协议」解耦，协议演进不需要改表。
 *
 * <p>富媒体的两个字段（{@code contentType} / {@code attrs}）是<b>列</b>而不是塞进
 * {@code body} 的约定：会话列表要按类型显示「[图片]」，只有类型单独成列才不用为了
 * 算摘要去反序列化每条历史的 body。
 *
 * @param conversationId      会话 ID
 * @param seq         会话内序号（单调递增，保序与拉齐依据）
 * @param serverMsgId 服务端消息 ID（UUIDv7，全局唯一；与 seq 解耦，不受 seq 回绕影响）
 * @param from        发送方
 * @param to          单聊接收方 / 群 ID
 * @param convType    SINGLE / GROUP
 * @param clientMsgId 客户端幂等键
 * @param body        业务消息体
 * @param createdAt   服务端受理时间（毫秒）
 * @param contentType 消息类型（TEXT / IMAGE / FILE / VOICE / VIDEO / CUSTOM）
 * @param attrs       媒体元数据 JSON（url/size/name/duration/...），服务端不解析
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public record StoredMessage(String conversationId,
                            long seq,
                            String serverMsgId,
                            String from,
                            String to,
                            String convType,
                            String clientMsgId,
                            String body,
                            long createdAt,
                            String contentType,
                            String attrs) {

    /** 纯文本 */
    public static final String CT_TEXT = "TEXT";

    /** 兼容构造：老调用方（与历史数据）没有富媒体字段，一律按纯文本处理 */
    public StoredMessage(String conversationId,
                         long seq,
                         String serverMsgId,
                         String from,
                         String to,
                         String convType,
                         String clientMsgId,
                         String body,
                         long createdAt) {
        this(conversationId, seq, serverMsgId, from, to, convType, clientMsgId, body, createdAt,
                CT_TEXT, "");
    }

    public StoredMessage {
        if (contentType == null || contentType.isBlank()) {
            contentType = CT_TEXT;
        }
        if (attrs == null) {
            attrs = "";
        }
    }
}
