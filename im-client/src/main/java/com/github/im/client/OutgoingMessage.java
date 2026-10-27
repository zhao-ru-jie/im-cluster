package com.github.im.client;

import java.util.UUID;

/**
 * 一条待发消息（上行 {@code msg.send} 的内容）。
 *
 * <p>字段全部可空，由服务端按语义补全：单聊只给 {@code to}，服务端按 (from,to) 推导会话 ID；
 * 群聊只给 {@code conversationId}（{@code g:xxx}），{@code to} 留空。
 *
 * <h2>clientMsgId 是幂等键，不是流水号</h2>
 *
 * <p>它由客户端生成、<b>重试时绝不改变</b>：服务端靠它判定「这条消息我见过」，
 * 重复提交会返回同一个 {@code serverMsgId / seq}，因此「本地消息表 + 重投」才是安全的。
 * 一旦重试时重新生成 ID，服务端会当成新消息，用户就会看到两条一样的内容。
 *
 * <p>缺省值由 {@link Builder#build()} 补：{@code clientMsgId = UUIDv4}（长度与字符集都安全）、
 * {@code clientTs = 当前时间}、{@code contentType = TEXT}。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public record OutgoingMessage(
        String clientMsgId,
        String to,
        String conversationId,
        String convType,
        String body,
        String contentType,
        String attrs,
        String traceId,
        long clientTs) {

    public static final String CT_TEXT = "TEXT";
    public static final String CONV_SINGLE = "SINGLE";
    public static final String CONV_GROUP = "GROUP";

    public OutgoingMessage {
        if (body == null) {
            body = "";
        }
        if (contentType == null || contentType.isBlank()) {
            contentType = CT_TEXT;
        }
    }

    /** 单聊纯文本：最常见的一条路径，不需要写 builder */
    public static OutgoingMessage text(String to, String body) {
        return builder().to(to).body(body).build();
    }

    /** 富媒体：{@code attrs} 是 JSON 串（url/size/width/height/thumb），客户端与服务端都不解析 */
    public static OutgoingMessage media(String to, String contentType, String attrs) {
        return builder().to(to).contentType(contentType).attrs(attrs).build();
    }

    /** 补齐缺省值，并保证 clientMsgId 一定存在（幂等键不能等到发送时才补） */
    public OutgoingMessage normalized() {
        Builder builder = new Builder()
                .clientMsgId(clientMsgId == null || clientMsgId.isBlank() ? newClientMsgId() : clientMsgId)
                .to(to)
                .conversationId(conversationId)
                .convType(convType)
                .body(body)
                .contentType(contentType)
                .attrs(attrs)
                .traceId(traceId)
                .clientTs(clientTs > 0 ? clientTs : System.currentTimeMillis());
        return builder.build();
    }

    /**
     * 生成幂等键。
     *
     * <p>用 UUIDv4 而不是「时间戳 + 自增」：自增需要落盘才能跨进程不重复，
     * 而时间戳在同一毫秒重装应用后会重复——重复的幂等键意味着「新消息被服务端当成旧消息回放」。
     */
    public static String newClientMsgId() {
        return UUID.randomUUID().toString();
    }

    public static Builder builder() {
        return new Builder();
    }

    /** 可变构建器：字段多且大多可空，builder 比 9 参构造可读 */
    public static final class Builder {
        private String clientMsgId;
        private String to;
        private String conversationId;
        private String convType;
        private String body;
        private String contentType;
        private String attrs;
        private String traceId;
        private long clientTs;

        public Builder clientMsgId(String v) {
            this.clientMsgId = v;
            return this;
        }

        public Builder to(String v) {
            this.to = v;
            return this;
        }

        public Builder conversationId(String v) {
            this.conversationId = v;
            return this;
        }

        public Builder convType(String v) {
            this.convType = v;
            return this;
        }

        public Builder body(String v) {
            this.body = v;
            return this;
        }

        public Builder contentType(String v) {
            this.contentType = v;
            return this;
        }

        public Builder attrs(String v) {
            this.attrs = v;
            return this;
        }

        public Builder traceId(String v) {
            this.traceId = v;
            return this;
        }

        public Builder clientTs(long v) {
            this.clientTs = v;
            return this;
        }

        public OutgoingMessage build() {
            return new OutgoingMessage(clientMsgId, to, conversationId, convType, body,
                    contentType, attrs, traceId, clientTs);
        }
    }
}
