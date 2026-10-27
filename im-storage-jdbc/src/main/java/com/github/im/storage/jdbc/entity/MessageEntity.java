package com.github.im.storage.jdbc.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.github.im.server.message.StoredMessage;

/**
 * {@code im_message} 的持久化对象。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@TableName("im_message")
public class MessageEntity {

    /** 自增主键，业务不感知；幂等与保序分别由下面的唯一索引保证 */
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;

    /** 会话 ID：单聊 {@code s:{a}:{b}}，群聊 {@code g:{groupId}} */
    private String conversationId;

    /** 会话内单调序号（保序与拉齐依据） */
    private Long seq;

    /** 服务端消息 ID（UUIDv7，与 seq 解耦） */
    private String serverMsgId;

    /** 发送方 userId */
    private String fromUser;

    /** 单聊接收方 / 群 ID */
    private String toUser;

    /** SINGLE / GROUP */
    private String convType;

    /** 客户端幂等键 */
    private String clientMsgId;

    /** 业务消息体（网关透传，服务端不解析） */
    private String body;

    /** 服务端受理时间（毫秒） */
    private Long createdAt;

    /** 消息类型：TEXT / IMAGE / FILE / VOICE / VIDEO / CUSTOM */
    private String contentType;

    /** 媒体元数据 JSON（url/size/name/duration/...），服务端不解析 */
    private String attrs;

    /** 撤回时间（毫秒），null = 未撤回；不放进 StoredMessage，渲染由 recalledSeqs 批量判定 */
    private Long recalledAt;

    public static MessageEntity from(StoredMessage message) {
        MessageEntity entity = new MessageEntity();
        entity.setConversationId(message.conversationId());
        entity.setSeq(message.seq());
        entity.setServerMsgId(message.serverMsgId());
        entity.setFromUser(message.from());
        entity.setToUser(message.to());
        entity.setConvType(message.convType());
        entity.setClientMsgId(message.clientMsgId());
        entity.setBody(message.body());
        entity.setCreatedAt(message.createdAt());
        entity.setContentType(message.contentType());
        entity.setAttrs(message.attrs());
        return entity;
    }

    public StoredMessage toStoredMessage() {
        return new StoredMessage(conversationId, seq == null ? 0L : seq, serverMsgId,
                fromUser, toUser, convType, clientMsgId, body,
                createdAt == null ? 0L : createdAt, contentType, attrs);
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getConversationId() {
        return conversationId;
    }

    public void setConversationId(String conversationId) {
        this.conversationId = conversationId;
    }

    public Long getSeq() {
        return seq;
    }

    public void setSeq(Long seq) {
        this.seq = seq;
    }

    public String getServerMsgId() {
        return serverMsgId;
    }

    public void setServerMsgId(String serverMsgId) {
        this.serverMsgId = serverMsgId;
    }

    public String getFromUser() {
        return fromUser;
    }

    public void setFromUser(String fromUser) {
        this.fromUser = fromUser;
    }

    public String getToUser() {
        return toUser;
    }

    public void setToUser(String toUser) {
        this.toUser = toUser;
    }

    public String getConvType() {
        return convType;
    }

    public void setConvType(String convType) {
        this.convType = convType;
    }

    public String getClientMsgId() {
        return clientMsgId;
    }

    public void setClientMsgId(String clientMsgId) {
        this.clientMsgId = clientMsgId;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public Long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Long createdAt) {
        this.createdAt = createdAt;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public String getAttrs() {
        return attrs;
    }

    public void setAttrs(String attrs) {
        this.attrs = attrs;
    }

    public Long getRecalledAt() {
        return recalledAt;
    }

    public void setRecalledAt(Long recalledAt) {
        this.recalledAt = recalledAt;
    }
}
