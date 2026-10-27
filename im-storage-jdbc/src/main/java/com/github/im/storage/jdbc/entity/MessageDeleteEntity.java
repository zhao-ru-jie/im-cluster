package com.github.im.storage.jdbc.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

/**
 * {@code im_message_delete} 的持久化对象（用户本地删除记录）。
 *
 * <p>与 {@code im_message.recalled_at} 的分工：撤回是会话级的（一列即可），
 * 删除是用户级的（一条消息可能被 N 个人各删一次，必须是独立行）。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@TableName("im_message_delete")
public class MessageDeleteEntity {

    @TableId(type = IdType.ASSIGN_ID)
    private Long id;

    private String userId;

    private String conversationId;

    private Long seq;

    private Long createdAt;

    public static MessageDeleteEntity of(String userId, String conversationId, long seq, long createdAt) {
        MessageDeleteEntity entity = new MessageDeleteEntity();
        entity.setUserId(userId);
        entity.setConversationId(conversationId);
        entity.setSeq(seq);
        entity.setCreatedAt(createdAt);
        return entity;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
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

    public Long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Long createdAt) {
        this.createdAt = createdAt;
    }
}
