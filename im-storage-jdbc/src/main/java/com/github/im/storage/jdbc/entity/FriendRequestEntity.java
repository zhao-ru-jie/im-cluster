package com.github.im.storage.jdbc.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

/**
 * {@code im_friend_request} 的持久化对象（好友申请单）。
 *
 * <p>与 {@link RelationEntity} 分开两张表：后者是「关系成立之后」的边，
 * 唯一键回答「是不是好友」；申请单是「关系成立之前」的流程，
 * 带 status / message / updated_at，且同一对用户只有一条（{@code uk_friend_request}）。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@TableName("im_friend_request")
public class FriendRequestEntity {

    @TableId(type = IdType.ASSIGN_ID)
    private Long id;

    /** 申请人 */
    private String fromId;

    /** 被申请人 */
    private String toId;

    /** PENDING / ACCEPTED / REJECTED */
    private String status;

    /** 验证消息（可为空串） */
    private String message;

    /** 首次申请时间：重新申请不刷新 */
    private Long createdAt;

    private Long updatedAt;

    public static FriendRequestEntity of(String fromId, String toId, String status, String message,
                                         long createdAt, long updatedAt) {
        FriendRequestEntity entity = new FriendRequestEntity();
        entity.setFromId(fromId);
        entity.setToId(toId);
        entity.setStatus(status);
        entity.setMessage(message == null ? "" : message);
        entity.setCreatedAt(createdAt);
        entity.setUpdatedAt(updatedAt);
        return entity;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getFromId() {
        return fromId;
    }

    public void setFromId(String fromId) {
        this.fromId = fromId;
    }

    public String getToId() {
        return toId;
    }

    public void setToId(String toId) {
        this.toId = toId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Long createdAt) {
        this.createdAt = createdAt;
    }

    public Long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Long updatedAt) {
        this.updatedAt = updatedAt;
    }
}
