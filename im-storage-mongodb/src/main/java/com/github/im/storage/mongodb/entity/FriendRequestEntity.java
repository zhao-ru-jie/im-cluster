package com.github.im.storage.mongodb.entity;

import com.github.im.server.relation.FriendRequest;
import com.github.im.storage.mongodb.MongoCollections;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoId;

/**
 * {@code im_friend_request} 的持久化对象：一条文档 = 一条好友申请。
 *
 * <p>{@code _id} 由 {@code fromId + ':' + toId} 唯一确定，因此「同一对用户只有一条申请」
 * 是文档主键级别的约束，不依赖额外查询；{@code status} 的迁移（PENDING → ACCEPTED/REJECTED）
 * 就是一次 {@code $set}。
 *
 * <p>与 {@code im_relation} 分成两个集合的原因见 MySQL 形态的
 * {@code im_friend_request} DDL 注释：边表只回答「是不是好友」，装不下流程字段。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Document(collection = MongoCollections.FRIEND_REQUEST)
@CompoundIndex(name = "uk_friend_request", def = "{'fromId': 1, 'toId': 1}", unique = true)
public class FriendRequestEntity {

    /** {@code fromId + ':' + toId}：一对用户一条记录，重复申请走覆盖 */
    @MongoId
    private String id;

    /** 申请人 */
    private String fromId;

    /** 被申请人；「谁在等我同意」按它反查 */
    @Indexed(name = "idx_friend_request_to")
    private String toId;

    /** PENDING / ACCEPTED / REJECTED */
    private String status;

    /** 验证消息 */
    private String message;

    /** 首次申请时间（重新申请不刷新） */
    private long createdAt;

    private long updatedAt;

    public static String idOf(String fromId, String toId) {
        return fromId + ':' + toId;
    }

    /**
     * 转回领域模型。
     *
     * <p>转换写在实体上而不是仓储里，与 {@code GroupEntity#toInfo()} /
     * {@code MessageEntity#toStoredMessage()} 一致：字段名写错的表现是「某个字段永远是 null」，
     * 只有把它放在能被单元测试直接调用的位置，才测得出来。
     */
    public FriendRequest toFriendRequest() {
        return new FriendRequest(fromId, toId, status, message == null ? "" : message, createdAt, updatedAt);
    }

    public static FriendRequestEntity of(String fromId, String toId, String status, String message,
                                         long createdAt, long updatedAt) {
        FriendRequestEntity entity = new FriendRequestEntity();
        entity.setId(idOf(fromId, toId));
        entity.setFromId(fromId);
        entity.setToId(toId);
        entity.setStatus(status);
        entity.setMessage(message == null ? "" : message);
        entity.setCreatedAt(createdAt);
        entity.setUpdatedAt(updatedAt);
        return entity;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
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

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }
}
