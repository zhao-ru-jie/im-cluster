package com.github.im.storage.jdbc.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

/**
 * {@code im_relation} 的持久化对象（好友 / 黑名单）。
 *
 * <p>好友是双向的两行、黑名单是单向的一行，见 {@code schema.sql} 里的说明：
 * 用「多存一行」换「热路径上的点查」，因为准入判定在每条消息上都要跑。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@TableName("im_relation")
public class RelationEntity {

    public static final String TYPE_FRIEND = "FRIEND";
    public static final String TYPE_BLOCK = "BLOCK";

    @TableId(type = IdType.ASSIGN_ID)
    private Long id;

    /** 关系主体：好友/黑名单是他的 */
    private String ownerId;

    /** 关系对象 */
    private String targetId;

    /** FRIEND / BLOCK */
    private String relationType;

    private Long createdAt;

    public static RelationEntity of(String ownerId, String targetId, String relationType, long createdAt) {
        RelationEntity entity = new RelationEntity();
        entity.setOwnerId(ownerId);
        entity.setTargetId(targetId);
        entity.setRelationType(relationType);
        entity.setCreatedAt(createdAt);
        return entity;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    public String getTargetId() {
        return targetId;
    }

    public void setTargetId(String targetId) {
        this.targetId = targetId;
    }

    public String getRelationType() {
        return relationType;
    }

    public void setRelationType(String relationType) {
        this.relationType = relationType;
    }

    public Long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Long createdAt) {
        this.createdAt = createdAt;
    }
}
