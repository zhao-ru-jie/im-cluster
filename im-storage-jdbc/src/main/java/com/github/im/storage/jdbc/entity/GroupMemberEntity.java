package com.github.im.storage.jdbc.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

/**
 * {@code im_group_member} 的持久化对象。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@TableName("im_group_member")
public class GroupMemberEntity {

    @TableId(type = IdType.ASSIGN_ID)
    private Long id;

    private String groupId;

    private String userId;

    /** OWNER / ADMIN / MEMBER */
    private String role;

    /** 禁言到期时间（毫秒），0 = 未禁言 */
    private Long muteUntil;

    private Long createdAt;

    public static GroupMemberEntity of(String groupId, String userId, String role, long createdAt) {
        GroupMemberEntity entity = new GroupMemberEntity();
        entity.setGroupId(groupId);
        entity.setUserId(userId);
        entity.setRole(role);
        entity.setCreatedAt(createdAt);
        return entity;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public Long getMuteUntil() {
        return muteUntil;
    }

    public void setMuteUntil(Long muteUntil) {
        this.muteUntil = muteUntil;
    }

    public Long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Long createdAt) {
        this.createdAt = createdAt;
    }
}
