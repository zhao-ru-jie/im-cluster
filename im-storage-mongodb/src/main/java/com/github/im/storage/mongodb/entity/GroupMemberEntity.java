package com.github.im.storage.mongodb.entity;

import com.github.im.server.group.GroupRole;
import com.github.im.storage.mongodb.MongoCollections;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoId;

/**
 * {@code im_group_member} 的持久化对象（群 × 人，一条文档）。
 *
 * <p>角色与禁言刻意挂在这条文档上：它们的生命周期与「他在不在这群里」完全一致，退群就该一起消失。
 * 分成两张集合则要保证两次删除都成功，而漏删的后果是真实存在的越权路径——
 * 被踢掉的管理员重新加群时直接带着管理员权限。
 *
 * <p>{@code userId} 上有单列索引，所以「我加入了哪些群」是一次普通二级查询。
 * Redis 形态必须额外维护 {@code im:group:user:{userId}} 这个反向 Set，是因为它没有二级索引；
 * 这里不该再引入一份需要双写的一致性负担。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Document(collection = MongoCollections.GROUP_MEMBER)
@CompoundIndex(name = "uk_group_member", def = "{'groupId': 1, 'userId': 1}", unique = true)
public class GroupMemberEntity {

    /** {@code groupId + ':' + userId}：同一个人在同一个群里只有一条 */
    @MongoId
    private String id;

    private String groupId;

    /** 反向索引：某用户加入了哪些群（会话列表补齐大群用） */
    @Indexed(name = "idx_group_member_user")
    private String userId;

    /** OWNER / ADMIN / MEMBER（缺省 MEMBER） */
    private String role;

    /** 禁言到期时间（毫秒），0 或小于当前时间表示未禁言 */
    private long mutedUntil;

    private long createdAt;

    public static String idOf(String groupId, String userId) {
        return groupId + ':' + userId;
    }

    /** 角色解析失败按最小权限处理（{@link GroupRole#parse} 的既定语义），不做「解析失败就放行」 */
    public GroupRole roleOrDefault() {
        return GroupRole.parse(role);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
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

    public long getMutedUntil() {
        return mutedUntil;
    }

    public void setMutedUntil(long mutedUntil) {
        this.mutedUntil = mutedUntil;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }
}
