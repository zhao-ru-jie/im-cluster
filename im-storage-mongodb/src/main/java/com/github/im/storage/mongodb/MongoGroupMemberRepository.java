package com.github.im.storage.mongodb;

import com.github.im.server.group.GroupMemberRepository;
import com.github.im.server.group.GroupRole;
import com.github.im.storage.mongodb.entity.GroupMemberEntity;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * MongoDB 版群成员存储（{@code im.storage=mongodb}），基于实体映射。
 *
 * <pre>im_group_member  一条文档 = 群 × 人，_id = groupId + ':' + userId
 * 字段：groupId / userId / role / mutedUntil / createdAt</pre>
 *
 * <h2>为什么角色与禁言挂在成员文档上</h2>
 *
 * <p>「某人在某群是什么角色、被禁言到什么时候」的生命周期与「他在不在这群里」完全一致：
 * 退群就该一起消失。放进同一个文档，删一次就干净；分成两张集合则要保证两次删除都成功，
 * 而漏删的后果是真实存在的越权路径——被踢掉的管理员重新加群时直接带着管理员权限。
 *
 * <h2>反向索引不需要额外维护</h2>
 *
 * <p>{@link #groupsOf} 走 {@code idx_group_member_user (userId)} 的普通二级查询即可。
 * Redis 形态必须额外维护 {@code im:group:user:{userId}} 这个 Set，是因为它没有二级索引；
 * MongoDB 有索引，就不该再引入一份需要双写的一致性负担。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class MongoGroupMemberRepository implements GroupMemberRepository {

    /** 默认角色：与 MySQL 形态的 {@code role DEFAULT 'MEMBER'} 对齐 */
    private static final String DEFAULT_ROLE = GroupRole.MEMBER.name();

    private final MongoTemplate mongo;

    public MongoGroupMemberRepository(MongoTemplate mongo) {
        this.mongo = mongo;
    }

    /**
     * 批量加成员（幂等）。
     *
     * <p>用 {@code $setOnInsert} 而不是 {@code $set}：已经在群里的人被重复添加时，
     * 既不该丢角色也不该重置禁言。一次 {@code BulkMode.UNORDERED} 提交把整批做完——
     * 无序模式是刻意的：某一条因唯一键冲突失败不该阻断其他人入群（与 {@code INSERT IGNORE} 同理）。
     */
    @Override
    public void addMembers(String groupId, Collection<String> userIds) {
        if (groupId == null || groupId.isBlank() || userIds == null || userIds.isEmpty()) {
            return;
        }
        long now = System.currentTimeMillis();
        BulkOperations bulk = mongo.bulkOps(BulkOperations.BulkMode.UNORDERED, GroupMemberEntity.class);
        int touched = 0;
        for (String userId : new LinkedHashSet<>(userIds)) {
            if (userId == null || userId.isBlank()) {
                continue;
            }
            bulk.upsert(memberOf(groupId, userId),
                    new Update()
                            .setOnInsert("groupId", groupId)
                            .setOnInsert("userId", userId)
                            .setOnInsert("role", DEFAULT_ROLE)
                            .setOnInsert("mutedUntil", 0L)
                            .setOnInsert("createdAt", now));
            touched++;
        }
        if (touched > 0) {
            bulk.execute();
        }
    }

    /** 删文档即同时清掉角色与禁言（它们本来就挂在这一条文档上） */
    @Override
    public void removeMember(String groupId, String userId) {
        if (!valid(groupId, userId)) {
            return;
        }
        mongo.remove(memberOf(groupId, userId), GroupMemberEntity.class);
    }

    @Override
    public List<String> membersOf(String groupId) {
        if (groupId == null || groupId.isBlank()) {
            return List.of();
        }
        Query query = Query.query(Criteria.where("groupId").is(groupId));
        query.fields().include("userId");
        List<String> members = new ArrayList<>();
        for (GroupMemberEntity entity : mongo.find(query, GroupMemberEntity.class)) {
            if (entity.getUserId() != null) {
                members.add(entity.getUserId());
            }
        }
        return members;
    }

    @Override
    public List<String> groupsOf(String userId) {
        if (userId == null || userId.isBlank()) {
            return List.of();
        }
        Query query = Query.query(Criteria.where("userId").is(userId));
        query.fields().include("groupId");
        List<String> groups = new ArrayList<>();
        for (GroupMemberEntity entity : mongo.find(query, GroupMemberEntity.class)) {
            if (entity.getGroupId() != null) {
                groups.add(entity.getGroupId());
            }
        }
        return groups;
    }

    /** O(1)：发言准入在每条群消息上都要跑，不能拉全量成员（{@code _id} 上的存在性查询） */
    @Override
    public boolean isMember(String groupId, String userId) {
        if (!valid(groupId, userId)) {
            return false;
        }
        return mongo.exists(memberOf(groupId, userId), GroupMemberEntity.class);
    }

    /** 非成员返回 {@code null}：调用方必须能区分「不是成员」与「是普通成员」 */
    @Override
    public GroupRole roleOf(String groupId, String userId) {
        if (!valid(groupId, userId)) {
            return null;
        }
        Query query = memberOf(groupId, userId);
        query.fields().include("role");
        GroupMemberEntity entity = mongo.findOne(query, GroupMemberEntity.class);
        return entity == null ? null : entity.roleOrDefault();
    }

    /** 只改已有成员的角色：不在群里的人不该被「设角色」凭空变成成员 */
    @Override
    public void setRole(String groupId, String userId, GroupRole role) {
        if (!valid(groupId, userId) || role == null) {
            return;
        }
        mongo.updateFirst(memberOf(groupId, userId), new Update().set("role", role.name()),
                GroupMemberEntity.class);
    }

    @Override
    public Map<String, GroupRole> rolesOf(String groupId) {
        if (groupId == null || groupId.isBlank()) {
            return Map.of();
        }
        Query query = Query.query(Criteria.where("groupId").is(groupId));
        query.fields().include("userId").include("role");
        Map<String, GroupRole> roles = new LinkedHashMap<>();
        for (GroupMemberEntity entity : mongo.find(query, GroupMemberEntity.class)) {
            if (entity.getUserId() != null) {
                roles.put(entity.getUserId(), entity.roleOrDefault());
            }
        }
        return roles;
    }

    /** 过期的禁言按「未禁言」返回；字段不清理（下一次 mute 覆盖，读路径不该有写副作用） */
    @Override
    public long mutedUntil(String groupId, String userId) {
        if (!valid(groupId, userId)) {
            return 0L;
        }
        Query query = memberOf(groupId, userId);
        query.fields().include("mutedUntil");
        GroupMemberEntity entity = mongo.findOne(query, GroupMemberEntity.class);
        if (entity == null) {
            return 0L;
        }
        long until = entity.getMutedUntil();
        return until > System.currentTimeMillis() ? until : 0L;
    }

    @Override
    public void mute(String groupId, String userId, long untilMillis) {
        if (!valid(groupId, userId)) {
            return;
        }
        mongo.updateFirst(memberOf(groupId, userId),
                new Update().set("mutedUntil", Math.max(untilMillis, 0L)),
                GroupMemberEntity.class);
    }

    private static Query memberOf(String groupId, String userId) {
        return Query.query(Criteria.where("id").is(GroupMemberEntity.idOf(groupId, userId)));
    }

    private static boolean valid(String groupId, String userId) {
        return groupId != null && !groupId.isBlank() && userId != null && !userId.isBlank();
    }
}
