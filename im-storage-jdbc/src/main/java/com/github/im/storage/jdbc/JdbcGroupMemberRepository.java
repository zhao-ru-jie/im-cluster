package com.github.im.storage.jdbc;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.github.im.server.group.GroupMemberRepository;
import com.github.im.storage.jdbc.entity.GroupMemberEntity;
import com.github.im.storage.jdbc.mapper.GroupMemberMapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * MySQL 版群成员存储（{@code im.storage=jdbc}），基于 MyBatis-Plus。
 *
 * <p>表结构见 {@code db/schema.sql} 的 {@code im_group_member}：
 * {@code uk_group_member (group_id, user_id)} 保证加成员幂等，
 * {@code idx_group_member_user (user_id)} 支撑「某用户加入了哪些群」的反查（Redis Set 做不到这一点）。
 *
 * <p>与 Redis 版的行为差异：这里第一版统一写 {@code role=MEMBER}。角色（OWNER/ADMIN）留给
 * 群管理接口引入，不在此处凭空发明默认群主——扇出只关心「谁在群里」。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class JdbcGroupMemberRepository implements GroupMemberRepository {

    /**
     * 一条 INSERT 里最多塞多少人。
     *
     * <p>每人 4 个占位符，500 人 = 2000 个参数，离 MySQL 的 65535 上限很远，
     * 同时也避免单条 SQL 过长把 max_allowed_packet 顶爆。
     */
    private static final int INSERT_CHUNK = 500;

    private static final String DEFAULT_ROLE = "MEMBER";

    private final GroupMemberMapper mapper;

    public JdbcGroupMemberRepository(GroupMemberMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public void addMembers(String groupId, Collection<String> userIds) {
        if (userIds == null || userIds.isEmpty()) {
            return;
        }
        long now = System.currentTimeMillis();
        List<String> batch = new ArrayList<>(userIds);
        for (int from = 0; from < batch.size(); from += INSERT_CHUNK) {
            List<String> chunk = batch.subList(from, Math.min(from + INSERT_CHUNK, batch.size()));
            // INSERT IGNORE：重复加人由 uk_group_member 静默跳过（等价于 Redis 的 SADD）
            mapper.insertIgnoreBatch(groupId, chunk, DEFAULT_ROLE, now);
        }
    }

    @Override
    public void removeMember(String groupId, String userId) {
        mapper.delete(Wrappers.<GroupMemberEntity>lambdaQuery()
                .eq(GroupMemberEntity::getGroupId, groupId)
                .eq(GroupMemberEntity::getUserId, userId));
    }

    @Override
    public List<String> membersOf(String groupId) {
        List<GroupMemberEntity> rows = mapper.selectList(Wrappers.<GroupMemberEntity>lambdaQuery()
                .select(GroupMemberEntity::getUserId)
                .eq(GroupMemberEntity::getGroupId, groupId));
        List<String> members = new ArrayList<>(rows.size());
        for (GroupMemberEntity row : rows) {
            members.add(row.getUserId());
        }
        return members;
    }
}
