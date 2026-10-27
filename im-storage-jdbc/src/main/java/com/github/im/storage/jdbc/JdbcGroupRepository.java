package com.github.im.storage.jdbc;

import com.github.im.server.group.GroupInfo;
import com.github.im.server.group.GroupRepository;
import com.github.im.storage.jdbc.entity.GroupEntity;
import com.github.im.storage.jdbc.mapper.GroupMapper;

import java.util.Optional;

/**
 * MySQL 版群实体存储（{@code im.storage=jdbc}）。
 *
 * <p>表结构见 {@code db/schema.sql} 的 {@code im_group}：主键就是 {@code group_id}，
 * 因为群 ID 会出现在会话 ID（{@code g:{groupId}}）并落进所有消息行，换 ID 等于迁移全量数据。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class JdbcGroupRepository implements GroupRepository {

    private final GroupMapper mapper;

    public JdbcGroupRepository(GroupMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Optional<GroupInfo> find(String groupId) {
        if (groupId == null || groupId.isBlank()) {
            return Optional.empty();
        }
        GroupEntity entity = mapper.selectById(groupId);
        return entity == null ? Optional.empty() : Optional.of(entity.toInfo());
    }

    /**
     * 整体覆盖保存。
     *
     * <p>用「先查再决定 insert 还是 update」而不是 upsert：群资料是低频写（治理动作），
     * 多一次查询换来的确定性远比省下的往返值钱——MySQL 的 {@code ON DUPLICATE KEY}
     * 与 MyBatis-Plus 的 {@code saveOrUpdate} 在字段为 null 时行为不同，容易踩坑。
     */
    @Override
    public void save(GroupInfo info) {
        if (info == null || info.groupId() == null || info.groupId().isBlank()) {
            return;
        }
        GroupEntity entity = GroupEntity.from(info);
        if (mapper.selectById(info.groupId()) == null) {
            mapper.insert(entity);
        } else {
            mapper.updateById(entity);
        }
    }

    /**
     * 只更新 {@code dismissed}，不整体覆盖。
     *
     * <p>整体覆盖在这里会丢并发更新：解散与改名同时发生时，后到的 save 可能把
     * {@code dismissed} 改回 0，于是已解散的群凭空复活。
     */
    @Override
    public boolean dismiss(String groupId) {
        Optional<GroupInfo> current = find(groupId);
        if (current.isEmpty() || current.get().dismissed()) {
            return false;
        }
        GroupEntity patch = new GroupEntity();
        patch.setGroupId(groupId);
        patch.setDismissed(1);
        mapper.updateById(patch);
        return true;
    }
}
