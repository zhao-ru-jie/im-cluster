package com.github.im.storage.jdbc;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.github.im.server.relation.RelationRepository;
import com.github.im.storage.jdbc.entity.RelationEntity;
import com.github.im.storage.jdbc.mapper.RelationMapper;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * 关系链的 MySQL 实现（{@code im.storage=jdbc}）。
 *
 * <p>与 Redis 形态的差别只在介质：
 * <ul>
 *   <li>好友双向两行、黑名单单向一行——都为了 {@code (owner_id, target_id, relation_type)}
 *       唯一键上的点查，理由见 {@code schema.sql}；</li>
 *   <li>批量写入走 {@code INSERT IGNORE} 而不是两次 SADD：关系同步是幂等重放，
 *       「先查再插」在并发下照样撞唯一键。</li>
 * </ul>
 *
 * <p>好友全量列表按 {@code idx_relation_target} 的兄弟索引 {@code uk_relation} 前缀扫，
 * 因此不需要额外建 {@code owner_id} 单列索引。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class JdbcRelationRepository implements RelationRepository {

    /** 单条 INSERT 里的最大行数：双向 × 500 人 = 1000 行，远低于 MySQL 参数上限 */
    private static final int INSERT_CHUNK = 500;

    private final RelationMapper mapper;

    public JdbcRelationRepository(RelationMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public boolean areFriends(String a, String b) {
        if (!valid(a) || !valid(b) || a.equals(b)) {
            return false;
        }
        // 两侧都存在才算好友：单边残留（同步中途失败）不该被当成关系成立
        return exists(a, b, RelationEntity.TYPE_FRIEND) && exists(b, a, RelationEntity.TYPE_FRIEND);
    }

    @Override
    public boolean isBlocked(String ownerId, String otherId) {
        if (!valid(ownerId) || !valid(otherId)) {
            return false;
        }
        return exists(ownerId, otherId, RelationEntity.TYPE_BLOCK);
    }

    @Override
    public void addFriends(String userId, Collection<String> friendIds) {
        if (!valid(userId) || friendIds == null || friendIds.isEmpty()) {
            return;
        }
        long now = System.currentTimeMillis();
        List<RelationEntity> rows = new ArrayList<>();
        for (String friendId : new LinkedHashSet<>(friendIds)) {
            if (!valid(friendId) || friendId.equals(userId)) {
                continue;
            }
            rows.add(RelationEntity.of(userId, friendId, RelationEntity.TYPE_FRIEND, now));
            rows.add(RelationEntity.of(friendId, userId, RelationEntity.TYPE_FRIEND, now));
        }
        for (int from = 0; from < rows.size(); from += INSERT_CHUNK) {
            mapper.insertIgnoreBatch(rows.subList(from, Math.min(from + INSERT_CHUNK, rows.size())));
        }
    }

    @Override
    public void removeFriend(String a, String b) {
        if (!valid(a) || !valid(b)) {
            return;
        }
        mapper.delete(relationOf(a, b, RelationEntity.TYPE_FRIEND));
        mapper.delete(relationOf(b, a, RelationEntity.TYPE_FRIEND));
    }

    @Override
    public void block(String ownerId, String otherId) {
        if (!valid(ownerId) || !valid(otherId) || ownerId.equals(otherId)) {
            return;
        }
        mapper.insertIgnoreBatch(List.of(
                RelationEntity.of(ownerId, otherId, RelationEntity.TYPE_BLOCK, System.currentTimeMillis())));
    }

    @Override
    public void unblock(String ownerId, String otherId) {
        if (!valid(ownerId) || !valid(otherId)) {
            return;
        }
        mapper.delete(relationOf(ownerId, otherId, RelationEntity.TYPE_BLOCK));
    }

    @Override
    public List<String> friendsOf(String userId) {
        return targetsOf(userId, RelationEntity.TYPE_FRIEND);
    }

    @Override
    public List<String> blockedOf(String userId) {
        return targetsOf(userId, RelationEntity.TYPE_BLOCK);
    }

    private List<String> targetsOf(String userId, String relationType) {
        List<RelationEntity> rows = mapper.selectList(Wrappers.<RelationEntity>lambdaQuery()
                .select(RelationEntity::getTargetId)
                .eq(RelationEntity::getOwnerId, userId)
                .eq(RelationEntity::getRelationType, relationType));
        List<String> targets = new ArrayList<>(rows.size());
        for (RelationEntity row : rows) {
            targets.add(row.getTargetId());
        }
        return targets;
    }

    private boolean exists(String ownerId, String targetId, String relationType) {
        Long count = mapper.selectCount(relationOf(ownerId, targetId, relationType));
        return count != null && count > 0;
    }

    private static com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper<RelationEntity> relationOf(
            String ownerId, String targetId, String relationType) {
        return Wrappers.<RelationEntity>lambdaQuery()
                .eq(RelationEntity::getOwnerId, ownerId)
                .eq(RelationEntity::getTargetId, targetId)
                .eq(RelationEntity::getRelationType, relationType);
    }

    private static boolean valid(String userId) {
        return StringUtils.hasText(userId);
    }
}
