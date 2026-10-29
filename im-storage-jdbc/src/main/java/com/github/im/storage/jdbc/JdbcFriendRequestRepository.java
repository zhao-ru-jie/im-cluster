package com.github.im.storage.jdbc;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.github.im.server.relation.FriendRequest;
import com.github.im.server.relation.FriendRequestRepository;
import com.github.im.storage.jdbc.entity.FriendRequestEntity;
import com.github.im.storage.jdbc.mapper.FriendRequestMapper;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * 好友申请的 MySQL 实现（{@code im.storage=jdbc}）。
 *
 * <h2>为什么是「查一次再写」而不是 INSERT ... ON DUPLICATE KEY UPDATE</h2>
 *
 * <p>申请单的写入是<b>用户触发的单条操作</b>（点一次「申请」/「同意」），不是
 * {@code RelationRepository#addFriends} 那种整批重放，所以不存在「并发批量插入」的压力。
 * 先 {@code selectOne} 再决定 insert / updateById，能顺便拿到 id 与 createdAt，
 * 也不必再为「upsert 时保留首次申请时间」写一段 {@code last_insert_id} 的赋值 SQL。
 *
 * <p>唯一键 {@code uk_friend_request(from_id, to_id)} 仍然保留：它是并发下的最终防线
 * （同一个人的两个端同时点申请），撞了就由 MyBatis 抛 DuplicateKeyException，
 * 由上层 {@code RelationService} 的幂等语义兜住（重复申请本来就不改变什么）。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class JdbcFriendRequestRepository implements FriendRequestRepository {

    private final FriendRequestMapper mapper;

    public JdbcFriendRequestRepository(FriendRequestMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public void save(FriendRequest request) {
        if (request == null || !valid(request.fromId()) || !valid(request.toId())) {
            return;
        }
        FriendRequestEntity existing = row(request.fromId(), request.toId());
        if (existing == null) {
            mapper.insert(FriendRequestEntity.of(request.fromId(), request.toId(), request.status(),
                    request.message(), request.createdAt(), request.updatedAt()));
            return;
        }
        existing.setStatus(request.status());
        existing.setMessage(request.message() == null ? "" : request.message());
        existing.setUpdatedAt(request.updatedAt());
        mapper.updateById(existing);
    }

    @Override
    public FriendRequest find(String fromId, String toId) {
        if (!valid(fromId) || !valid(toId)) {
            return null;
        }
        return toRequest(row(fromId, toId));
    }

    @Override
    public List<FriendRequest> incoming(String userId) {
        return list(Wrappers.<FriendRequestEntity>lambdaQuery()
                .eq(FriendRequestEntity::getToId, userId));
    }

    @Override
    public List<FriendRequest> outgoing(String userId) {
        return list(Wrappers.<FriendRequestEntity>lambdaQuery()
                .eq(FriendRequestEntity::getFromId, userId));
    }

    @Override
    public void delete(String fromId, String toId) {
        if (!valid(fromId) || !valid(toId)) {
            return;
        }
        mapper.delete(pair(fromId, toId));
    }

    private FriendRequestEntity row(String fromId, String toId) {
        return mapper.selectOne(pair(fromId, toId));
    }

    private List<FriendRequest> list(LambdaQueryWrapper<FriendRequestEntity> query) {
        // 最近处理的排前面：列表页第一屏就是用户最关心的那几条
        query.orderByDesc(FriendRequestEntity::getUpdatedAt);
        List<FriendRequest> result = new ArrayList<>();
        for (FriendRequestEntity entity : mapper.selectList(query)) {
            FriendRequest request = toRequest(entity);
            if (request != null) {
                result.add(request);
            }
        }
        return result;
    }

    private static FriendRequest toRequest(FriendRequestEntity entity) {
        if (entity == null) {
            return null;
        }
        return new FriendRequest(entity.getFromId(), entity.getToId(), entity.getStatus(),
                entity.getMessage() == null ? "" : entity.getMessage(),
                entity.getCreatedAt() == null ? 0L : entity.getCreatedAt(),
                entity.getUpdatedAt() == null ? 0L : entity.getUpdatedAt());
    }

    private static LambdaQueryWrapper<FriendRequestEntity> pair(String fromId, String toId) {
        return Wrappers.<FriendRequestEntity>lambdaQuery()
                .eq(FriendRequestEntity::getFromId, fromId)
                .eq(FriendRequestEntity::getToId, toId);
    }

    private static boolean valid(String userId) {
        return StringUtils.hasText(userId);
    }
}
