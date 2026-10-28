package com.github.im.storage.mongodb;

import com.github.im.server.relation.RelationRepository;
import com.github.im.storage.mongodb.entity.RelationEntity;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * 关系链的 MongoDB 实现（{@code im.storage=mongodb}），基于实体映射。
 *
 * <pre>im_relation  一条文档 = 一条有向边，_id = kind + ':' + ownerId + ':' + targetId</pre>
 *
 * <h2>为什么是有向边</h2>
 *
 * <p>好友写成两条（A→B、B→A），黑名单写一条（我拉黑了谁）。这样
 * 「A 和 B 是不是好友 / A 有没有拉黑 B」永远是 {@code _id} 上的点查，
 * 而准入判定是<b>每条单聊消息</b>都要跑一次的热路径。写放大 2 倍换热路径 O(1)，
 * 与 MySQL 形态把 {@code (owner_id, target_id, relation_type)} 做成唯一键是同一个取舍。
 *
 * <p>「好友」判定要求两个方向都存在：同步中途失败留下的单边残留不该被当成关系成立——
 * 那等于给「让某人发不出消息」留了一条没人维护的后门。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class MongoRelationRepository implements RelationRepository {

    private final MongoTemplate mongo;

    public MongoRelationRepository(MongoTemplate mongo) {
        this.mongo = mongo;
    }

    @Override
    public boolean areFriends(String a, String b) {
        if (!valid(a) || !valid(b) || a.equals(b)) {
            return false;
        }
        return exists(RelationEntity.idOf(RelationEntity.TYPE_FRIEND, a, b))
                && exists(RelationEntity.idOf(RelationEntity.TYPE_FRIEND, b, a));
    }

    @Override
    public boolean isBlocked(String ownerId, String otherId) {
        if (!valid(ownerId) || !valid(otherId)) {
            return false;
        }
        return exists(RelationEntity.idOf(RelationEntity.TYPE_BLOCK, ownerId, otherId));
    }

    /**
     * 批量建立双向好友（幂等）。
     *
     * <p>关系同步是「整批重放」的形态，所以用 {@code $setOnInsert} + 无序批量写：
     * 已存在的边保持原有 {@code createdAt}（那是关系建立的证据），新边一次插入。
     */
    @Override
    public void addFriends(String userId, Collection<String> friendIds) {
        if (!valid(userId) || friendIds == null || friendIds.isEmpty()) {
            return;
        }
        long now = System.currentTimeMillis();
        BulkOperations bulk = mongo.bulkOps(BulkOperations.BulkMode.UNORDERED, RelationEntity.class);
        int edges = 0;
        for (String friendId : new LinkedHashSet<>(friendIds)) {
            if (!valid(friendId) || friendId.equals(userId)) {
                continue;
            }
            bulk.upsert(edge(RelationEntity.TYPE_FRIEND, userId, friendId),
                    insertOnly(RelationEntity.TYPE_FRIEND, userId, friendId, now));
            bulk.upsert(edge(RelationEntity.TYPE_FRIEND, friendId, userId),
                    insertOnly(RelationEntity.TYPE_FRIEND, friendId, userId, now));
            edges += 2;
        }
        if (edges > 0) {
            bulk.execute();
        }
    }

    @Override
    public void removeFriend(String a, String b) {
        if (!valid(a) || !valid(b)) {
            return;
        }
        mongo.remove(edge(RelationEntity.TYPE_FRIEND, a, b), RelationEntity.class);
        mongo.remove(edge(RelationEntity.TYPE_FRIEND, b, a), RelationEntity.class);
    }

    /** 拉黑是单向且幂等的：重复拉黑不该刷新时间，也不该影响好友边 */
    @Override
    public void block(String ownerId, String otherId) {
        if (!valid(ownerId) || !valid(otherId) || ownerId.equals(otherId)) {
            return;
        }
        mongo.upsert(edge(RelationEntity.TYPE_BLOCK, ownerId, otherId),
                insertOnly(RelationEntity.TYPE_BLOCK, ownerId, otherId, System.currentTimeMillis()),
                RelationEntity.class);
    }

    @Override
    public void unblock(String ownerId, String otherId) {
        if (!valid(ownerId) || !valid(otherId)) {
            return;
        }
        mongo.remove(edge(RelationEntity.TYPE_BLOCK, ownerId, otherId), RelationEntity.class);
    }

    @Override
    public List<String> friendsOf(String userId) {
        return targetsOf(userId, RelationEntity.TYPE_FRIEND);
    }

    @Override
    public List<String> blockedOf(String userId) {
        return targetsOf(userId, RelationEntity.TYPE_BLOCK);
    }

    private List<String> targetsOf(String userId, String kind) {
        if (!valid(userId)) {
            return List.of();
        }
        Query query = Query.query(Criteria.where("ownerId").is(userId).and("kind").is(kind));
        query.fields().include("targetId");
        List<String> targets = new ArrayList<>();
        for (RelationEntity entity : mongo.find(query, RelationEntity.class)) {
            if (entity.getTargetId() != null) {
                targets.add(entity.getTargetId());
            }
        }
        return targets;
    }

    private boolean exists(String id) {
        return mongo.exists(Query.query(Criteria.where("id").is(id)), RelationEntity.class);
    }

    private static Query edge(String kind, String ownerId, String targetId) {
        return Query.query(Criteria.where("id").is(RelationEntity.idOf(kind, ownerId, targetId)));
    }

    private static Update insertOnly(String kind, String ownerId, String targetId, long now) {
        return new Update()
                .setOnInsert("ownerId", ownerId)
                .setOnInsert("targetId", targetId)
                .setOnInsert("kind", kind)
                .setOnInsert("createdAt", now);
    }

    private static boolean valid(String userId) {
        return StringUtils.hasText(userId);
    }
}
