package com.github.im.storage.mongodb;

import com.github.im.server.relation.FriendRequest;
import com.github.im.server.relation.FriendRequestRepository;
import com.github.im.storage.mongodb.entity.FriendRequestEntity;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * 好友申请的 MongoDB 实现（{@code im.storage=mongodb}）。
 *
 * <pre>im_friend_request  一条文档 = 一条申请，_id = fromId + ':' + toId</pre>
 *
 * <p>写入用 {@code $set + $setOnInsert} 的 upsert：状态与附言是可变部分（{@code $set}），
 * 而 {@code createdAt} 是「首次申请时间」的证据，重新申请必须保留（{@code $setOnInsert}）。
 * 这与 {@code MongoRelationRepository} 里「边一旦建立就不刷新 createdAt」是同一个取舍。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class MongoFriendRequestRepository implements FriendRequestRepository {

    private final MongoTemplate mongo;

    public MongoFriendRequestRepository(MongoTemplate mongo) {
        this.mongo = mongo;
    }

    @Override
    public void save(FriendRequest request) {
        if (request == null || !valid(request.fromId()) || !valid(request.toId())) {
            return;
        }
        Update update = new Update()
                .set("status", request.status())
                .set("message", request.message() == null ? "" : request.message())
                .set("updatedAt", request.updatedAt())
                .setOnInsert("fromId", request.fromId())
                .setOnInsert("toId", request.toId())
                .setOnInsert("createdAt", request.createdAt());
        mongo.upsert(byPair(request.fromId(), request.toId()), update, FriendRequestEntity.class);
    }

    @Override
    public FriendRequest find(String fromId, String toId) {
        if (!valid(fromId) || !valid(toId)) {
            return null;
        }
        FriendRequestEntity entity = mongo.findOne(byPair(fromId, toId), FriendRequestEntity.class);
        return entity == null ? null : entity.toFriendRequest();
    }

    @Override
    public List<FriendRequest> incoming(String userId) {
        return list(Criteria.where("toId").is(userId));
    }

    @Override
    public List<FriendRequest> outgoing(String userId) {
        return list(Criteria.where("fromId").is(userId));
    }

    @Override
    public void delete(String fromId, String toId) {
        if (!valid(fromId) || !valid(toId)) {
            return;
        }
        mongo.remove(byPair(fromId, toId), FriendRequestEntity.class);
    }

    private List<FriendRequest> list(Criteria criteria) {
        Query query = Query.query(criteria).with(Sort.by(Sort.Direction.DESC, "updatedAt"));
        List<FriendRequest> result = new ArrayList<>();
        for (FriendRequestEntity entity : mongo.find(query, FriendRequestEntity.class)) {
            result.add(entity.toFriendRequest());
        }
        return result;
    }

    private static Query byPair(String fromId, String toId) {
        return Query.query(Criteria.where("id").is(FriendRequestEntity.idOf(fromId, toId)));
    }

    private static boolean valid(String userId) {
        return StringUtils.hasText(userId);
    }
}
