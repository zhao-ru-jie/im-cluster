package com.github.im.storage.mongodb;

import com.github.im.server.group.GroupInfo;
import com.github.im.server.group.GroupRepository;
import com.github.im.storage.mongodb.entity.GroupEntity;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.Optional;

/**
 * MongoDB 版群实体存储（{@code im.storage=mongodb}），基于实体映射。
 *
 * <pre>im_group  一条文档 = 一个群，_id 就是 groupId</pre>
 *
 * <p>{@code _id} 直接用 groupId 而不是自增 ID：群 ID 会出现在会话 ID（{@code g:{groupId}}）
 * 并落进所有消息文档，换一次 ID 等于迁移全量数据。
 *
 * <p>整体覆盖保存（{@link #save}）与「只改一个字段」（{@link #dismiss}）必须分开：
 * 解散与改名并发时，后到的整体覆盖会把 {@code dismissed} 改回 false，
 * 于是「已解散的群」凭空复活。所以解散走带条件的单字段更新，
 * 由数据库保证「只有从未解散变已解散的那一次返回 true」。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class MongoGroupRepository implements GroupRepository {

    private final MongoTemplate mongo;

    public MongoGroupRepository(MongoTemplate mongo) {
        this.mongo = mongo;
    }

    /** 群不存在返回空，而不是一个默认对象：调用方要能区分「群不存在」与「群存在但没配置」 */
    @Override
    public Optional<GroupInfo> find(String groupId) {
        if (groupId == null || groupId.isBlank()) {
            return Optional.empty();
        }
        GroupEntity entity = mongo.findById(groupId, GroupEntity.class);
        return entity == null ? Optional.empty() : Optional.of(entity.toInfo());
    }

    /**
     * 整体覆盖保存（建群与治理动作都走它）。
     *
     * <p>{@code save} 在 {@code _id} 不存在时插入、存在时整体替换，正好是「群资料是一个整体」
     * 的语义。实体上所有字段都参与映射（含 {@code false}/{@code true}），
     * 不会出现「漏写字段的老文档」在替换后留下 undefined 状态。
     */
    @Override
    public void save(GroupInfo info) {
        if (info == null || info.groupId() == null || info.groupId().isBlank()) {
            return;
        }
        mongo.save(GroupEntity.from(info));
    }

    /**
     * 解散：带条件的单文档更新。
     *
     * <p>条件是「{@code dismissed} 不为 true」（字段缺失也算未解散），
     * 因此并发的两次解散只有一次会修改到文档，另一次返回 {@code false}——
     * 不需要「先查再写」的两步，也就不存在两步之间的竞争窗口。
     */
    @Override
    public boolean dismiss(String groupId) {
        if (groupId == null || groupId.isBlank()) {
            return false;
        }
        Query query = Query.query(Criteria.where("groupId").is(groupId).and("dismissed").ne(true));
        return mongo.updateFirst(query, new Update().set("dismissed", true), GroupEntity.class)
                .getModifiedCount() > 0;
    }
}
