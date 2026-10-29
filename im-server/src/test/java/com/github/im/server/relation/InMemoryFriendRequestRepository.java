package com.github.im.server.relation;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 测试用的内存版好友申请存储。
 *
 * <p>存在理由与 {@link InMemoryRelationRepository} 相同：{@code RelationService} 的规则测试
 * 不能反向依赖 im-storage-*（那会构成 reactor 环）。这里刻意复刻真实实现的三个语义：
 * <ul>
 *   <li><b>按 (fromId, toId) 唯一</b>：同一对用户多次申请只有一条，重新申请是覆盖；</li>
 *   <li><b>列表按 updatedAt 倒序</b>：与 SQL 的 {@code order by updated_at desc} 一致，
 *       否则测试里断言「第一条是刚刚那条」会时对时错；</li>
 *   <li><b>incoming / outgoing 是同一份数据的两个视角</b>：删一条两边都看不到。</li>
 * </ul>
 */
public final class InMemoryFriendRequestRepository implements FriendRequestRepository {

    private final Map<String, FriendRequest> rows = new ConcurrentHashMap<>();

    @Override
    public void save(FriendRequest request) {
        if (request == null) {
            return;
        }
        rows.put(key(request.fromId(), request.toId()), request);
    }

    @Override
    public FriendRequest find(String fromId, String toId) {
        return rows.get(key(fromId, toId));
    }

    @Override
    public List<FriendRequest> incoming(String userId) {
        return list(request -> request.toId().equals(userId));
    }

    @Override
    public List<FriendRequest> outgoing(String userId) {
        return list(request -> request.fromId().equals(userId));
    }

    @Override
    public void delete(String fromId, String toId) {
        rows.remove(key(fromId, toId));
    }

    private List<FriendRequest> list(java.util.function.Predicate<FriendRequest> filter) {
        List<FriendRequest> matched = new ArrayList<>();
        for (FriendRequest request : rows.values()) {
            if (filter.test(request)) {
                matched.add(request);
            }
        }
        matched.sort(Comparator.comparingLong(FriendRequest::updatedAt).reversed());
        return matched;
    }

    private static String key(String fromId, String toId) {
        return fromId + '\u0000' + toId;
    }
}
