package com.github.im.server.relation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 测试用的内存版关系链存储：只保证 {@link RelationRepository} 的语义，不模拟介质。
 *
 * <p>为什么需要它：Redis 实现已搬到 im-storage-redis，而 im-server 的测试不能反向依赖那个模块
 * （im-storage-redis → im-server 是编译依赖，再加 test 依赖就构成 reactor 环）。
 * 这里刻意复刻两处容易做错的语义：
 * <ul>
 *   <li><b>好友双向</b>：{@code areFriends} 要求两边互为好友，只写一边必须判否；</li>
 *   <li><b>黑名单单向</b>：A 拉黑 B 不影响 B 的列表，也不影响 B 的消息能发给 A 之外的人。</li>
 * </ul>
 */
public final class InMemoryRelationRepository implements RelationRepository {

    private final Map<String, Set<String>> friends = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> blocks = new ConcurrentHashMap<>();

    @Override
    public boolean areFriends(String a, String b) {
        if (!valid(a) || !valid(b) || a.equals(b)) {
            return false;
        }
        return friends.getOrDefault(a, Set.of()).contains(b)
                && friends.getOrDefault(b, Set.of()).contains(a);
    }

    @Override
    public boolean isBlocked(String ownerId, String otherId) {
        if (!valid(ownerId) || !valid(otherId)) {
            return false;
        }
        return blocks.getOrDefault(ownerId, Set.of()).contains(otherId);
    }

    @Override
    public void addFriends(String userId, Collection<String> friendIds) {
        if (!valid(userId) || friendIds == null || friendIds.isEmpty()) {
            return;
        }
        for (String friendId : new LinkedHashSet<>(friendIds)) {
            if (!valid(friendId) || friendId.equals(userId)) {
                continue;
            }
            friends.computeIfAbsent(userId, key -> ConcurrentHashMap.newKeySet()).add(friendId);
            friends.computeIfAbsent(friendId, key -> ConcurrentHashMap.newKeySet()).add(userId);
        }
    }

    @Override
    public void removeFriend(String a, String b) {
        if (!valid(a) || !valid(b)) {
            return;
        }
        remove(friends, a, b);
        remove(friends, b, a);
    }

    @Override
    public void block(String ownerId, String otherId) {
        if (!valid(ownerId) || !valid(otherId) || ownerId.equals(otherId)) {
            return;
        }
        blocks.computeIfAbsent(ownerId, key -> ConcurrentHashMap.newKeySet()).add(otherId);
    }

    @Override
    public void unblock(String ownerId, String otherId) {
        if (!valid(ownerId) || !valid(otherId)) {
            return;
        }
        remove(blocks, ownerId, otherId);
    }

    /** 不能写成 {@code getOrDefault(k, Set.of()).remove(v)}：{@code Set.of()} 不可变，未命中时会抛异常 */
    private static void remove(Map<String, Set<String>> index, String key, String value) {
        Set<String> current = index.get(key);
        if (current != null) {
            current.remove(value);
        }
    }

    @Override
    public List<String> friendsOf(String userId) {
        return new ArrayList<>(friends.getOrDefault(userId, Set.of()));
    }

    @Override
    public List<String> blockedOf(String userId) {
        return new ArrayList<>(blocks.getOrDefault(userId, Set.of()));
    }

    private static boolean valid(String userId) {
        return userId != null && !userId.isBlank();
    }
}
