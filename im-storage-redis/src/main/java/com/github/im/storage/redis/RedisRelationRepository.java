package com.github.im.storage.redis;

import com.github.im.contract.RedisKeys;
import com.github.im.server.relation.RelationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 关系链的 Redis 实现（{@code im.storage=redis}，缺省形态）。
 *
 * <pre>
 * im:rel:friend:{userId}   Set，好友
 * im:rel:block:{userId}    Set，被我拉黑的人
 * </pre>
 *
 * <p><b>为什么好友要写两份</b>：关系本身对称，但准入判定是热路径上的 O(1) 查询
 * （每条单聊消息一次）。只存一份就得反查、或者维护一张「关系对」的哈希——
 * 后者在 Cluster 下两边 userId 不同 slot，查询要重定向，反而更慢。
 *
 * <p><b>为什么写入用 pipeline 而不是 Lua</b>：{@code im:rel:friend:{a}} 与
 * {@code im:rel:friend:{b}} 落在不同 slot，Cluster 下跨 slot 脚本会直接被拒。
 * pipeline 不保证原子，但这个操作的失败后果只是「单向关系」，重试即收敛——
 * 为它引入分布式锁，代价远大于收益。
 *
 * <p>生产形态用 {@code im.storage=jdbc} 切到 {@code im_relation} 表
 * （{@code com.github.im.storage.jdbc.JdbcRelationRepository}）。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class RedisRelationRepository implements RelationRepository {

    private static final Logger log = LoggerFactory.getLogger(RedisRelationRepository.class);

    private final StringRedisTemplate redis;

    public RedisRelationRepository(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public boolean areFriends(String a, String b) {
        if (!valid(a) || !valid(b) || a.equals(b)) {
            return false;
        }
        return Boolean.TRUE.equals(redis.opsForSet().isMember(friendKey(a), b))
                && Boolean.TRUE.equals(redis.opsForSet().isMember(friendKey(b), a));
    }

    @Override
    public boolean isBlocked(String ownerId, String otherId) {
        if (!valid(ownerId) || !valid(otherId)) {
            return false;
        }
        return Boolean.TRUE.equals(redis.opsForSet().isMember(blockKey(ownerId), otherId));
    }

    @Override
    public void addFriends(String userId, Collection<String> friendIds) {
        if (!valid(userId) || friendIds == null || friendIds.isEmpty()) {
            return;
        }
        List<String> targets = new ArrayList<>();
        for (String friendId : new LinkedHashSet<>(friendIds)) {
            if (valid(friendId) && !friendId.equals(userId)) {
                targets.add(friendId);
            }
        }
        if (targets.isEmpty()) {
            return;
        }
        try {
            // 两次 SADD：跨 slot 不能进一个脚本，理由见类注释
            redis.executePipelined((RedisCallback<Object>) connection -> {
                for (String friendId : targets) {
                    connection.setCommands().sAdd(raw(friendKey(userId)), raw(friendId));
                    connection.setCommands().sAdd(raw(friendKey(friendId)), raw(userId));
                }
                return null;
            });
        } catch (RuntimeException e) {
            log.warn("[relation] 建立好友关系失败 user={} count={} err={}",
                    userId, targets.size(), e.getMessage());
            throw e;
        }
    }

    @Override
    public void removeFriend(String a, String b) {
        if (!valid(a) || !valid(b)) {
            return;
        }
        redis.opsForSet().remove(friendKey(a), b);
        redis.opsForSet().remove(friendKey(b), a);
    }

    @Override
    public void block(String ownerId, String otherId) {
        if (!valid(ownerId) || !valid(otherId) || ownerId.equals(otherId)) {
            return;
        }
        redis.opsForSet().add(blockKey(ownerId), otherId);
    }

    @Override
    public void unblock(String ownerId, String otherId) {
        if (!valid(ownerId) || !valid(otherId)) {
            return;
        }
        redis.opsForSet().remove(blockKey(ownerId), otherId);
    }

    @Override
    public List<String> friendsOf(String userId) {
        return members(friendKey(userId));
    }

    @Override
    public List<String> blockedOf(String userId) {
        return members(blockKey(userId));
    }

    private List<String> members(String key) {
        Set<String> members = redis.opsForSet().members(key);
        return members == null ? List.of() : new ArrayList<>(members);
    }

    private static boolean valid(String userId) {
        return StringUtils.hasText(userId);
    }

    private static String friendKey(String userId) {
        return RedisKeys.userFriends(userId);
    }

    private static String blockKey(String userId) {
        return RedisKeys.userBlocks(userId);
    }

    private static byte[] raw(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
