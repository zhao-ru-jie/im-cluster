package com.github.im.server.group;

import com.github.im.contract.RedisKeys;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Redis 版群成员存储（默认实现）。
 *
 * <pre>im:group:members:{groupId}   Set，成员 userId</pre>
 *
 * <p>用 Set 而不是 List：加/删成员天然幂等，扇出也只需要「是不是成员」而不关心顺序。
 * 生产形态用 {@code im.storage=jdbc} 切到 {@code im_group_member} 表——那时才能支持
 * 「查某用户加入了哪些群」「按角色做权限」，Redis Set 做不到。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
@ConditionalOnProperty(name = "im.storage", havingValue = "redis", matchIfMissing = true)
public class RedisGroupMemberRepository implements GroupMemberRepository {

    private final StringRedisTemplate redis;

    public RedisGroupMemberRepository(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public void addMembers(String groupId, Collection<String> userIds) {
        if (userIds == null || userIds.isEmpty()) {
            return;
        }
        redis.opsForSet().add(key(groupId), userIds.toArray(String[]::new));
    }

    @Override
    public void removeMember(String groupId, String userId) {
        redis.opsForSet().remove(key(groupId), userId);
    }

    @Override
    public List<String> membersOf(String groupId) {
        Set<String> members = redis.opsForSet().members(key(groupId));
        return members == null ? List.of() : new ArrayList<>(members);
    }

    private String key(String groupId) {
        return RedisKeys.GROUP_MEMBERS_PREFIX + groupId;
    }
}
