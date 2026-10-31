package com.github.im.storage.redis;

import com.github.im.contract.RedisKeys;
import com.github.im.server.group.GroupMemberRepository;
import com.github.im.server.group.GroupRole;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Redis 版群成员存储（{@code im.storage=redis}，缺省形态）。
 *
 * <pre>
 * im:group:members:{groupId}   Set，成员 userId
 * im:group:user:{userId}       Set，加入的 groupId（反向索引）
 * </pre>
 *
 * <p>用 Set 而不是 List：加/删成员天然幂等，扇出也只需要「是不是成员」而不关心顺序。
 * 生产形态用 {@code im.storage=jdbc} 切到 {@code im_group_member} 表——那里天然支持
 * 按 {@code user_id} 建索引反查，也能带上角色做权限。
 *
 * <p><b>为什么要维护反向索引</b>：正查（群 → 成员）只够做扇出。会话列表需要的是
 * 「我加入了哪些群」——大群的会话列表写扩散会被跳过，只能靠它补齐。
 * 两个方向必须同时维护，所以写入时一次 pipeline 把正反两边的 SADD 都做掉。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
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
        List<String> ids = List.copyOf(userIds);
        redis.opsForSet().add(membersKey(groupId), ids.toArray(String[]::new));
        // 反向索引：解一次群成员之外，还让「用户加入的群」变成一次 SMEMBERS
        for (String userId : ids) {
            redis.opsForSet().add(groupsKey(userId), groupId);
        }
    }

    @Override
    public void removeMember(String groupId, String userId) {
        redis.opsForSet().remove(membersKey(groupId), userId);
        // 两边必须一起摘：留着反向索引会让退群的人在自己的会话列表里一直看到这个群
        redis.opsForSet().remove(groupsKey(userId), groupId);
        // 角色与禁言也必须一起清：否则「被踢掉的管理员」再加回来就直接带着管理员权限，
        // 那是一类真实存在的越权路径
        redis.opsForHash().delete(rolesKey(groupId), userId);
        redis.opsForHash().delete(mutesKey(groupId), userId);
    }

    @Override
    public GroupRole roleOf(String groupId, String userId) {
        if (!isMember(groupId, userId)) {
            return null;
        }
        Object raw = redis.opsForHash().get(rolesKey(groupId), userId);
        return GroupRole.parse(raw == null ? null : raw.toString());
    }

    @Override
    public void setRole(String groupId, String userId, GroupRole role) {
        if (groupId == null || userId == null || role == null) {
            return;
        }
        if (role == GroupRole.MEMBER) {
            // MEMBER 是缺省值，不存：否则这张表会随群人数线性增长
            redis.opsForHash().delete(rolesKey(groupId), userId);
        } else {
            redis.opsForHash().put(rolesKey(groupId), userId, role.name());
        }
    }

    @Override
    public Map<String, GroupRole> rolesOf(String groupId) {
        Map<Object, Object> stored = redis.opsForHash().entries(rolesKey(groupId));
        Map<String, GroupRole> roles = new HashMap<>();
        for (String userId : membersOf(groupId)) {
            roles.put(userId, GroupRole.MEMBER);
        }
        for (Map.Entry<Object, Object> entry : stored.entrySet()) {
            String userId = String.valueOf(entry.getKey());
            // 只回填仍在群里的：角色表可能残留已退群的人（历史数据），不该凭空造出成员
            if (roles.containsKey(userId)) {
                roles.put(userId, GroupRole.parse(String.valueOf(entry.getValue())));
            }
        }
        return roles;
    }

    /**
     * 一次 {@code HMGET} 取这几个人的角色。
     *
     * <p>刻意<b>不</b>像 {@link #roleOf} 那样先判成员资格：调用方（成员列表分页）的 id 本来就来自
     * 成员集合，再补一次 {@code SISMEMBER} 只是把 1 次往返变 2 次。角色的缺省值是 MEMBER，
     * 所以 Hash 里查不到的人按 MEMBER 回填即可。
     */
    @Override
    public Map<String, GroupRole> rolesOf(String groupId, Collection<String> userIds) {
        Map<String, GroupRole> result = new HashMap<>();
        if (userIds == null || userIds.isEmpty()) {
            return result;
        }
        List<Object> fields = new ArrayList<>(userIds);
        List<Object> stored = redis.opsForHash().multiGet(rolesKey(groupId), fields);
        int index = 0;
        for (String userId : userIds) {
            Object raw = index < stored.size() ? stored.get(index) : null;
            index++;
            result.put(userId, raw == null ? GroupRole.MEMBER : GroupRole.parse(String.valueOf(raw)));
        }
        return result;
    }

    @Override
    public long mutedUntil(String groupId, String userId) {
        if (groupId == null || userId == null) {
            return 0L;
        }
        Object raw = redis.opsForHash().get(mutesKey(groupId), userId);
        if (raw == null) {
            return 0L;
        }
        long until;
        try {
            until = Long.parseLong(raw.toString());
        } catch (NumberFormatException e) {
            return 0L;
        }
        if (until <= System.currentTimeMillis()) {
            // 过期的禁言顺手清掉，免得这张表只增不减
            redis.opsForHash().delete(mutesKey(groupId), userId);
            return 0L;
        }
        return until;
    }

    @Override
    public void mute(String groupId, String userId, long untilMillis) {
        if (groupId == null || userId == null) {
            return;
        }
        if (untilMillis <= System.currentTimeMillis()) {
            redis.opsForHash().delete(mutesKey(groupId), userId);
        } else {
            redis.opsForHash().put(mutesKey(groupId), userId, String.valueOf(untilMillis));
        }
    }

    private String rolesKey(String groupId) {
        return RedisKeys.groupRoles(groupId);
    }

    private String mutesKey(String groupId) {
        return RedisKeys.groupMutes(groupId);
    }

    @Override
    public List<String> membersOf(String groupId) {
        Set<String> members = redis.opsForSet().members(membersKey(groupId));
        return members == null ? List.of() : new ArrayList<>(members);
    }

    /** O(1)：拉人前判人数上限只需要一个数字，不该为此拉全量成员 */
    @Override
    public long memberCount(String groupId) {
        Long size = redis.opsForSet().size(membersKey(groupId));
        return size == null ? 0L : size;
    }

    /**
     * 一次 {@code HGETALL} 取全部禁言状态。
     *
     * <p>此前成员列表对每个人各查一次 {@code mutedUntil}：5000 人群就是 5000 次往返，
     * 而这张 Hash 里通常只有个位数条目。顺手把过期的字段删掉（与 {@link #mutedUntil} 同样的清理策略）。
     */
    @Override
    public Map<String, Long> mutesOf(String groupId) {
        Map<Object, Object> stored = redis.opsForHash().entries(mutesKey(groupId));
        if (stored.isEmpty()) {
            return Map.of();
        }
        long now = System.currentTimeMillis();
        Map<String, Long> result = new HashMap<>(stored.size());
        List<Object> expired = new ArrayList<>();
        for (Map.Entry<Object, Object> entry : stored.entrySet()) {
            String userId = String.valueOf(entry.getKey());
            long until;
            try {
                until = Long.parseLong(String.valueOf(entry.getValue()));
            } catch (NumberFormatException e) {
                expired.add(userId);
                continue;
            }
            if (until > now) {
                result.put(userId, until);
            } else {
                expired.add(userId);
            }
        }
        if (!expired.isEmpty()) {
            redis.opsForHash().delete(mutesKey(groupId), expired.toArray());
        }
        return result;
    }

    @Override
    public List<String> groupsOf(String userId) {
        Set<String> groups = redis.opsForSet().members(groupsKey(userId));
        return groups == null ? List.of() : new ArrayList<>(groups);
    }

    /** O(1)：发言准入在每条群消息上都要跑一次，不能走全量 SMEMBERS */
    @Override
    public boolean isMember(String groupId, String userId) {
        if (groupId == null || userId == null) {
            return false;
        }
        return Boolean.TRUE.equals(redis.opsForSet().isMember(membersKey(groupId), userId));
    }

    private String membersKey(String groupId) {
        return RedisKeys.groupMembers(groupId);
    }

    private String groupsKey(String userId) {
        return RedisKeys.userGroups(userId);
    }
}
