package com.github.im.storage.redis;

import com.github.im.contract.RedisKeys;
import com.github.im.server.group.GroupInfo;
import com.github.im.server.group.GroupRepository;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Redis 版群实体存储（{@code im.storage=redis}，缺省形态）。
 *
 * <pre>im:group:{groupId}:info   Hash，群资料</pre>
 *
 * <p>用 Hash 而不是序列化成一个 String：治理动作（改名、改公告、开全员禁言）
 * 都是<b>改一两个字段</b>，HSET 单字段不需要「读-改-写」整对象，
 * 并发改名与改公告不会互相覆盖。
 *
 * <p>生产形态用 {@code im.storage=jdbc} 切到 {@code im_group} 表
 * （{@code com.github.im.storage.jdbc.JdbcGroupRepository}）。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class RedisGroupRepository implements GroupRepository {

    private static final String FIELD_NAME = "name";
    private static final String FIELD_OWNER = "owner_id";
    private static final String FIELD_ANNOUNCEMENT = "announcement";
    private static final String FIELD_MAX_MEMBERS = "max_members";
    private static final String FIELD_CREATED_AT = "created_at";
    private static final String FIELD_MUTED_ALL = "muted_all";
    private static final String FIELD_DISMISSED = "dismissed";

    private final StringRedisTemplate redis;

    public RedisGroupRepository(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public Optional<GroupInfo> find(String groupId) {
        if (groupId == null || groupId.isBlank()) {
            return Optional.empty();
        }
        Map<Object, Object> raw = redis.opsForHash().entries(key(groupId));
        if (raw == null || raw.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new GroupInfo(
                groupId,
                text(raw, FIELD_NAME),
                text(raw, FIELD_OWNER),
                text(raw, FIELD_ANNOUNCEMENT),
                (int) number(raw, FIELD_MAX_MEMBERS, 200),
                number(raw, FIELD_CREATED_AT, 0L),
                "1".equals(text(raw, FIELD_MUTED_ALL)),
                "1".equals(text(raw, FIELD_DISMISSED))));
    }

    @Override
    public void save(GroupInfo info) {
        if (info == null || info.groupId() == null || info.groupId().isBlank()) {
            return;
        }
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put(FIELD_NAME, info.name() == null ? "" : info.name());
        fields.put(FIELD_OWNER, info.ownerId() == null ? "" : info.ownerId());
        fields.put(FIELD_ANNOUNCEMENT, info.announcement() == null ? "" : info.announcement());
        fields.put(FIELD_MAX_MEMBERS, String.valueOf(info.maxMembers()));
        fields.put(FIELD_CREATED_AT, String.valueOf(info.createdAt()));
        fields.put(FIELD_MUTED_ALL, info.mutedAll() ? "1" : "0");
        fields.put(FIELD_DISMISSED, info.dismissed() ? "1" : "0");
        redis.opsForHash().putAll(key(info.groupId()), fields);
    }

    /**
     * 只把 {@code dismissed} 置 1，不整体覆盖。
     *
     * <p>整体覆盖在这里是危险的：解散与改名并发时，后到的 save 会把 dismissed 又改回 0，
     * 于是「已解散的群」凭空复活。
     */
    @Override
    public boolean dismiss(String groupId) {
        Optional<GroupInfo> current = find(groupId);
        if (current.isEmpty() || current.get().dismissed()) {
            return false;
        }
        redis.opsForHash().put(key(groupId), FIELD_DISMISSED, "1");
        return true;
    }

    private static String key(String groupId) {
        return RedisKeys.groupInfo(groupId);
    }

    private static String text(Map<Object, Object> raw, String field) {
        Object value = raw.get(field);
        return value == null ? "" : value.toString();
    }

    private static long number(Map<Object, Object> raw, String field, long fallback) {
        Object value = raw.get(field);
        if (value == null) {
            return fallback;
        }
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
