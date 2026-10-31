package com.github.im.storage.redis;

import com.github.im.contract.RedisKeys;
import com.github.im.server.group.GroupInvite;
import com.github.im.server.group.GroupJoinRequest;
import com.github.im.server.group.GroupJoinStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 入群申请与邀请码的 Redis 实现（<b>与 {@code im.storage} 无关</b>，始终走 Redis）。
 *
 * <pre>
 * im:group:{groupId}:join-req          Hash，field = applicantId，value = 申请单 JSON（带 TTL）
 * im:group:{groupId}:invites           Set，该群放出去的邀请码（只做列出/吊销索引）
 * im:invite:{code}                     Hash，邀请码本体（带 TTL）
 * </pre>
 *
 * <h2>为什么邀请码存 Hash 字段而不是一段 JSON</h2>
 *
 * <p>{@code uses} 必须能被 Lua <b>原子自增</b>，而 JSON 在脚本里无法安全地「解析-加一-写回」。
 * 拆成字段后，限量校验与自增在同一段脚本里完成——{@code maxUses=1} 的链接才不会在并发下被用两次。
 *
 * <h2>为什么申请反倒用 JSON</h2>
 *
 * <p>申请的读路径是「整条拿出来给审批人看」，没有字段级原子操作；用一个 Hash 装整条 JSON，
 * 既省了多次往返，也避免了「字段加一个、三个实现都要改」的维护成本。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class RedisGroupJoinStore implements GroupJoinStore {

    private static final Logger log = LoggerFactory.getLogger(RedisGroupJoinStore.class);

    private static final String FIELD_GROUP_ID = "group_id";
    private static final String FIELD_INVITER = "inviter_id";
    private static final String FIELD_CREATED_AT = "created_at";
    private static final String FIELD_EXPIRES_AT = "expires_at";
    private static final String FIELD_MAX_USES = "max_uses";
    private static final String FIELD_USES = "uses";
    private static final String FIELD_APPROVAL = "approval_required";

    /**
     * 校验 + 自增一步到位。
     *
     * <p>用 Lua 而不是 {@code WATCH/MULTI}：限量校验是「读两个字段再写一个字段」，
     * 事务重试在这种高频小操作上反而更贵，而单 key 脚本本身就能保证原子。
     *
     * <p>返回单值 {@code OK / NOT_FOUND / EXPIRED / EXHAUSTED}；成功时 {@code uses} 已加一。
     */
    private static final byte[] USE_INVITE = """
            local groupId = redis.call('HGET', KEYS[1], 'group_id')
            if not groupId then return 'NOT_FOUND' end
            local expiresAt = tonumber(redis.call('HGET', KEYS[1], 'expires_at') or '0')
            if expiresAt > 0 and tonumber(ARGV[1]) >= expiresAt then return 'EXPIRED' end
            local maxUses = tonumber(redis.call('HGET', KEYS[1], 'max_uses') or '0')
            local uses = tonumber(redis.call('HGET', KEYS[1], 'uses') or '0')
            if maxUses > 0 and uses >= maxUses then return 'EXHAUSTED' end
            redis.call('HINCRBY', KEYS[1], 'uses', 1)
            return 'OK'
            """.getBytes(StandardCharsets.UTF_8);

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    /** 申请单的存活时长：过了这个时间没人处理就自然消失（审批列表不该无限堆积） */
    private final Duration requestTtl;
    /** 长期有效（{@code expiresAt=0}）的邀请码也给它一个上限，避免「永不过期」的链接被留下 */
    private final Duration inviteTtl;

    public RedisGroupJoinStore(StringRedisTemplate redis, ObjectMapper objectMapper,
                               Duration requestTtl, Duration inviteTtl) {
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.requestTtl = requestTtl == null || requestTtl.isZero() || requestTtl.isNegative()
                ? Duration.ofDays(7) : requestTtl;
        this.inviteTtl = inviteTtl == null || inviteTtl.isZero() || inviteTtl.isNegative()
                ? Duration.ofDays(30) : inviteTtl;
    }

    // ==== 入群申请 ====

    @Override
    public void saveRequest(GroupJoinRequest request) {
        if (request == null || request.groupId() == null || request.applicantId() == null) {
            return;
        }
        String key = requestsKey(request.groupId());
        redis.opsForHash().put(key, request.applicantId(), toJson(request));
        // 每写一次就续期：一个还在被申请的群不该因为最早的申请太久而整批消失
        redis.expire(key, requestTtl);
    }

    @Override
    public GroupJoinRequest findRequest(String groupId, String applicantId) {
        if (groupId == null || applicantId == null) {
            return null;
        }
        Object raw = redis.opsForHash().get(requestsKey(groupId), applicantId);
        return raw == null ? null : parseRequest(raw.toString());
    }

    @Override
    public List<GroupJoinRequest> pendingRequests(String groupId) {
        if (groupId == null) {
            return List.of();
        }
        Map<Object, Object> stored = redis.opsForHash().entries(requestsKey(groupId));
        if (stored.isEmpty()) {
            return List.of();
        }
        long now = System.currentTimeMillis();
        long oldest = now - requestTtl.toMillis();
        List<GroupJoinRequest> result = new ArrayList<>();
        for (Object value : stored.values()) {
            GroupJoinRequest request = parseRequest(String.valueOf(value));
            // 已经过了 TTL 却还在 Hash 里（Hash 的过期是整键的，个别条目会残留）
            if (request != null && request.isPending() && request.createdAt() >= oldest) {
                result.add(request);
            }
        }
        // 先来先审：审批人关心的是「谁等了最久」
        result.sort(Comparator.comparingLong(GroupJoinRequest::createdAt)
                .thenComparing(GroupJoinRequest::applicantId));
        return result;
    }

    @Override
    public void deleteRequest(String groupId, String applicantId) {
        if (groupId == null || applicantId == null) {
            return;
        }
        redis.opsForHash().delete(requestsKey(groupId), applicantId);
    }

    // ==== 邀请码 ====

    @Override
    public void saveInvite(GroupInvite invite) {
        if (invite == null || invite.code() == null || invite.groupId() == null) {
            return;
        }
        String key = inviteKey(invite.code());
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put(FIELD_GROUP_ID, invite.groupId());
        fields.put(FIELD_INVITER, invite.inviterId() == null ? "" : invite.inviterId());
        fields.put(FIELD_CREATED_AT, String.valueOf(invite.createdAt()));
        fields.put(FIELD_EXPIRES_AT, String.valueOf(invite.expiresAt()));
        fields.put(FIELD_MAX_USES, String.valueOf(invite.maxUses()));
        fields.put(FIELD_USES, String.valueOf(invite.uses()));
        fields.put(FIELD_APPROVAL, invite.approvalRequired() ? "1" : "0");
        redis.opsForHash().putAll(key, fields);
        redis.expire(key, resolveInviteTtl(invite));
        // 索引单独续期：它只服务于管理端的「我放出去了哪些链接」
        redis.opsForSet().add(invitesKey(invite.groupId()), invite.code());
        redis.expire(invitesKey(invite.groupId()), inviteTtl);
    }

    @Override
    public GroupInvite findInvite(String code) {
        if (code == null || code.isBlank()) {
            return null;
        }
        Map<Object, Object> stored = redis.opsForHash().entries(inviteKey(code));
        return stored.isEmpty() ? null : parseInvite(code, stored);
    }

    @Override
    public List<GroupInvite> invitesOf(String groupId) {
        if (groupId == null) {
            return List.of();
        }
        Set<String> codes = redis.opsForSet().members(invitesKey(groupId));
        if (codes == null || codes.isEmpty()) {
            return List.of();
        }
        long now = System.currentTimeMillis();
        List<GroupInvite> result = new ArrayList<>(codes.size());
        List<String> stale = new ArrayList<>();
        for (String code : codes) {
            GroupInvite invite = findInvite(code);
            if (invite == null || invite.expired(now)) {
                // 失效的码顺手从索引里摘掉：邀请码数量少，惰性清理足够
                stale.add(code);
                continue;
            }
            result.add(invite);
        }
        if (!stale.isEmpty()) {
            redis.opsForSet().remove(invitesKey(groupId), stale.toArray());
        }
        result.sort(Comparator.comparingLong(GroupInvite::createdAt).reversed());
        return result;
    }

    @Override
    public void revokeInvite(String groupId, String code) {
        if (code == null) {
            return;
        }
        redis.delete(inviteKey(code));
        if (groupId != null) {
            redis.opsForSet().remove(invitesKey(groupId), code);
        }
    }

    @Override
    public InviteUse useInvite(String code, long now) {
        if (code == null || code.isBlank()) {
            return InviteUse.of(InviteUse.Status.NOT_FOUND, null);
        }
        String status;
        try {
            // 注意返回类型：走的是原始 scriptingCommands，不经过 template 的序列化器，
            // Lua 返回的字符串到手是 byte[]。按 String 接会 ClassCastException，
            // 被下面的 catch 吞掉之后表现为「所有邀请码都不存在」——只有真连一次才看得见。
            Object raw = redis.execute((RedisCallback<Object>) connection ->
                    connection.scriptingCommands().eval(
                            USE_INVITE, ReturnType.VALUE, 1,
                            inviteKey(code).getBytes(StandardCharsets.UTF_8),
                            String.valueOf(now).getBytes(StandardCharsets.UTF_8)));
            status = raw instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : null;
        } catch (RuntimeException e) {
            // 存储抖动时不要给出「可用」：宁可让调用方看到失败，也不要让限量链接多放一次人进来
            log.warn("[group] 取用邀请码失败 code={} err={}", code, e.getMessage());
            return InviteUse.of(InviteUse.Status.NOT_FOUND, null);
        }
        InviteUse.Status parsed = parseUseStatus(status);
        if (parsed != InviteUse.Status.OK) {
            return InviteUse.of(parsed, null);
        }
        // 自增之后的形态（uses 已是新值）用于日志与后续判断
        return InviteUse.of(InviteUse.Status.OK, findInvite(code));
    }

    // ==== 内部 ====

    private static InviteUse.Status parseUseStatus(String raw) {
        String value = raw == null ? "" : raw;
        return switch (value.trim()) {
            case "OK" -> InviteUse.Status.OK;
            case "EXPIRED" -> InviteUse.Status.EXPIRED;
            case "EXHAUSTED" -> InviteUse.Status.EXHAUSTED;
            default -> InviteUse.Status.NOT_FOUND;
        };
    }

    /** 码的有效期：显式过期时间优先，否则用兜底 TTL */
    private Duration resolveInviteTtl(GroupInvite invite) {
        if (invite.expiresAt() <= 0) {
            return inviteTtl;
        }
        long remaining = invite.expiresAt() - System.currentTimeMillis();
        // 多留 1 分钟：过期判断以字段为准，key 提前消失会让「已过期」变成「不存在」，两者对用户是同一结论，
        // 但排查问题时「存在但过期」的信息更有价值
        return Duration.ofMillis(Math.max(remaining, 0L) + 60_000L);
    }

    private GroupInvite parseInvite(String code, Map<Object, Object> stored) {
        try {
            return new GroupInvite(code,
                    str(stored, FIELD_GROUP_ID),
                    str(stored, FIELD_INVITER),
                    num(stored, FIELD_CREATED_AT),
                    num(stored, FIELD_EXPIRES_AT),
                    (int) num(stored, FIELD_MAX_USES),
                    (int) num(stored, FIELD_USES),
                    "1".equals(str(stored, FIELD_APPROVAL)));
        } catch (RuntimeException e) {
            log.warn("[group] 邀请码字段损坏，按不存在处理 code={} err={}", code, e.getMessage());
            return null;
        }
    }

    private GroupJoinRequest parseRequest(String json) {
        try {
            return objectMapper.readValue(json, GroupJoinRequest.class);
        } catch (RuntimeException e) {
            log.warn("[group] 申请单解析失败，跳过 err={}", e.getMessage());
            return null;
        }
    }

    private String toJson(GroupJoinRequest request) {
        try {
            return objectMapper.writeValueAsString(request);
        } catch (RuntimeException e) {
            throw new IllegalStateException("入群申请序列化失败: " + e.getMessage(), e);
        }
    }

    private static String str(Map<Object, Object> stored, String field) {
        Object value = stored.get(field);
        return value == null ? "" : String.valueOf(value);
    }

    private static long num(Map<Object, Object> stored, String field) {
        Object value = stored.get(field);
        if (value == null) {
            return 0L;
        }
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private static String requestsKey(String groupId) {
        return RedisKeys.groupJoinRequests(groupId);
    }

    private static String invitesKey(String groupId) {
        return RedisKeys.groupInvites(groupId);
    }

    private static String inviteKey(String code) {
        return RedisKeys.groupInvite(code);
    }
}
