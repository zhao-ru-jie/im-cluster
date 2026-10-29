package com.github.im.storage.redis;

import com.github.im.contract.RedisKeys;
import com.github.im.server.relation.FriendRequest;
import com.github.im.server.relation.FriendRequestRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.StringUtils;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * 好友申请的 Redis 实现（{@code im.storage=redis}，缺省形态）。
 *
 * <pre>
 * im:user:{A}:friend-req:out   Hash，field = B，value = 申请单 JSON   （A 发出的）
 * im:user:{B}:friend-req:in    Hash，field = A，value = 申请单 JSON   （B 收到的）
 * </pre>
 *
 * <h2>为什么是「两份 Hash」而不是「一份成绩单」</h2>
 *
 * <p>申请是<b>有向</b>的（A 申请 B ≠ B 申请 A），而列表页要的是「我收到的」和「我发出的」
 * 两屏。只在一边存，另一边就得全库扫；存一份 pair 哈希再按前缀扫，同样躲不掉 O(N)。
 * 两侧各写一份把查询压成一次 {@code HGETALL}，代价是同一份 JSON 存两遍——
 * 申请是低频写、数据量以「人」计，这个放大完全值得。
 *
 * <h2>为什么 value 存整条 JSON</h2>
 *
 * <p>申请单有 status / message / 两个时间戳，用一个 Hash 的多字段表达就得再设计一层
 * field 前缀（{@code A:status}、{@code A:message}…），读写都变成多次往返；
 * 存整条 JSON 让 {@link #save} 是一次 {@code HSET}、{@link #incoming} 是一次 {@code HGETALL}。
 * 代价是不能在 Redis 侧按状态过滤——申请列表最多几十条，在内存里过滤不值得优化。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class RedisFriendRequestRepository implements FriendRequestRepository {

    private static final Logger log = LoggerFactory.getLogger(RedisFriendRequestRepository.class);

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;

    public RedisFriendRequestRepository(StringRedisTemplate redis, ObjectMapper objectMapper) {
        this.redis = redis;
        this.objectMapper = objectMapper;
    }

    @Override
    public void save(FriendRequest request) {
        if (request == null || !valid(request.fromId()) || !valid(request.toId())) {
            return;
        }
        String json = objectMapper.writeValueAsString(request);
        // 两边都写：申请人看「发出的」，被申请人看「收到的」
        redis.opsForHash().put(inKey(request.toId()), request.fromId(), json);
        redis.opsForHash().put(outKey(request.fromId()), request.toId(), json);
    }

    @Override
    public FriendRequest find(String fromId, String toId) {
        if (!valid(fromId) || !valid(toId)) {
            return null;
        }
        Object raw = redis.opsForHash().get(inKey(toId), fromId);
        return parse(raw instanceof String text ? text : null);
    }

    @Override
    public List<FriendRequest> incoming(String userId) {
        return entries(inKey(userId));
    }

    @Override
    public List<FriendRequest> outgoing(String userId) {
        return entries(outKey(userId));
    }

    @Override
    public void delete(String fromId, String toId) {
        if (!valid(fromId) || !valid(toId)) {
            return;
        }
        redis.opsForHash().delete(inKey(toId), fromId);
        redis.opsForHash().delete(outKey(fromId), toId);
    }

    /** 一次 HGETALL 拿全套，脏行跳过而不是整页失败：一条坏数据不该让「新的朋友」空白 */
    private List<FriendRequest> entries(String key) {
        Map<Object, Object> raw = redis.opsForHash().entries(key);
        if (raw == null || raw.isEmpty()) {
            return List.of();
        }
        List<FriendRequest> list = new ArrayList<>(raw.size());
        for (Object value : raw.values()) {
            FriendRequest request = parse(value instanceof String text ? text : null);
            if (request != null) {
                list.add(request);
            }
        }
        list.sort(Comparator.comparingLong(FriendRequest::updatedAt).reversed());
        return list;
    }

    private FriendRequest parse(String json) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            return objectMapper.readValue(json, FriendRequest.class);
        } catch (RuntimeException e) {
            log.warn("[relation] 好友申请记录解析失败，已跳过：{}", e.getMessage());
            return null;
        }
    }

    private static boolean valid(String userId) {
        return StringUtils.hasText(userId);
    }

    private static String inKey(String userId) {
        return RedisKeys.userFriendRequestsIn(userId);
    }

    private static String outKey(String userId) {
        return RedisKeys.userFriendRequestsOut(userId);
    }
}
