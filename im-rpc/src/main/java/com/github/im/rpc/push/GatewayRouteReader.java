package com.github.im.rpc.push;

import com.github.im.contract.RedisKeys;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Set;

/**
 * 在线路由表的<b>只读</b>视图：{@code im:gw:user:{userId}} → 网关实例集合。
 *
 * <p>写入权只属于长连接服务（{@code GatewayRegistry}）。IM 服务通过本类读取，
 * 用来把「推给 userId」翻译成「推到哪些网关实例」。这条约束保证路由表只有一个写者，
 * 不会出现两侧互相覆盖导致的路由抖动。
 *
 * <p>多端语义：同一用户可能同时连在多个网关实例上（手机在 A、网页在 C），
 * 所以这里返回的是<b>集合</b>，调用方必须每个实例都推一次；只取一个会让部分设备收不到实时消息。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class GatewayRouteReader {

    private final StringRedisTemplate redis;

    public GatewayRouteReader(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /**
     * @return 该用户当前所在的网关实例集合；空集合 = 不在线（或路由已 TTL 过期）
     */
    public Set<String> instancesOf(String userId) {
        String key = RedisKeys.GATEWAY_USER_PREFIX + userId;
        try {
            Set<String> members = redis.opsForSet().members(key);
            return members == null ? Set.of() : members;
        } catch (RuntimeException e) {
            // 滚动升级期间可能遇到旧版写入的 String 类型 key：按离线处理，
            // 由 IM 服务的离线队列兜底；网关会很快用 SET 重写这个 key（TTL 30s）
            return Set.of();
        }
    }
}
