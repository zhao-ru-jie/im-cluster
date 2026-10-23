package com.github.im.rpc.push;

import com.github.im.contract.RedisKeys;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * 在线路由表的<b>只读</b>视图：{@code im:gw:user:{userId}} → 网关实例 ID。
 *
 * <p>写入权只属于长连接服务（{@code GatewayRegistry}）。IM 服务通过本类读取，
 * 用来把「推给 userId」翻译成「推到哪个网关实例」。这条约束保证路由表只有一个写者，
 * 不会出现两侧互相覆盖导致的路由抖动。
 */
public class GatewayRouteReader {

    private final StringRedisTemplate redis;

    public GatewayRouteReader(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /** @return 网关实例 ID；null = 不在线（或路由已 TTL 过期） */
    public String instanceOf(String userId) {
        return redis.opsForValue().get(RedisKeys.GATEWAY_USER_PREFIX + userId);
    }
}
