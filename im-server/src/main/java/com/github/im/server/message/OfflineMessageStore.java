package com.github.im.server.message;

import com.github.im.contract.RedisKeys;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 离线消息存储（归属 IM 服务，不再放在连接层）。
 *
 * <p>拆分前它和 WebSocket 会话放在同一个包里，会让「连接」与「消息」两种生命周期
 * 混在一起。现在它的写入方只有 IM 服务：只有当 {@code PushPort} 明确返回失败
 * （{@code failed_user_ids}）时才落这里，网关侧不再有写离线队列的代码路径。
 *
 * <pre>im:offline:{userId}   Redis List，FIFO</pre>
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class OfflineMessageStore {

    private final StringRedisTemplate redis;

    public OfflineMessageStore(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public void store(String userId, String frame) {
        redis.opsForList().rightPush(RedisKeys.OFFLINE_PREFIX + userId, frame);
    }

    /**
     * 取走最多 {@code max} 条离线帧。
     * 用 LPOP 批量语义（LRANGE + LTRIM）而不是全取后 DEL，避免拉取期间的并发写入被误删。
     */
    public List<String> drain(String userId, int max) {
        String key = RedisKeys.OFFLINE_PREFIX + userId;
        List<String> frames = redis.opsForList().range(key, 0, max - 1L);
        if (frames == null || frames.isEmpty()) {
            return List.of();
        }
        redis.opsForList().trim(key, frames.size(), -1);
        return frames;
    }

    public long size(String userId) {
        Long size = redis.opsForList().size(RedisKeys.OFFLINE_PREFIX + userId);
        return size == null ? 0L : size;
    }
}
