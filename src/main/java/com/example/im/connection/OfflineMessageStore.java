package com.example.im.connection;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 离线消息存储：目标用户不在线时，消息进入 Redis 列表 im:offline:{userId}；
 * 用户重新连上后由 ConnectionManager 补发。
 */
@Component
public class OfflineMessageStore {

    private static final String KEY = "im:offline:";

    private final StringRedisTemplate redis;

    public OfflineMessageStore(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public void store(String userId, String payload) {
        redis.opsForList().rightPush(KEY + userId, payload);
    }

    /** 取出并清空某用户的所有离线消息 */
    public List<String> drain(String userId) {
        List<String> messages = redis.opsForList().range(KEY + userId, 0, -1);
        if (messages != null && !messages.isEmpty()) {
            redis.delete(KEY + userId);
        }
        return messages == null ? List.of() : messages;
    }
}
