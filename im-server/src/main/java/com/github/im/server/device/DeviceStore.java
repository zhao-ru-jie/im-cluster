package com.github.im.server.device;

import com.github.im.contract.RedisKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 设备台账的存储层。
 *
 * <h2>两张表，生命周期不同</h2>
 *
 * <ul>
 *   <li><b>设备槽位</b>（{@link RedisKeys#userDevices}，Set，网关写）：只回答「这个账号注册过哪些设备」，
 *       用于限制同账号登录台数。断连后<b>故意保留</b> 30 天——同一台手机第二天再来，还是同一台设备，
 *       不该占新槽位；</li>
 *   <li><b>设备台账</b>（{@link RedisKeys#userDeviceState}，Hash，本类写）：回答「每台设备最后一次
 *       从哪登入、什么传输、现在还在不在线」。{@code login} 时写全，{@code logout} 时只把 online 位
 *       置 0、保留其它字段（「上次用网页登录」这种信息对用户有用，断连后不该消失）。</li>
 * </ul>
 *
 * <p>合并它们就能得到一份完整的设备列表：槽位里有、台账里没有 = 注册过但从未在本版本登录过
 * （老数据），列表里以「离线 + 无登录信息」呈现，而不是凭空消失。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class DeviceStore {

    private static final Logger log = LoggerFactory.getLogger(DeviceStore.class);

    /** 台账与槽位同寿命：用户长期不回来，这些行自然会过期，不需要清理任务 */
    private final Duration ttl;

    private final StringRedisTemplate redis;

    public DeviceStore(StringRedisTemplate redis,
                       @Value("${im.device.ttl:30d}") Duration ttl) {
        this.redis = redis;
        this.ttl = ttl;
    }

    /**
     * 一条设备记录。
     *
     * @param deviceId  设备标识（老客户端没带 deviceId 时为 {@code default}）
     * @param transport websocket / tcp；台账里没有时为 null
     * @param gateway   最后登录的网关实例；没有时为 null
     * @param loginAt   最后登录时间（毫秒）；0 表示没有登录信息
     * @param online    当前是否仍在线（以台账的 online 位为准）
     */
    public record DeviceRow(String deviceId, String transport, String gateway,
                            long loginAt, boolean online) {

        static DeviceRow unknown(String deviceId) {
            return new DeviceRow(deviceId, null, null, 0L, false);
        }

        /** 存储格式：{@code transport|gateway|loginAt|online}。字段里不会出现竖线（都是受限字符集） */
        String encode() {
            return (transport == null ? "" : transport) + '|'
                    + (gateway == null ? "" : gateway) + '|'
                    + loginAt + '|'
                    + (online ? 1 : 0);
        }

        static DeviceRow decode(String deviceId, String raw) {
            if (raw == null || raw.isBlank()) {
                return unknown(deviceId);
            }
            String[] parts = raw.split("\\|", -1);
            if (parts.length < 4) {
                // 格式不认识（旧版本写入 / 手工改过）：当作没有登录信息，不要让整份列表报错
                return unknown(deviceId);
            }
            long loginAt;
            try {
                loginAt = Long.parseLong(parts[2]);
            } catch (NumberFormatException e) {
                loginAt = 0L;
            }
            return new DeviceRow(deviceId,
                    parts[0].isBlank() ? null : parts[0],
                    parts[1].isBlank() ? null : parts[1],
                    loginAt,
                    "1".equals(parts[3]));
        }
    }

    /** 记一次登录：写全字段并把 online 置 1 */
    public void recordLogin(String userId, String deviceId, String transport,
                            String gateway, long loginAt) {
        if (blank(userId) || blank(deviceId)) {
            return;
        }
        DeviceRow row = new DeviceRow(deviceId, transport, gateway,
                loginAt > 0 ? loginAt : System.currentTimeMillis(), true);
        write(userId, row);
    }

    /**
     * 记一次下线：<b>保留</b>传输/网关/登录时间，只把 online 置 0。
     *
     * <p>不能直接 HDEL：设备列表里「上次用网页登录过」这个信息恰恰是断连后才有价值
     * （在线的设备不需要「上次」）。删掉字段会让设备从列表里彻底消失，而它仍然占着手机号下的
     * 一个设备槽位，用户就再也无法在这里把它移除。
     */
    public void recordLogout(String userId, String deviceId) {
        if (blank(userId) || blank(deviceId)) {
            return;
        }
        try {
            String key = RedisKeys.userDeviceState(userId);
            String raw = redis.opsForHash().get(key, deviceId) instanceof String value ? value : null;
            DeviceRow previous = DeviceRow.decode(deviceId, raw);
            if (previous.loginAt() == 0L && previous.transport() == null) {
                // 台账里本来就没有这台（例如服务重启后先收到 logout）：不凭空造一条空记录
                return;
            }
            write(userId, new DeviceRow(previous.deviceId(), previous.transport(),
                    previous.gateway(), previous.loginAt(), false));
        } catch (RuntimeException e) {
            log.debug("[device] 下线台账写入失败 user={} device={} err={}", userId, deviceId, e.getMessage());
        }
    }

    /** 把指定设备标记为离线（踢下线后调用：连接是被服务端关的，网关的上报可能还没到） */
    public void markOffline(String userId, List<String> deviceIds) {
        if (blank(userId) || deviceIds == null || deviceIds.isEmpty()) {
            return;
        }
        try {
            String key = RedisKeys.userDeviceState(userId);
            Map<Object, Object> raw = redis.opsForHash().entries(key);
            for (String deviceId : deviceIds) {
                DeviceRow previous = DeviceRow.decode(deviceId, (String) raw.get(deviceId));
                write(userId, new DeviceRow(deviceId, previous.transport(), previous.gateway(),
                        previous.loginAt(), false));
            }
        } catch (RuntimeException e) {
            log.debug("[device] 标记离线失败 user={} devices={} err={}", userId, deviceIds, e.getMessage());
        }
    }

    /**
     * 该用户的设备列表：槽位 ∪ 台账，按「在线优先、再按登录时间倒序」排。
     *
     * <p>排序固定为「最近登录的排前面」：用户打开设备管理就是要找「那台不认识的设备」，
     * 最新的那条最可疑，放最上面。
     */
    public List<DeviceRow> devicesOf(String userId) {
        if (blank(userId)) {
            return List.of();
        }
        Set<String> slots = new LinkedHashSet<>();
        Map<String, DeviceRow> rows = new HashMap<>();
        try {
            Set<String> members = redis.opsForSet().members(RedisKeys.userDevices(userId));
            if (members != null) {
                slots.addAll(members);
            }
            Map<Object, Object> raw = redis.opsForHash().entries(RedisKeys.userDeviceState(userId));
            for (Map.Entry<Object, Object> entry : raw.entrySet()) {
                String deviceId = String.valueOf(entry.getKey());
                slots.add(deviceId);
                rows.put(deviceId, DeviceRow.decode(deviceId, (String) entry.getValue()));
            }
        } catch (RuntimeException e) {
            // 展示型查询失败按「没有信息」处理：让设备管理页报 500 没有任何收益
            log.debug("[device] 设备列表读取失败 user={} err={}", userId, e.getMessage());
            return List.of();
        }
        List<DeviceRow> result = new ArrayList<>(slots.size());
        for (String deviceId : slots) {
            result.add(rows.getOrDefault(deviceId, DeviceRow.unknown(deviceId)));
        }
        result.sort((left, right) -> {
            if (left.online() != right.online()) {
                return left.online() ? -1 : 1;
            }
            return Long.compare(right.loginAt(), left.loginAt());
        });
        return result;
    }

    /**
     * 移除一台设备：同时清台账字段与设备槽位。
     *
     * <p>两处都删才算「移除」：只删槽位的话台账残留会让它继续出现在列表里；
     * 只删台账的话槽位仍占着名额，用户会遇到「明明删了却提示设备数已满」。
     */
    public void forget(String userId, String deviceId) {
        if (blank(userId) || blank(deviceId)) {
            return;
        }
        try {
            redis.opsForSet().remove(RedisKeys.userDevices(userId), deviceId);
            redis.opsForHash().delete(RedisKeys.userDeviceState(userId), deviceId);
        } catch (RuntimeException e) {
            log.warn("[device] 移除设备失败 user={} device={} err={}", userId, deviceId, e.getMessage());
        }
    }

    private void write(String userId, DeviceRow row) {
        String key = RedisKeys.userDeviceState(userId);
        redis.opsForHash().put(key, row.deviceId(), row.encode());
        redis.expire(key, ttl);
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }
}
