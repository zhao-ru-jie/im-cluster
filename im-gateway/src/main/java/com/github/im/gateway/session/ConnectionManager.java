package com.github.im.gateway.session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 本网关实例上的「userId → 设备 → 会话」映射。
 *
 * <p>这是长连接服务的核心状态，也是<b>唯一不外部化</b>的状态：连接本身就绑在进程上，
 * 无法跨实例共享。所以网关的扩缩容必须配合「优雅摘流 + 客户端重连」，
 * 而不能像无状态服务那样随意杀掉（见 {@code GatewayDrainService}）。
 *
 * <p>职责边界：只做本机会话的登记与推送；不做「用户是否在线」的全局判定
 * （那是 Redis 在线路由表的职责，见 {@link GatewayRegistry}），也不做空闲判定
 * （由 Netty 的 {@code IdleStateHandler} 按连接计时）。
 *
 * <h2>多端语义</h2>
 * <p>一个 userId 下可以有多个设备并存（手机 + 网页 + 桌面），设备身份取
 * {@link ClientSession#deviceId()}：
 * <ul>
 *   <li>同一个 deviceId 再次建连 = 该设备重连 → <b>踢掉旧会话</b>（避免消息推到僵尸连接）；</li>
 *   <li>不同 deviceId → 并存，{@link #send} 向该用户<b>所有</b>设备各推一份；</li>
 *   <li>没带 deviceId 的客户端统一落到 {@code default} 槽位，行为与旧版「单用户单连接」一致
 *       —— 想多端就必须带 deviceId，这条约定写进了 README。</li>
 * </ul>
 * 每个用户的设备数有上限（{@code im.gateway.max-devices-per-user}）：超限时<b>拒绝新连接</b>
 * 而不是踢掉老设备，避免「一个客户端 bug 反复重连把用户其它设备挤下线」。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class ConnectionManager {

    private static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);

    /** 没带 deviceId 的客户端共用这个槽位：等价于旧版的单连接语义 */
    private static final String DEFAULT_DEVICE = "default";

    /**
     * userId → (deviceKey → 会话)。
     *
     * <p><b>实现约定</b>：{@link ClientSession} 的实现类不得覆写 {@code equals/hashCode}——
     * {@link #remove} 依赖 {@code ConcurrentHashMap.remove(key, value)} 的值相等判断（引用相等）
     * 来保证「只摘自己那条会话」。
     *
     * <p>内层 map 永不留空壳：最后一个设备移除时连同外层条目一起删掉，
     * 否则「连接过的用户」会永久占着一个 map（长跑后就是内存泄漏）。
     */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, ClientSession>> devices =
            new ConcurrentHashMap<>();

    private final int maxDevicesPerUser;

    public ConnectionManager(@Value("${im.gateway.max-devices-per-user:5}") int maxDevicesPerUser) {
        // 至少 1：配成 0 会让任何人都连不上，属于配置事故，直接兜住
        this.maxDevicesPerUser = Math.max(maxDevicesPerUser, 1);
    }

    /**
     * 登记一条已认证的连接。
     *
     * @return {@code false} = 该用户设备数已达上限，<b>调用方必须关闭这条连接</b>（不要登记路由）
     */
    public boolean add(String userId, ClientSession session) {
        String deviceKey = deviceKeyOf(session);
        for (; ; ) {
            ConcurrentHashMap<String, ClientSession> byDevice =
                    devices.computeIfAbsent(userId, _ -> new ConcurrentHashMap<>(2));
            synchronized (byDevice) {
                // 拿到锁之后复核：并发 remove 可能刚把这个空壳从外层摘掉了，
                // 此时往里写会写进一个没人能看到的孤儿 map（表现为「明明连着却被判离线」）
                if (devices.get(userId) != byDevice) {
                    continue;
                }
                if (!byDevice.containsKey(deviceKey) && byDevice.size() >= maxDevicesPerUser) {
                    log.warn("user {} 已达设备上限 {}，拒绝新连接（device={}）",
                            userId, maxDevicesPerUser, deviceKey);
                    if (byDevice.isEmpty()) {
                        devices.remove(userId, byDevice);
                    }
                    return false;
                }
                ClientSession previous = byDevice.put(deviceKey, session);
                if (previous != null && previous != session && previous.isOpen()) {
                    // 同一设备重连（换传输、断网重连）：踢掉旧会话，避免消息推到僵尸连接
                    log.debug("kick stale session of {} on device {}: {} -> {}",
                            userId, deviceKey, previous, session);
                    previous.close("replaced-by-new-connection");
                }
                return true;
            }
        }
    }

    /**
     * 移除一条会话：<b>只在「该设备槽位仍是这条会话」时才移除</b>。
     *
     * <p>这里刻意不提供 {@code remove(userId)} 的简化重载：客户端快速重连时，新会话已经登记进来，
     * 而旧会话的关闭回调可能稍后才执行；无条件删除会把<b>新会话</b>误删，
     * 结果是「客户端明明连着，消息却被判为离线」。改回按 userId 删除前请先想清楚这一点。
     */
    public void remove(String userId, ClientSession session) {
        ConcurrentHashMap<String, ClientSession> byDevice = devices.get(userId);
        if (byDevice == null) {
            return;
        }
        synchronized (byDevice) {
            // 两参 remove 是「仅当该设备槽位仍是这个值时才移除」的原子操作
            byDevice.remove(deviceKeyOf(session), session);
            if (byDevice.isEmpty()) {
                // 同一把锁内删除，保证不会出现「刚判空、又被 add 写进来」的孤儿 map
                devices.remove(userId, byDevice);
            }
        }
    }

    /** 本实例上是否还有该用户的任意设备在线 */
    public boolean isLocal(String userId) {
        return devices.containsKey(userId);
    }

    /** 本实例上该用户的设备数（0 = 不在线） */
    public int deviceCount(String userId) {
        ConcurrentHashMap<String, ClientSession> byDevice = devices.get(userId);
        return byDevice == null ? 0 : byDevice.size();
    }

    public List<String> userIds() {
        return new ArrayList<>(devices.keySet());
    }

    /** 当前实例承载的**连接**数（多设备时不再是用户数），供连接数 HPA 指标使用 */
    public int connectionCount() {
        int count = 0;
        for (ConcurrentHashMap<String, ClientSession> byDevice : devices.values()) {
            count += byDevice.size();
        }
        return count;
    }

    /** 按传输类型统计（websocket / tcp），用于观测双通道的实际占比 */
    public int connectionCount(String transport) {
        int count = 0;
        for (ClientSession session : sessions()) {
            if (transport.equals(session.transport())) {
                count++;
            }
        }
        return count;
    }

    /** 全部连接的快照（摘流/巡检遍历时不受并发写入影响） */
    public List<ClientSession> sessions() {
        List<ClientSession> snapshot = new ArrayList<>(connectionCount());
        for (ConcurrentHashMap<String, ClientSession> byDevice : devices.values()) {
            snapshot.addAll(byDevice.values());
        }
        return snapshot;
    }

    /**
     * 把已序列化的下行帧推给该用户在本实例上的<b>所有设备</b>。
     *
     * <p>「部分设备写失败」不影响返回值：只要有任意一台收到就算投递成功——
     * 其它设备可以在重连后按 seq 拉齐。只有全部失败（或根本不在线）才判失败，
     * 由调用方转离线队列。
     *
     * @return 至少一台设备推送成功返回 true
     */
    public boolean send(String userId, String payload) {
        ConcurrentHashMap<String, ClientSession> byDevice = devices.get(userId);
        if (byDevice == null || byDevice.isEmpty()) {
            return false;
        }
        int success = 0;
        int failed = 0;
        for (Map.Entry<String, ClientSession> entry : byDevice.entrySet()) {
            if (sendTo(userId, entry.getValue(), payload)) {
                success++;
            } else {
                failed++;
            }
        }
        if (failed > 0) {
            log.debug("send to {} partially failed: success={} failed={}", userId, success, failed);
        }
        return success > 0;
    }

    /** 向指定会话发送（摘流/踢下线时按会话发，不看该设备槽位当前映射） */
    public boolean sendTo(String userId, ClientSession session, String payload) {
        if (session == null || !session.isOpen()) {
            return false;
        }
        if (!session.send(payload)) {
            log.debug("send to {} failed on {}", userId, session);
            return false;
        }
        return true;
    }

    public void close(String userId, ClientSession session, String reason) {
        if (session == null) {
            return;
        }
        log.debug("closing session of {} (device={}): reason={}", userId, deviceKeyOf(session), reason);
        session.close(reason);
    }

    private static String deviceKeyOf(ClientSession session) {
        String deviceId = session.deviceId();
        return deviceId == null || deviceId.isBlank() ? DEFAULT_DEVICE : deviceId;
    }
}
