package com.github.im.gateway.session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 本网关实例上的「userId → 会话」映射。
 *
 * <p>这是长连接服务的核心状态，也是<b>唯一不外部化</b>的状态：连接本身就绑在进程上，
 * 无法跨实例共享。所以网关的扩缩容必须配合「优雅摘流 + 客户端重连」，
 * 而不能像无状态服务那样随意杀掉（见 {@code GatewayDrainService}）。
 *
 * <p>职责边界：只做本机会话的登记与推送；不做「用户是否在线」的全局判定
 * （那是 Redis 在线路由表的职责，见 {@link GatewayRegistry}），也不做空闲判定
 * （由 Netty 的 {@code IdleStateHandler} 按连接计时）。
 *
 * <p>会话类型是 {@link ClientSession}（传输无关），因此 WebSocket 与原生 TCP 的连接
 * 在这张表里是等价的——同一用户先 WS 后 TCP 重连时，旧连接会被踢掉，行为一致。
 *
 * <p><b>单用户单连接</b>：一个 userId 只保留最后建立的那条连接（多设备支持需要把
 * key 换成 userId+deviceId，届时 {@link #send} 的语义也要改成「推给所有设备」）。
 */
@Component
public class ConnectionManager {

    private static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);

    /**
     * userId → 会话。
     *
     * <p><b>实现约定</b>：{@link ClientSession} 的实现类不得覆写 {@code equals/hashCode}——
     * {@link #remove(String, ClientSession)} 依赖 {@code ConcurrentHashMap.remove(key, value)}
     * 的值相等判断（引用相等）来保证「只摘自己那条会话」。
     */
    private final ConcurrentHashMap<String, ClientSession> sessions = new ConcurrentHashMap<>();

    public void add(String userId, ClientSession session) {
        ClientSession previous = sessions.put(userId, session);
        if (previous != null && previous != session && previous.isOpen()) {
            // 同一用户在新连接上重连：踢掉旧会话，避免消息推到僵尸连接
            log.debug("kick stale session of {}: {} -> {}", userId, previous, session);
            previous.close("replaced-by-new-connection");
        }
    }

    /**
     * 移除会话：<b>只在「当前映射仍是这条会话」时才移除</b>。
     *
     * <p>这里刻意不提供 {@code remove(userId)} 的简化重载：客户端快速重连时，新会话已经登记进来，
     * 而旧会话的关闭回调可能稍后才执行；无条件删除 userId 会把<b>新会话</b>误删，
     * 结果是「客户端明明连着，消息却被判为离线」。改回按 userId 删除前请先想清楚这一点。
     */
    public void remove(String userId, ClientSession session) {
        // 两参 remove 本身就是「仅当当前映射仍是这个值时才移除」的原子操作，
        // 不需要先 get 再判断（那是两次读、也不是原子的）。
        // 关键语义：客户端快速重连时新会话已登记，旧会话的关闭回调不能把它删掉。
        sessions.remove(userId, session);
    }

    public boolean isLocal(String userId) {
        return sessions.containsKey(userId);
    }

    public List<String> userIds() {
        return new ArrayList<>(sessions.keySet());
    }

    /** 当前实例承载的连接数，供连接数 HPA 指标使用 */
    public int connectionCount() {
        return sessions.size();
    }

    /** 按传输类型统计（websocket / tcp），用于观测双通道的实际占比 */
    public int connectionCount(String transport) {
        int count = 0;
        for (ClientSession session : sessions.values()) {
            if (transport.equals(session.transport())) {
                count++;
            }
        }
        return count;
    }

    public Map<String, ClientSession> snapshot() {
        // Map.copyOf 做一次浅拷贝快照：调用方（摘流/巡检）遍历时不受并发写入影响
        return Map.copyOf(sessions);
    }

    /**
     * 把已序列化的下行帧推给本地连接的指定用户。
     *
     * @return 用户在线且推送成功返回 true，否则 false（由调用方转离线）
     */
    public boolean send(String userId, String payload) {
        ClientSession session = sessions.get(userId);
        return session != null && sendTo(userId, session, payload);
    }

    /** 向指定会话发送（摘流/踢下线时按会话发，不看 userId 当前映射） */
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
        log.debug("closing session of {}: reason={}", userId, reason);
        session.close(reason);
    }
}
