package com.github.im.server.message;

import com.github.im.contract.MessageEnvelope;
import com.github.im.contract.PushPort;
import com.github.im.proto.push.v1.PushTask;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 消息状态机的「回执半程」：把对端的 delivered / read 推回给<b>发送方</b>。
 *
 * <h2>补的是哪一环</h2>
 *
 * <p>在此之前，一条消息的可见状态只有 {@code msg.ack}——那是「服务端受理了」，
 * 与「对方收到没有」无关。而 delivered / read 其实一直在写
 * （{@code im:wm:{userId}:{deviceId}} 与 {@code im:user:read:{userId}}），
 * 只是<b>只有接收方自己知道</b>：发送方的气泡永远停在「已发送」。
 *
 * <p>这里把两件事接起来：状态推进之后，向会话对端推一条 {@code msg.state} 帧。
 *
 * <h2>为什么只推单聊</h2>
 *
 * <p>群聊的「已读」是一个集合（谁读到第几号），对每个成员都要向前缀内的每个发送者分发，
 * 扇出是「成员数 × 消息数」。要做得对，得先有群已读的聚合视图（谁已读/未读人数），
 * 那是另一个模型，塞进这个单点回执里只会得到一条没人能解释清楚的帧。所以群里显式不发。
 *
 * <h2>三个刻意的选择</h2>
 *
 * <ul>
 *   <li><b>只前进</b>：进程内记「这个会话的这种状态推到了哪个 seq」，重复上报
 *       （{@code sync} 与增量 {@code msg.recv} 常常重叠、多端各报一次）不再重复推。
 *       没有这一层，一条消息在两台设备上确认就是两条回执。</li>
 *   <li><b>异步 + 失败静默</b>：回执是纯展示信息，丢了不影响任何正确性——下一条消息的
 *       状态会带更大的 seq 把对端的显示纠正过来。为它阻塞收消息的主流程、或让客户端写重试
 *       逻辑，都是拿主链路换装饰品。</li>
 *   <li><b>不查任何存储</b>：只依赖调用方传进来的 seq，因此不会在 ACK 热路径上多一次往返。</li>
 * </ul>
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class MessageStateNotifier {

    private static final Logger log = LoggerFactory.getLogger(MessageStateNotifier.class);

    /** 回执本身有「下一条会覆盖」的自愈能力，TTL 不必长 */
    private static final int TTL_MS = 30_000;

    /** 去重表上限：超了就整体丢掉重来，宁可多推几条，也不让一张 map 无限长 */
    private static final int MAX_TRACKED = 200_000;

    private final ClientFrames frames;
    private final ObjectProvider<PushPort> pushPortProvider;
    private final boolean enabled;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    /** {@code state|conversationId} → 已推过的最大 seq */
    private final Map<String, Long> pushed = new ConcurrentHashMap<>();

    public MessageStateNotifier(ClientFrames frames,
                                ObjectProvider<PushPort> pushPortProvider,
                                @Value("${im.state-receipt.enabled:true}") boolean enabled) {
        this.frames = frames;
        this.pushPortProvider = pushPortProvider;
        this.enabled = enabled;
        log.info("[state] 消息状态回执（msg.state）{}", enabled ? "已开启" : "已关闭");
    }

    /** 对端设备确认收到 seq（delivered） */
    public void delivered(String conversationId, String convType, String actorUserId, long seq) {
        notifyState(conversationId, convType, actorUserId, ClientFrames.STATE_DELIVERED, seq);
    }

    /** 对端读到 seq（read） */
    public void read(String conversationId, String convType, String actorUserId, long seq) {
        notifyState(conversationId, convType, actorUserId, ClientFrames.STATE_READ, seq);
    }

    /**
     * 推一条状态回执。
     *
     * @param actorUserId 状态主体：谁的设备确认/谁读了（不是接收方）
     * @param seq        该主体确认到的最大 seq
     */
    public void notifyState(String conversationId, String convType, String actorUserId,
                            String state, long seq) {
        if (!enabled || seq <= 0 || conversationId == null || actorUserId == null) {
            return;
        }
        String peer = peerOf(conversationId, actorUserId);
        if (peer == null) {
            // 群聊（g:xxx）或多方会话：见类注释，暂不发
            return;
        }
        if (!advanced(state, conversationId, seq)) {
            return;
        }
        PushPort pushPort = pushPortProvider == null ? null : pushPortProvider.getIfAvailable();
        if (pushPort == null) {
            return;
        }
        String frame = frames.stateFrame(conversationId, convType, state, seq, actorUserId,
                System.currentTimeMillis());
        String traceId = "state-" + state + "-" + conversationId + "-" + seq;
        executor.execute(() -> {
            try {
                pushPort.push(PushTask.newBuilder()
                        .setTraceId(traceId)
                        .addTargetUserIds(peer)
                        .setFrame(frame)
                        .setTtlMs(TTL_MS)
                        .setConversationId(conversationId)
                        .build());
            } catch (RuntimeException e) {
                log.debug("[state] 状态回执推送失败（下一条会带更大的 seq 自愈）conv={} peer={} err={}",
                        conversationId, peer, e.getMessage());
            }
        });
    }

    /**
     * 单聊会话 ID 是 {@code s:{a}:{b}}（两个 userId 排序后拼接），从中取出对端。
     *
     * <p>解析逻辑已收敛到契约层（{@link MessageEnvelope#peerOf}）：输入状态、回执、
     * 会话列表都要做同一件事，各写一份就会在某次协议微调后只有一半的地方生效。
     *
     * @return 对端 userId；群聊 / 格式不合法 / actor 不属于这个会话时返回 {@code null}
     */
    static String peerOf(String conversationId, String actorUserId) {
        return MessageEnvelope.peerOf(conversationId, actorUserId);
    }

    /** @return true = 这是该会话该状态的新高，值得推一条 */
    private boolean advanced(String state, String conversationId, long seq) {
        if (pushed.size() > MAX_TRACKED) {
            pushed.clear();
        }
        String cursor = state + '|' + conversationId;
        boolean[] moved = {false};
        pushed.compute(cursor, (key, previous) -> {
            if (previous != null && previous >= seq) {
                return previous;
            }
            moved[0] = true;
            return seq;
        });
        return moved[0];
    }

    /** 已推过的会话数（观测/测试用） */
    public int tracked() {
        return pushed.size();
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }
}
