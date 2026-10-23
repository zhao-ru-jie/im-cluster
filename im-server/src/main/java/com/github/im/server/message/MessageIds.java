package com.github.im.server.message;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 服务端消息 ID 生成器：<b>UUIDv7</b>（RFC 9562），与 {@code seq} 完全解耦。
 *
 * <p>为什么不再用 {@code conversationId#seq}：seq 是「会话内排序游标」，它的权威状态在
 * Redis（{@code im:seq:{conversationId}}）。一旦该 key 丢失（重启无持久化、allkeys-* 淘汰、
 * 主从切换、FLUSH），seq 会从下界重新起步，而 {@code conversationId#seq} 这种
 * 「由 seq 派生 ID」的设计会让消息 ID 跟着回绕 —— 于是新旧两条不同消息拿到同一个 ID。
 *
 * <p>拆开之后两者的职责各自单纯：
 * <ul>
 *   <li>{@code serverMsgId}：只需<b>全局唯一</b>，不参与排序，回绕问题不存在；</li>
 *   <li>{@code seq}：只需<b>会话内单调</b>（见 {@link SeqAllocator}），允许有洞。</li>
 * </ul>
 *
 * <p>为什么选 UUIDv7 而不是雪花：雪花需要「机器号」这类集群范围的协调（配置下发、或随机
 * 位带碰撞风险），而 UUIDv7 把 48 位毫秒时间戳放高位、其余位随机，<b>完全本地生成、零协调</b>，
 * 又天然按时间近似有序（便于按 ID 建索引与排查）。代价是 36 字符而非 64 位整数。
 *
 * <p>单调性：同一毫秒内用 12 位 {@code rand_a} 递增计数，保证<b>单实例内</b>严格递增；
 * 时间戳回拨（NTP 校时、虚拟机漂移）时冻结时间戳继续递增计数，绝不回退。
 */
public final class MessageIds {

    /** UUIDv7 的 rand_a 段宽度：12 bit。同毫秒最多容纳 4096 个 ID，超出则借用下一毫秒 */
    private static final int RAND_A_LIMIT = 1 << 12;

    private static final long VERSION_7 = 0x7L << 12;
    private static final long VARIANT_RFC = 0x8000_0000_0000_0000L;
    private static final long RAND_B_MASK = 0x3FFF_FFFF_FFFF_FFFFL;

    private static final Object LOCK = new Object();
    private static long lastTimestamp = -1L;
    private static int sequenceInMillis;

    private MessageIds() {
    }

    /** @return 形如 {@code 0192f0c4-8a3b-7c2d-9f01-4b7a2c8d5e60} 的 36 字符消息 ID */
    public static String next() {
        long timestamp;
        int sequence;
        synchronized (LOCK) {
            long now = System.currentTimeMillis();
            if (now > lastTimestamp) {
                lastTimestamp = now;
                sequenceInMillis = 0;
            } else {
                // 同一毫秒内连发，或时钟回拨：时间戳不前进，用计数继续保证唯一且有序
                sequenceInMillis++;
                if (sequenceInMillis >= RAND_A_LIMIT) {
                    lastTimestamp++;
                    sequenceInMillis = 0;
                }
            }
            timestamp = lastTimestamp;
            sequence = sequenceInMillis;
        }

        long msb = (timestamp << 16) | VERSION_7 | sequence;
        long lsb = (ThreadLocalRandom.current().nextLong() & RAND_B_MASK) | VARIANT_RFC;
        return new UUID(msb, lsb).toString();
    }
}
