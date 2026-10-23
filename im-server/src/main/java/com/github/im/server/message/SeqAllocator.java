package com.github.im.server.message;

import com.github.im.contract.RedisKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 会话内 seq 的发号器：<b>每条消息一次 Redis 原子递增</b>，全局单调。
 *
 * <h2>为什么不做「本地号段缓存」</h2>
 *
 * <p>号段方案（一次 {@code INCRBY N}，把 {@code [start, end]} 缓存在本进程内存里逐条发）
 * 能省 Redis 往返，但它有一个致命副作用：<b>同一会话的 seq 会跨实例交错</b>。
 * 两个实例各持一段，A 发 1、2、3，B 发 101、102、103，客户端看到的就是 {@code 1, 101, 2, 102}。
 *
 * <p>这不是「可接受的跳号」，而是语义被破坏：
 * <ul>
 *   <li>契约里 seq 是「会话内单调递增（保序与拉齐依据）」，交错后既不单调也不等于受理顺序；</li>
 *   <li>客户端按 {@code lastSeq} 补洞的常规实现是「{@code seq <= lastSeq} 视为旧消息丢弃」，
 *       于是 B 发了 101 之后，A 的 2、3 会被当成旧消息<b>永久丢掉</b>；</li>
 *   <li>离线补发是 FIFO（按到达），与 seq 顺序互相打架，水位失去意义。</li>
 * </ul>
 *
 * <p>而逐条 {@code INCR} 的代价只是一次 Redis 往返（本机 0.2~0.5ms，相对上行 300ms 的
 * deadline 可忽略）：Redis 单线程串行执行，返回值就是「所有实例共同认可的会话内顺序」。
 *
 * <h2>三道防回绕（与号段方案保持同等强度）</h2>
 *
 * <p>1. <b>下界保护</b>：稳态下每次取号都把「已落库最大 seq」作为下界传给 Lua，计数器低于下界时先抬到
 * 下界再递增。为摊薄 DB 查询，每 {@value #FLOOR_REFRESH_EVERY} 条消息才真正查一次库，
 * 期间用本进程已知的最大 seq 兜底——和原来「每 100 条取一次号段」的查询频率一致。
 *
 * <p>2. <b>冷 key 初始化</b>：{@code im:seq:{conv}} 不存在（会话首次发号 / Redis 被清空）时，
 * 用「已落库最大 seq + 客户端水位 + 本地已知」三者取大做起点，避免从 1 重来撞唯一索引。
 *
 * <p>3. <b>客户端水位</b>（{@link ClientWatermarkStore}）：消息存储与发号器共用同一个 Redis
 * 且整体丢失时，前两道下界会同时归零，只有客户端手里的 {@code lastSeq} 还能证明
 * 「这些 seq 已被消费过」。没有这一道，新消息会被客户端当历史去重丢掉。
 *
 * <p>为什么用 Lua 而不是「先 GET 再 INCR」：两者之间会被其它实例插入，导致写回旧值；
 * Lua 在 Redis 内串行执行，天然原子。
 */
@Component
public class SeqAllocator {

    private static final Logger log = LoggerFactory.getLogger(SeqAllocator.class);

    /** 每多少条消息重新查一次「已落库最大 seq」来校准下界（摊薄成 1/100 次 DB 查询） */
    private static final int FLOOR_REFRESH_EVERY = 100;

    /**
     * 稳态取号。
     *
     * <p>{@code KEYS[1]} = {@code im:seq:{conversationId}}，{@code ARGV[1]} = 下界 floor。
     * <b>key 不存在时返回 -1</b>：让调用方走冷启动路径去算真正的下界（那需要查库），
     * 从而保证稳态下每条消息只有一次 Redis 调用。
     */
    private static final DefaultRedisScript<Long> NEXT_SEQ = new DefaultRedisScript<>("""
            local cur = redis.call('GET', KEYS[1])
            if cur == false then
                return -1
            end
            local floor = tonumber(ARGV[1])
            if floor > 0 and tonumber(cur) < floor then
                redis.call('SET', KEYS[1], floor)
            end
            return redis.call('INCR', KEYS[1])
            """, Long.class);

    /** 冷启动：先把计数器抬到 floor，再取第一个号 */
    private static final DefaultRedisScript<Long> INIT_SEQ = new DefaultRedisScript<>("""
            local cur = redis.call('GET', KEYS[1])
            local floor = tonumber(ARGV[1])
            if cur == false or tonumber(cur) < floor then
                redis.call('SET', KEYS[1], floor)
            end
            return redis.call('INCR', KEYS[1])
            """, Long.class);

    private final StringRedisTemplate redis;
    private final MessageRepository messageRepository;
    private final ClientWatermarkStore watermarkStore;

    /** conversationId -> 本进程的已知下界与取号计数（不含任何「预留号」，发号本身始终走 Redis） */
    private final Map<String, ConversationSeq> conversations = new ConcurrentHashMap<>();

    public SeqAllocator(StringRedisTemplate redis,
                        MessageRepository messageRepository,
                        ClientWatermarkStore watermarkStore) {
        this.redis = redis;
        this.messageRepository = messageRepository;
        this.watermarkStore = watermarkStore;
    }

    /**
     * 取下一条会话内序号：全局唯一、会话内单调递增，顺序 = Redis 串行化顺序。
     *
     * @param conversationId 会话 ID（seq 的作用域）
     */
    public long next(String conversationId) {
        ConversationSeq state = conversations.computeIfAbsent(conversationId, _ -> new ConversationSeq());
        if (state.needsFloorRefresh()) {
            state.raiseFloor(storageFloor(conversationId));
        }
        String key = RedisKeys.SEQ_PREFIX + conversationId;
        Long seq = redis.execute(NEXT_SEQ, List.of(key), String.valueOf(state.floor()));
        if (seq != null && seq > 0) {
            state.raiseFloor(seq);
            return seq;
        }
        return coldStart(conversationId, state, key);
    }

    /** 计数器不存在：用「已落库最大 seq + 客户端水位 + 本地已知」三者取大作为起点 */
    private long coldStart(String conversationId, ConversationSeq state, String key) {
        long floor = Math.max(state.floor(), watermarkStore.maxSeqOf(conversationId));
        floor = Math.max(floor, storageFloor(conversationId));
        Long seq = redis.execute(INIT_SEQ, List.of(key), String.valueOf(floor));
        if (seq == null || seq <= 0) {
            // Redis 异常的极端情况：不让消息无声失败，退回本地推算值（真撞号由唯一索引兜住）
            log.error("[seq] 会话 {} 取号异常，退回本地推算值 {}", conversationId, floor + 1);
            seq = floor + 1;
        }
        state.raiseFloor(seq);
        log.info("[seq] 会话 {} 计数器初始化：初始 seq={}，下界={}", conversationId, seq, floor);
        return seq;
    }

    /** 已落库的最大 seq；读库失败不阻断发号（Redis 计数器通常已经是权威值） */
    private long storageFloor(String conversationId) {
        try {
            return messageRepository.maxSeq(conversationId);
        } catch (RuntimeException e) {
            log.warn("[seq] 读取会话 {} 的已落库最大 seq 失败，本次下界仅用本地记录: {}",
                    conversationId, e.getMessage());
            return 0L;
        }
    }

    /** 单会话的本地状态：只有「下界」和「计数」，不缓存任何未发的号 */
    private static final class ConversationSeq {

        /** 本进程已知的最大 seq（也用作 Lua 的下界参数，防 Redis 回退到旧快照） */
        private final AtomicLong floor = new AtomicLong();
        /** 取号次数，用于把 DB 下界校准摊薄到 1/{@value FLOOR_REFRESH_EVERY} */
        private final AtomicLong issued = new AtomicLong();

        boolean needsFloorRefresh() {
            return issued.incrementAndGet() % FLOOR_REFRESH_EVERY == 0;
        }

        long floor() {
            return floor.get();
        }

        void raiseFloor(long value) {
            if (value > 0) {
                floor.accumulateAndGet(value, Math::max);
            }
        }
    }
}
