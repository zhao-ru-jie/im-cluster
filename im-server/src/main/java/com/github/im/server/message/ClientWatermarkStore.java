package com.github.im.server.message;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 客户端消费水位表：{@code conversationId -> 该会话中客户端已见过的最大 seq}。
 *
 * <h2>为什么需要它</h2>
 *
 * <p>{@link SeqAllocator} 的下界原本只有两个来源：本进程已知的最大 seq、以及
 * {@code MessageRepository.maxSeq}（已落库的最大 seq）。当**消息存储与发号器共用同一个 Redis**
 * 且整体丢失时（无持久化重启、FLUSH、回滚到旧快照），这两个来源会同时失效：下界变 0，
 * seq 从 1 重新开始。
 *
 * <p>此时库里没有冲突（消息一并没了），但会造成更隐蔽的伤害：客户端 {@code lastSeq} 已经
 * 推进到 N，服务端却重新发 1、2、3，客户端会把这些判成「已消费过的历史消息」直接丢弃，
 * 且水位不再前进——表现为**新消息永久不显示**，直到服务端 seq 追上 N。
 *
 * <p>客户端水位是这种情况下唯一还活着的证据。它由客户端经 {@code PullPending} 上报
 * （{@code sync} 帧），发号器把它并入下界，保证「绝不发出比客户端水位更小的 seq」。
 *
 * <h2>边界</h2>
 *
 * <p>本类状态是进程内的，重启即空；它只覆盖「客户端上报过」的会话，纯离线会话仍靠存储下界。
 * 也就是说它是**兜底增益**而非强一致保证——真正的强一致要求 seq 分配落在持久存储里
 * （见 {@code SeqAllocator} 类注释）。进程内保存还有一个好处：上报路径不依赖 Redis，
 * 恰恰在 Redis 不可用的场景下仍然有效。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class ClientWatermarkStore {

    /** 水位只增不减：同一会话多次上报取最大值 */
    private final Map<String, Long> watermarks = new ConcurrentHashMap<>();

    /**
     * 合并一批上报的水位（按会话取较大值）。
     *
     * @param reported 会话 ID → 客户端已见最大 seq；空 map 安全
     */
    public void observe(Map<String, Long> reported) {
        if (reported == null || reported.isEmpty()) {
            return;
        }
        reported.forEach((conversationId, seq) -> {
            if (conversationId == null || conversationId.isBlank() || seq == null || seq <= 0) {
                return;
            }
            watermarks.merge(conversationId, seq, Math::max);
        });
    }

    /** @return 该会话已知的客户端水位；{@code 0} = 从未上报过 */
    public long maxSeqOf(String conversationId) {
        return watermarks.getOrDefault(conversationId, 0L);
    }

    /** 仅用于可观测性 / 测试断言 */
    public int size() {
        return watermarks.size();
    }
}
