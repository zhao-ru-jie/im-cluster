package com.github.im.client;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * 本地消息表（客户端侧的「发件箱 + 收件箱 + 水位」）。
 *
 * <h2>它到底解决什么</h2>
 *
 * <ol>
 *   <li><b>不丢发出去的东西</b>：用户点发送后先写表，再上网；ack 没回来就重投，
 *       幂等键（{@code clientMsgId}）保证重投不会变成两条消息；</li>
 *   <li><b>不重复展示收到的东西</b>：离线补发是「至少一次」，同一 seq 会被推多次，
 *       落表时按 (会话, seq) 去重；</li>
 *   <li><b>重连时能自证水位</b>：{@link #inboundWatermarks()} 报给服务端，
 *       它是 Redis 整体丢失后 seq 发号器的最后一道下界。</li>
 * </ol>
 *
 * <h2>实现约束</h2>
 *
 * <p>实现必须是<b>线程安全</b>的：读循环（收包）与控制线程（发送/重投/上报）会并发调用。
 * 但不需要事务：单条消息的写入是独立的，崩溃最多丢「最后一条正在写的记录」，
 * 而它对应的场景（进程被杀）本来就需要服务端按 seq 补发。
 *
 * <p>实现也必须能<b>长期增长而不崩</b>：收件箱会一直变大（它就是聊天记录），
 * 所以列表类方法都带 {@code limit}，调用方不许「全量扫一遍」。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public interface LocalMessageTable extends AutoCloseable {

    /** 写入/覆盖一行（按 clientMsgId 或 (会话, seq) 定位） */
    void upsert(LocalMessage message);

    Optional<LocalMessage> find(String clientMsgId);

    /**
     * 到点可重投的待发消息（按 {@code nextAttemptAt} 升序）。
     *
     * @param limit 单次上限：断网很久后积压可能上万条，一次全捞出来会把内存和连接一起打满
     */
    List<LocalMessage> pendingDue(long nowMs, int limit);

    /** 待发（含未到点的）；用于「发送中/未送达」角标与调试 */
    List<LocalMessage> outbound(int limit);

    /**
     * 落一条下行消息并去重。
     *
     * <p>去重口径是<b>精确的 (会话, seq)</b>，不是「比高水位小就丢」：补发允许乱序
     * （群聊按会话补、单聊按条数分页），用高水位一刀切会把「真正没收到过、
     * 只是比高水位小」的消息误删——那正是丢消息。
     *
     * @return {@code true} 表示这条是新的；{@code false} 表示同一个 (会话, seq) 已经存在
     *         （补发重复），调用方应当静默丢弃
     */
    boolean recordInbound(LocalMessage message);

    /** 会话内已收到的最大 seq；没有记录时为空 */
    OptionalLong lastInboundSeq(String conversationId);

    /** 会话 → 已收到的最大 seq。{@code sync} 帧直接用它，因此它的正确性等于「不会丢消息」 */
    Map<String, Long> inboundWatermarks();

    /** 某会话最近的消息（按 seq 升序，最多 {@code limit} 条） */
    List<LocalMessage> inbound(String conversationId, int limit);

    /** 表内总行数（用于日志与 compaction 判定） */
    int size();

    @Override
    void close();
}
