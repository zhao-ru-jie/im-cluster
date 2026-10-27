package com.github.im.client;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;

/**
 * 纯内存实现：测试、以及「宁可丢历史也要零磁盘依赖」的场景用。
 *
 * <p>它同时也是 {@link FileLocalMessageTable} 的<b>索引层</b>：落盘实现只在
 * {@link #afterMutation} 里追加一行日志，其余逻辑（去重、水位、待发筛选）与这里完全一致。
 * 这样「内存版」不是「落盘版的简化版」，而是同一份语义的两个持久化后端——
 * 否则单测通过的语义和线上跑的就不是一套了。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class InMemoryLocalMessageTable implements LocalMessageTable {

    /** 待发/已发行：clientMsgId 是主键（幂等键） */
    private final Map<String, LocalMessage> outbound = new LinkedHashMap<>();
    /** 已收行：会话 → (seq → 行)，用 TreeMap 才能在 O(log n) 判断重复与取最大 seq */
    private final Map<String, TreeMap<Long, LocalMessage>> inbound = new LinkedHashMap<>();

    @Override
    public synchronized void upsert(LocalMessage message) {
        if (message.direction() == LocalMessage.Direction.OUT) {
            outbound.put(message.clientMsgId(), message);
        } else {
            inbound.computeIfAbsent(message.conversationId(), key -> new TreeMap<>())
                    .put(message.seq(), message);
        }
        afterMutation(message);
    }

    @Override
    public synchronized Optional<LocalMessage> find(String clientMsgId) {
        if (clientMsgId == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(outbound.get(clientMsgId));
    }

    @Override
    public synchronized List<LocalMessage> pendingDue(long nowMs, int limit) {
        List<LocalMessage> due = new ArrayList<>();
        outbound.values().stream()
                .filter(message -> message.dueAt(nowMs))
                .sorted(Comparator.comparingLong(LocalMessage::nextAttemptAt)
                        .thenComparingLong(LocalMessage::createdAt))
                .limit(Math.max(1, limit))
                .forEach(due::add);
        return due;
    }

    @Override
    public synchronized List<LocalMessage> outbound(int limit) {
        List<LocalMessage> rows = new ArrayList<>();
        outbound.values().stream()
                .sorted(Comparator.comparingLong(LocalMessage::createdAt))
                .limit(Math.max(1, limit))
                .forEach(rows::add);
        return rows;
    }

    @Override
    public synchronized boolean recordInbound(LocalMessage message) {
        TreeMap<Long, LocalMessage> seqs =
                inbound.computeIfAbsent(message.conversationId(), key -> new TreeMap<>());
        if (message.seq() <= 0) {
            return false;
        }
        // 去重口径是「这个 (会话, seq) 已经有了」，不是「seq 比高水位小就丢」：
        // 补发允许乱序（群聊按会话补、单聊有上限分页），用高水位一刀切会把
        // 「真正没收到过、只是比高水位小」的消息误删——那正是丢消息。
        if (seqs.containsKey(message.seq())) {
            return false;
        }
        seqs.put(message.seq(), message);
        afterMutation(message);
        return true;
    }

    @Override
    public synchronized OptionalLong lastInboundSeq(String conversationId) {
        TreeMap<Long, LocalMessage> seqs = inbound.get(conversationId);
        if (seqs == null || seqs.isEmpty()) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(seqs.lastKey());
    }

    @Override
    public synchronized Map<String, Long> inboundWatermarks() {
        Map<String, Long> watermarks = new LinkedHashMap<>();
        inbound.forEach((conversationId, seqs) -> {
            if (!seqs.isEmpty()) {
                watermarks.put(conversationId, seqs.lastKey());
            }
        });
        return watermarks;
    }

    @Override
    public synchronized List<LocalMessage> inbound(String conversationId, int limit) {
        TreeMap<Long, LocalMessage> seqs = inbound.get(conversationId);
        if (seqs == null || seqs.isEmpty()) {
            return List.of();
        }
        List<LocalMessage> rows = new ArrayList<>();
        // 取最近 limit 条再按 seq 升序返回：调用方要的是「最后几条」，不是「最早几条」
        seqs.descendingMap().values().stream()
                .limit(Math.max(1, limit))
                .forEach(rows::add);
        rows.sort(Comparator.comparingLong(LocalMessage::seq));
        return rows;
    }

    @Override
    public synchronized int size() {
        int total = outbound.size();
        for (TreeMap<Long, LocalMessage> seqs : inbound.values()) {
            total += seqs.size();
        }
        return total;
    }

    @Override
    public void close() {
        // 内存实现没有需要释放的资源；保留给落盘实现与上层统一 close()
    }

    /** 全表快照（落盘实现 compaction 时用来重写文件；也便于调试打印） */
    protected synchronized List<LocalMessage> snapshot() {
        List<LocalMessage> rows = new ArrayList<>(outbound.values());
        inbound.values().forEach(seqs -> rows.addAll(seqs.values()));
        return rows;
    }

    /**
     * 日志回放专用：只更新索引、<b>不触发持久化</b>。
     *
     * <p>落盘实现的构造过程正是「读日志 → 回放」，如果回放再写一遍日志，
     * 启动一次文件就翻倍。
     */
    protected synchronized void restore(LocalMessage message) {
        if (message.direction() == LocalMessage.Direction.OUT) {
            outbound.put(message.clientMsgId(), message);
        } else {
            inbound.computeIfAbsent(message.conversationId(), key -> new TreeMap<>())
                    .put(message.seq(), message);
        }
    }

    /**
     * 每次索引发生变化后的钩子（落盘实现在这里追加日志）。
     *
     * <p>调用时已持有本对象锁，实现可以直接写文件——不需要再加锁，
     * 也保证了「索引变化」与「日志追加」的先后顺序与调用顺序一致。
     */
    protected void afterMutation(LocalMessage message) {
        // 内存实现无需持久化
    }
}
