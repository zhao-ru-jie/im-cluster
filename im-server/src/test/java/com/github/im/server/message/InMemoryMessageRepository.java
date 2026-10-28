package com.github.im.server.message;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 测试用的内存版消息存储：只保证 {@link MessageRepository} 的<b>语义</b>，不模拟任何介质。
 *
 * <p>为什么需要它：Redis 实现已搬到 im-storage-redis，而 im-server 的测试<b>不能</b>反向依赖那个模块
 * （im-storage-redis → im-server 是编译依赖，再加一条 test 依赖就构成 reactor 环，Maven 直接报
 * cyclic reference）。于是 im-server 侧的用例自带一个最简实现，需要验证介质行为时去
 * im-storage-redis 自己的测试里测。
 *
 * <p>按 {@code seq} 排序（与 jdbc 形态的 {@code ORDER BY seq} 一致），而不是 Redis 形态的
 * 「append 顺序」——测试断言的是业务语义，不该被落库顺序影响。
 */
final class InMemoryMessageRepository implements MessageRepository {

    private final Map<String, NavigableMap<Long, StoredMessage>> messages = new ConcurrentHashMap<>();
    private final Map<String, Set<Long>> recalled = new ConcurrentHashMap<>();
    private final Map<String, Set<Long>> deleted = new ConcurrentHashMap<>();

    @Override
    public void append(StoredMessage message) {
        messages.computeIfAbsent(message.conversationId(), key -> new ConcurrentSkipListMap<>())
                .put(message.seq(), message);
    }

    @Override
    public List<StoredMessage> recent(String conversationId, int limit) {
        NavigableMap<Long, StoredMessage> all = messages.get(conversationId);
        if (all == null || all.isEmpty() || limit <= 0) {
            return List.of();
        }
        List<StoredMessage> ordered = new ArrayList<>(all.values());
        return new ArrayList<>(ordered.subList(Math.max(0, ordered.size() - limit), ordered.size()));
    }

    @Override
    public List<StoredMessage> since(String conversationId, long afterSeq, int limit) {
        NavigableMap<Long, StoredMessage> all = messages.get(conversationId);
        if (all == null || all.isEmpty() || limit <= 0) {
            return List.of();
        }
        List<StoredMessage> matched = new ArrayList<>();
        for (StoredMessage message : all.tailMap(afterSeq, false).values()) {
            if (matched.size() >= limit) {
                break;
            }
            matched.add(message);
        }
        return matched;
    }

    @Override
    public long maxSeq(String conversationId) {
        NavigableMap<Long, StoredMessage> all = messages.get(conversationId);
        return all == null || all.isEmpty() ? 0L : all.lastKey();
    }

    @Override
    public Optional<StoredMessage> find(String conversationId, long seq) {
        NavigableMap<Long, StoredMessage> all = messages.get(conversationId);
        return all == null ? Optional.empty() : Optional.ofNullable(all.get(seq));
    }

    @Override
    public boolean recall(String conversationId, long seq) {
        if (seq <= 0) {
            return false;
        }
        return recalled.computeIfAbsent(conversationId, key -> ConcurrentHashMap.newKeySet()).add(seq);
    }

    @Override
    public Set<Long> recalledSeqs(String conversationId, java.util.Collection<Long> seqs) {
        Set<Long> marked = recalled.getOrDefault(conversationId, Set.of());
        Set<Long> matched = new java.util.LinkedHashSet<>();
        for (Long seq : seqs) {
            if (marked.contains(seq)) {
                matched.add(seq);
            }
        }
        return matched;
    }

    @Override
    public void markDeleted(String userId, String conversationId, long seq) {
        if (seq <= 0 || userId == null || userId.isBlank()) {
            return;
        }
        deleted.computeIfAbsent(userId + '|' + conversationId, key -> ConcurrentHashMap.newKeySet()).add(seq);
    }

    @Override
    public Set<Long> deletedSeqs(String userId, String conversationId) {
        if (userId == null || userId.isBlank()) {
            return Set.of();
        }
        return Set.copyOf(deleted.getOrDefault(userId + '|' + conversationId, Set.of()));
    }
}
