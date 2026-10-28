package com.github.im.client;

import java.util.List;

/**
 * 一页历史消息（{@code GET /conv/{id}/messages} 的结果）。
 *
 * <p>翻页方式：第一次不带 {@code beforeSeq}（或传 0），之后每次把
 * {@link #nextBeforeSeq()} 原样回传，直到 {@link #hasMore()} 为 false。
 *
 * <pre>{@code
 * HistoryPage page = client.http().history("s:alice:bob", 0, 50);
 * while (page.hasMore()) {
 *     page = client.http().history("s:alice:bob", page.nextBeforeSeq(), 50);
 * }
 * }</pre>
 *
 * <p>为什么游标是 seq 而不是 offset：seq 在会话内单调、只追加，{@code seq < 游标} 的切片
 * 不会因为「翻页期间来了新消息」而错位；offset 会整体偏移，表现为上滑时看到重复或漏掉的消息。
 *
 * @param messages     本页消息（按 seq 升序，与实时 {@code msg.push} 同构；撤回的条目已脱敏）
 * @param nextBeforeSeq 下一页游标（取 {@code seq < 它}）；{@code 0} 表示没有更早的消息了
 * @param hasMore      服务端存储里是否还有更早的消息（false 时不必再请求）
 * @param limit        服务端实际生效的单页条数（客户端传 0 或超过上限时被夹取）
 * @author zhaorujie
 * @since 1.0.0
 */
public record HistoryPage(List<DownlinkFrame.Push> messages, long nextBeforeSeq, boolean hasMore, int limit) {

    public HistoryPage {
        messages = messages == null ? List.of() : List.copyOf(messages);
    }
}
