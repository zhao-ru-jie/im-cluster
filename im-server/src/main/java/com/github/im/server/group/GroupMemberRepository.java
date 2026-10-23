package com.github.im.server.group;

import java.util.Collection;
import java.util.List;

/**
 * 群成员存储端口（IM 服务独占）。
 *
 * <p>与 {@code MessageRepository} 同一套切换策略：默认 Redis（{@code im:group:members:{groupId}}，
 * 零外部依赖），{@code im.storage=jdbc} 时切到 MySQL 的 {@code im_group_member} 表。
 * 业务侧（群聊扇出）只依赖 {@link #membersOf}，不感知存储介质。
 *
 * <p><b>为什么不缓存</b>：群成员是扇出的权威来源，缓存与库不一致会直接表现为「有人收不到消息」。
 * 需要降延迟时先按群维度加短 TTL 缓存 + 成员变更时主动失效，而不是把缓存当唯一来源。
 */
public interface GroupMemberRepository {

    /** 批量加成员（幂等：已在群内的成员重复添加不报错） */
    void addMembers(String groupId, Collection<String> userIds);

    /** 移出单个成员（不在群内时静默成功） */
    void removeMember(String groupId, String userId);

    /**
     * 群成员 userId 列表。
     *
     * <p>空列表表示「群不存在或没有成员」——扇出方必须当成「无人可投」处理，
     * 不允许退化成「投给群 ID」，否则会往离线队列塞进永远不会被拉取的数据。
     */
    List<String> membersOf(String groupId);
}
