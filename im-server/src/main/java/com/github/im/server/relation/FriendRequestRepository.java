package com.github.im.server.relation;

import java.util.List;

/**
 * 好友申请的存储端口。
 *
 * <h2>为什么独立于 {@link RelationRepository}</h2>
 *
 * <p>两者回答的是不同问题：{@code RelationRepository} 回答「A 和 B 现在是不是好友」，
 * 是每条单聊消息都要跑一次的热路径；本端口回答「谁在等谁处理」，只在通讯录页面和
 * 审批动作上用。它们的唯一键、字段、生命周期都不一样（好友边只有存在/不存在，
 * 申请要带状态与附言），合在一起会让热路径的实体背上无关的列。
 *
 * <h2>写入语义</h2>
 *
 * <p>{@link #save} 按 {@code (fromId, toId)} 做幂等覆盖：同一对用户始终只有一条记录，
 * 重新申请就是把状态改回 {@code PENDING}。这样「同一人反复点申请」不会在对方列表里
 * 堆出一堆重复项，也不需要额外的去重逻辑。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public interface FriendRequestRepository {

    /** 覆盖写入一条申请（按 fromId + toId 唯一定位），返回落库后的形态 */
    void save(FriendRequest request);

    /** @return 不存在返回 {@code null} */
    FriendRequest find(String fromId, String toId);

    /** 我收到的申请（含已处理的历史，按 {@code updatedAt} 倒序） */
    List<FriendRequest> incoming(String userId);

    /** 我发出的申请（同上） */
    List<FriendRequest> outgoing(String userId);

    /** 删除一条申请（撤回 / 清理）；不存在时静默成功 */
    void delete(String fromId, String toId);
}
