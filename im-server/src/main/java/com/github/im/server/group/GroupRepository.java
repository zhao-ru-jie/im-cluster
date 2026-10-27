package com.github.im.server.group;

import java.util.Optional;

/**
 * 群实体存储端口（群资料 + 群级开关）。
 *
 * <p>与 {@link GroupMemberRepository} 的分工：这个回答「群是什么」（名字、群主、公告、人数上限、
 * 是否全员禁言、是否已解散），那个回答「谁在群里、是什么角色」。
 *
 * <p>为什么不合并成一个端口：两者的生命周期与规模完全不同——群资料是「一个群一行」，
 * 成员是「群 × 人」。合并后任何只想读群名的路径（比如消息准入里的禁言判断）
 * 都会被绑上一个可能上万行的成员查询。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public interface GroupRepository {

    /**
     * 取群资料。
     *
     * <p>群不存在时返回空而不是一个默认对象：调用方必须能区分「群存在但没配置」
     * 与「这个群根本不存在」——后者在消息准入里应当是拒绝，而不是按默认值放行。
     */
    Optional<GroupInfo> find(String groupId);

    /** 整体覆盖保存（建群与所有治理动作都走它，避免每种改动各写一条 update） */
    void save(GroupInfo info);

    /**
     * 解散群（逻辑删除）。
     *
     * <p>不物理删：历史消息、审计、以及「解散后仍能看历史」都依赖群资料还在。
     * 解散只让后续消息一律被拒绝。
     *
     * @return 是否从「未解散」变成「已解散」；重复解散返回 {@code false}
     */
    boolean dismiss(String groupId);
}
