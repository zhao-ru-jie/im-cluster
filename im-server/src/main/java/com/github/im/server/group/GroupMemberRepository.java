package com.github.im.server.group;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 群成员存储端口（IM 服务独占）。
 *
 * <p>与 {@code MessageRepository} 同一套切换策略：默认 Redis（{@code im:group:members:{groupId}}，
 * 零外部依赖），{@code im.storage=jdbc} 时切到 MySQL 的 {@code im_group_member} 表。
 * 业务侧（群聊扇出）只依赖 {@link #membersOf}，不感知存储介质。
 *
 * <p><b>为什么不缓存</b>：群成员是扇出的权威来源，缓存与库不一致会直接表现为「有人收不到消息」。
 * 需要降延迟时先按群维度加短 TTL 缓存 + 成员变更时主动失效，而不是把缓存当唯一来源。
 *
 * @author zhaorujie
 * @since 1.0.0
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

    /**
     * 分页取群成员（成员列表接口用）。
     *
     * <p>为什么不能只靠 {@link #membersOf}：5000 人群的成员列表此前是「一次全量返回」，
     * 客户端拿到 5000 条才切页——既浪费带宽，也让服务端每次都做一次 O(N) 序列化。
     *
     * <p><b>为什么按 userId 字典序</b>：offset 分页要求顺序稳定，否则翻页会重复/漏人。
     * Redis 的 Set 本身无序，按 userId 排序是唯一不需要额外存储（不需要再维护一个
     * 加入时间的有序结构）的稳定顺序。代价是「管理员不会自动排在第一页」——那需要
     * 另一种「角色优先」的列表设计，不在这里混。
     *
     * @param offset 从 0 开始；负数按 0 处理
     * @param limit  {@code <= 0} 表示「取到末尾」
     */
    default List<String> membersOf(String groupId, int offset, int limit) {
        List<String> all = new ArrayList<>(membersOf(groupId));
        all.sort(Comparator.naturalOrder());
        int from = Math.max(offset, 0);
        if (from >= all.size()) {
            return List.of();
        }
        int to = limit <= 0 ? all.size() : (int) Math.min((long) from + limit, all.size());
        return new ArrayList<>(all.subList(from, to));
    }

    /**
     * 成员数。
     *
     * <p>存在理由是「拉人前要判人数上限」：那件事只需要一个数字，此前却要拉全量成员。
     * 默认实现退回 {@link #membersOf}，Redis 实现覆盖成 {@code SCARD}（O(1)）。
     */
    default long memberCount(String groupId) {
        return membersOf(groupId).size();
    }

    /**
     * 批量取禁言到期时间（成员列表渲染用）。
     *
     * <p>默认实现逐个 {@link #mutedUntil}：N 个成员就是 N 次往返。5000 人群的成员列表此前
     * 正是栽在这里——接口本身没分页，还叠加了 N+1 查询。Redis 实现覆盖成一次 {@code HGETALL}。
     *
     * @return userId → 仍在禁言期内的到期时间（已过期的不出现在结果里）
     */
    default Map<String, Long> mutesOf(String groupId) {
        Map<String, Long> result = new LinkedHashMap<>();
        long now = System.currentTimeMillis();
        for (String userId : membersOf(groupId)) {
            long until = mutedUntil(groupId, userId);
            if (until > now) {
                result.put(userId, until);
            }
        }
        return result;
    }

    /**
     * 某用户加入的群 ID 列表（反向索引：{@code userId -> groupId}）。
     *
     * <p>用途是<b>会话列表的补齐</b>：群消息的会话列表是写扩散的，大群会跳过写扩散
     * （见 {@code ConversationStore#fanoutLimit}），此时成员的会话列表里没有这个群，
     * 只能靠这张反查表在拉取时补出来。少了它，大群成员「明明在群里却看不到会话」。
     *
     * <p>空列表表示该用户没有加入任何群。
     */
    List<String> groupsOf(String userId);

    /**
     * 某用户是否在这个群里（<b>发言准入</b>用）。
     *
     * <p>默认实现退回 {@link #membersOf}，正确性没问题，但代价是**每次群消息都要拉全量成员**：
     * 一个万人群，每条消息都是一次 1 万成员的 SMEMBERS。所以两个实现都覆盖成 O(1)
     * （Redis {@code SISMEMBER}、JDBC 走 {@code uk_group_member} 唯一键的存在性查询）。
     *
     * <p>注意它和 {@link #membersOf} 的分工：这个只回答「能不能发」，扇出仍然要全量成员。
     */
    default boolean isMember(String groupId, String userId) {
        return userId != null && membersOf(groupId).contains(userId);
    }

    /**
     * 成员角色。
     *
     * @return 非成员返回 {@code null}（而不是 MEMBER），让调用方能区分「不是成员」与「是普通成员」——
     *         这两者在鉴权里是完全不同的结论，混成一个值就会出现「陌生人被当成普通成员放行」
     */
    GroupRole roleOf(String groupId, String userId);

    /** 设置角色；不改成员资格（不在这里做「必须是成员」的校验，那是服务层的规则） */
    void setRole(String groupId, String userId, GroupRole role);

    /** 批量取角色（成员列表渲染用，避免逐人查询） */
    Map<String, GroupRole> rolesOf(String groupId);

    /**
     * 批量取<b>指定几个人的</b>角色（成员列表分页用）。
     *
     * <p>{@link #rolesOf(String)} 是「这个群的全部角色」，它内部要先拉全量成员再回填 MEMBER——
     * 为了一页 50 个人去构造一张 5000 项的 map 是白费力气。这里只回答「这些人各是什么角色」。
     *
     * <p>默认实现逐个 {@link #roleOf}（N 次往返）；Redis 实现覆盖成一次 {@code HMGET}，
     * 因为角色 Hash 里只有非 MEMBER 的条目（通常个位数）。
     *
     * @return 入参里那些<b>是成员</b>的人；非成员不会出现在结果里（与 {@link #roleOf} 的 null 语义一致）
     */
    default Map<String, GroupRole> rolesOf(String groupId, Collection<String> userIds) {
        Map<String, GroupRole> result = new LinkedHashMap<>();
        if (userIds == null) {
            return result;
        }
        for (String userId : userIds) {
            GroupRole role = roleOf(groupId, userId);
            if (role != null) {
                result.put(userId, role);
            }
        }
        return result;
    }

    /**
     * 单人禁言的到期时间。
     *
     * @return 毫秒时间戳；{@code 0} 或小于当前时间表示未禁言
     */
    long mutedUntil(String groupId, String userId);

    /**
     * 禁言到某个时间点。
     *
     * @param untilMillis 到期时间；{@code <= now} 表示解除禁言
     */
    void mute(String groupId, String userId, long untilMillis);
}
