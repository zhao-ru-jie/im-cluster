package com.github.im.server.group;

import com.github.im.server.message.ClientFrames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * 群治理：建群、群资料、成员与角色、禁言、解散。
 *
 * <h2>为什么权限判断全在这一层</h2>
 *
 * <p>「谁能做什么」是群体系里最容易分叉的东西：HTTP 接口一套判断、消息准入又一套判断，
 * 两处一旦漂移就会出现「接口拒绝了但消息能发」这种很难复现的问题。
 * 所以规则集中在这里（{@link GroupRole#outranks} 提供顺序语义），
 * 消息侧只调用 {@link #canSpeak(String, String)}。
 *
 * <h2>五条不可让步的规则</h2>
 *
 * <ol>
 *   <li><b>群主不能被操作</b>：没人能踢群主、禁言群主——群主退出只能走解散；</li>
 *   <li><b>同级不能互踢</b>：管理员不能踢管理员，否则群会在一天内变成无主状态；</li>
 *   <li><b>群主不能直接退群</b>：返回 409，要求先解散——否则群变成没有 owner 的孤儿；</li>
 *   <li><b>解散是逻辑删除</b>：历史消息与群资料都留着，后续消息一律拒绝（可读不可写）；</li>
 *   <li><b>群 ID 不可枚举</b>：用 12 位随机串而不是自增，否则「猜一个群 ID 就能往里发消息」。</li>
 * </ol>
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Service
public class GroupService {

    private static final Logger log = LoggerFactory.getLogger(GroupService.class);

    private static final int CODE_OK = 0;
    private static final int CODE_BAD_REQUEST = 400;
    private static final int CODE_FORBIDDEN = 403;
    /** 群不存在或已解散：两者对调用方是同一个结论，不额外暴露「存在但解散了」 */
    private static final int CODE_NOT_FOUND = 404;
    /** 状态冲突：群主试图退群、群已解散还做治理操作 */
    private static final int CODE_CONFLICT = 409;
    /** 人数超限 */
    private static final int CODE_TOO_LARGE = 413;

    private static final int DEFAULT_MAX_MEMBERS = 200;
    /** 群 ID 长度：12 位 base36 ≈ 4.7e18 组合，扫不出来，也不至于长到影响 key 长度 */
    private static final int GROUP_ID_LENGTH = 12;

    private final GroupRepository groups;
    private final GroupMemberRepository members;
    /** 可空：单测与「纯存储」场景不需要下行事件 */
    private final GroupEventNotifier notifier;

    public GroupService(GroupRepository groups, GroupMemberRepository members,
                        GroupEventNotifier notifier) {
        this.groups = groups;
        this.members = members;
        this.notifier = notifier;
    }

    // ==== 读 ====

    public Optional<GroupInfo> info(String groupId) {
        return groups.find(groupId);
    }

    /** 群详情：资料 + 每个成员的角色与禁言到期时间（客户端据此渲染管理菜单） */
    public Optional<GroupDetail> detail(String groupId) {
        Optional<GroupInfo> info = groups.find(groupId);
        if (info.isEmpty()) {
            return Optional.empty();
        }
        Map<String, GroupRole> roles = members.rolesOf(groupId);
        Map<String, Long> mutes = new LinkedHashMap<>();
        long now = System.currentTimeMillis();
        for (String userId : roles.keySet()) {
            long until = members.mutedUntil(groupId, userId);
            if (until > now) {
                mutes.put(userId, until);
            }
        }
        return Optional.of(new GroupDetail(info.get(), roles, mutes));
    }

    /**
     * 能否发言（消息准入调用）。
     *
     * <p>顺序即优先级：非成员 → 群不存在/已解散 → 个人禁言 → 全员禁言。
     * 个人禁言先于全员禁言判断，是为了让 403 的原因更准确。
     *
     * @return {@code null} 表示可以发；否则是拒绝原因
     */
    public String canSpeak(String groupId, String userId) {
        GroupRole role = members.roleOf(groupId, userId);
        if (role == null) {
            return "sender is not a member of group " + groupId;
        }
        Optional<GroupInfo> info = groups.find(groupId);
        if (info.isEmpty()) {
            return "group not found: " + groupId;
        }
        if (info.get().dismissed()) {
            return "group dismissed: " + groupId;
        }
        long now = System.currentTimeMillis();
        long mutedUntil = members.mutedUntil(groupId, userId);
        if (mutedUntil > now) {
            return "sender is muted until " + mutedUntil;
        }
        if (info.get().mutedAll() && !role.canManageGroup()) {
            return "group is muted for all members";
        }
        return null;
    }

    // ==== 建群与治理 ====

    /**
     * 建群：创建者成为群主。
     *
     * <p>群 ID 用随机串（不可枚举），并且<b>先落群资料再落成员</b>：
     * 反过来的话，中间失败会留下「成员存在但群不存在」的集合，
     * 而消息准入按「群不存在」拒绝——症状是「群建好了但一句话都发不出去」。
     */
    public GroupResult create(String ownerId, String name, int maxMembers) {
        if (!StringUtils.hasText(ownerId)) {
            return GroupResult.fail(CODE_BAD_REQUEST, "ownerId is required");
        }
        String groupId = newGroupId();
        GroupInfo info = GroupInfo.create(groupId, name, ownerId,
                maxMembers <= 0 ? DEFAULT_MAX_MEMBERS : maxMembers, System.currentTimeMillis());
        groups.save(info);
        members.addMembers(groupId, List.of(ownerId));
        members.setRole(groupId, ownerId, GroupRole.OWNER);
        notifyChange(groupId, ClientFrames.GROUP_CREATED, ownerId, ownerId, List.of(ownerId));
        log.info("[group] 建群 group={} owner={} name={}", groupId, ownerId, info.name());
        return GroupResult.ok(info);
    }

    public GroupResult rename(String operatorId, String groupId, String name) {
        if (!StringUtils.hasText(name)) {
            return GroupResult.fail(CODE_BAD_REQUEST, "name is required");
        }
        GroupInfo info = activeGroup(groupId);
        if (info == null) {
            return GroupResult.fail(CODE_NOT_FOUND, "group not found");
        }
        if (!isOwner(operatorId, groupId)) {
            return GroupResult.fail(CODE_FORBIDDEN, "only the owner can rename the group");
        }
        GroupInfo updated = info.withName(name);
        groups.save(updated);
        notifyChange(groupId, ClientFrames.GROUP_UPDATED, operatorId, null, groupAudience(groupId));
        return GroupResult.ok(updated);
    }

    /** 群公告：管理员也能改（这是最常被下放的一项） */
    public GroupResult announce(String operatorId, String groupId, String text) {
        GroupInfo info = activeGroup(groupId);
        if (info == null) {
            return GroupResult.fail(CODE_NOT_FOUND, "group not found");
        }
        GroupRole role = members.roleOf(groupId, operatorId);
        if (role == null || !role.canManageGroup()) {
            return GroupResult.fail(CODE_FORBIDDEN, "only owner or admin can change the announcement");
        }
        GroupInfo updated = info.withAnnouncement(text == null ? "" : text);
        groups.save(updated);
        notifyChange(groupId, ClientFrames.GROUP_UPDATED, operatorId, null, groupAudience(groupId));
        return GroupResult.ok(updated);
    }

    /** 全员禁言：管理员也可以（禁言是管理动作，不是所有权动作） */
    public GroupResult muteAll(String operatorId, String groupId, boolean muted) {
        GroupInfo info = activeGroup(groupId);
        if (info == null) {
            return GroupResult.fail(CODE_NOT_FOUND, "group not found");
        }
        GroupRole role = members.roleOf(groupId, operatorId);
        if (role == null || !role.canManageGroup()) {
            return GroupResult.fail(CODE_FORBIDDEN, "only owner or admin can mute the whole group");
        }
        GroupInfo updated = info.withMutedAll(muted);
        groups.save(updated);
        notifyChange(groupId, ClientFrames.GROUP_UPDATED, operatorId, null, groupAudience(groupId));
        return GroupResult.ok(updated);
    }

    /** 人数上限：只有群主能改（它直接决定「谁能把这个群撑爆」） */
    public GroupResult setMaxMembers(String operatorId, String groupId, int maxMembers) {
        if (maxMembers <= 0) {
            return GroupResult.fail(CODE_BAD_REQUEST, "maxMembers must be positive");
        }
        GroupInfo info = activeGroup(groupId);
        if (info == null) {
            return GroupResult.fail(CODE_NOT_FOUND, "group not found");
        }
        if (!isOwner(operatorId, groupId)) {
            return GroupResult.fail(CODE_FORBIDDEN, "only the owner can change max members");
        }
        GroupInfo updated = info.withMaxMembers(maxMembers);
        groups.save(updated);
        notifyChange(groupId, ClientFrames.GROUP_UPDATED, operatorId, null, groupAudience(groupId));
        return GroupResult.ok(updated);
    }

    /**
     * 拉人入群。
     *
     * <p>任何成员都可以拉人（社交产品的常态），但受人数上限约束。
     * 管理员/群主批量拉人同样走这里，区别只在人数上限的宽容度——不为它单独开一条路径。
     *
     * <p>{@code membersOf} 是全量读取：治理动作是低频的，为它维护一份「成员数」计数器
     * 反而要处理一致性（加人成功但计数失败就永久偏小），不值。
     */
    public GroupResult addMembers(String operatorId, String groupId, Collection<String> userIds) {
        if (userIds == null || userIds.isEmpty()) {
            return GroupResult.fail(CODE_BAD_REQUEST, "userIds is required");
        }
        GroupInfo info = activeGroup(groupId);
        if (info == null) {
            return GroupResult.fail(CODE_NOT_FOUND, "group not found");
        }
        if (members.roleOf(groupId, operatorId) == null) {
            return GroupResult.fail(CODE_FORBIDDEN, "operator is not a member of the group");
        }
        List<String> existing = members.membersOf(groupId);
        List<String> toAdd = new ArrayList<>();
        for (String userId : new LinkedHashSet<>(userIds)) {
            if (StringUtils.hasText(userId) && !existing.contains(userId)) {
                toAdd.add(userId);
            }
        }
        if (toAdd.isEmpty()) {
            return GroupResult.ok(info);
        }
        if (existing.size() + toAdd.size() > info.maxMembers()) {
            return GroupResult.fail(CODE_TOO_LARGE,
                    "group would exceed max members (" + info.maxMembers() + ")");
        }
        members.addMembers(groupId, toAdd);
        List<String> audience = new ArrayList<>(members.membersOf(groupId));
        audience.addAll(toAdd);
        notifyChange(groupId, ClientFrames.GROUP_MEMBER_ADDED, operatorId, toAdd.getFirst(), audience);
        return GroupResult.ok(info);
    }

    /**
     * 踢人。
     *
     * <p>权限要求是<b>严格大于</b>目标角色：管理员不能踢管理员，也没人能踢群主。
     * 这三条挡掉的是最常见的两类事故——同级互踢导致群无人管理，以及「群主被踢」的越权。
     */
    public GroupResult kick(String operatorId, String groupId, String targetId) {
        GroupInfo info = activeGroup(groupId);
        if (info == null) {
            return GroupResult.fail(CODE_NOT_FOUND, "group not found");
        }
        GroupRole operator = members.roleOf(groupId, operatorId);
        GroupRole target = members.roleOf(groupId, targetId);
        if (operator == null || !operator.canManageMembers()) {
            return GroupResult.fail(CODE_FORBIDDEN, "only owner or admin can remove members");
        }
        if (target == null) {
            return GroupResult.fail(CODE_NOT_FOUND, "target is not a member of the group");
        }
        if (!operator.outranks(target)) {
            return GroupResult.fail(CODE_FORBIDDEN,
                    "cannot remove a member whose role is " + target);
        }
        members.removeMember(groupId, targetId);
        // 被踢的人已经不是成员了，必须显式加进接收方，否则他收不到「你已被移出群」
        List<String> audience = new ArrayList<>(members.membersOf(groupId));
        audience.add(targetId);
        notifyChange(groupId, ClientFrames.GROUP_MEMBER_REMOVED, operatorId, targetId, audience);
        log.info("[group] 踢人 group={} operator={} target={}", groupId, operatorId, targetId);
        return GroupResult.ok(info);
    }

    /** 退群；群主必须先解散（或转让，转让暂未实现） */
    public GroupResult leave(String userId, String groupId) {
        GroupInfo info = activeGroup(groupId);
        if (info == null) {
            return GroupResult.fail(CODE_NOT_FOUND, "group not found");
        }
        GroupRole role = members.roleOf(groupId, userId);
        if (role == null) {
            return GroupResult.fail(CODE_NOT_FOUND, "user is not a member of the group");
        }
        if (role == GroupRole.OWNER) {
            return GroupResult.fail(CODE_CONFLICT,
                    "owner cannot leave; disband the group first (transfer is not supported yet)");
        }
        members.removeMember(groupId, userId);
        List<String> audience = new ArrayList<>(members.membersOf(groupId));
        audience.add(userId);
        notifyChange(groupId, ClientFrames.GROUP_MEMBER_LEFT, userId, userId, audience);
        return GroupResult.ok(info);
    }

    /**
     * 设置角色。
     *
     * <p>只有群主能设/撤管理员；且<b>不能把别人设成群主</b>——转让群主是另一个动作
     * （需要同时改群资料的 ownerId 与两个成员角色），混在「设角色」里会让「谁是群主」
     * 出现两个真相来源。当前不提供转让，群主唯一的退出路径是解散。
     */
    public GroupResult setRole(String operatorId, String groupId, String targetId, GroupRole role) {
        if (role == null) {
            return GroupResult.fail(CODE_BAD_REQUEST, "role is required");
        }
        if (role == GroupRole.OWNER) {
            return GroupResult.fail(CODE_BAD_REQUEST,
                    "ownership transfer is not supported; do not set OWNER directly");
        }
        GroupInfo info = activeGroup(groupId);
        if (info == null) {
            return GroupResult.fail(CODE_NOT_FOUND, "group not found");
        }
        if (!isOwner(operatorId, groupId)) {
            return GroupResult.fail(CODE_FORBIDDEN, "only the owner can change roles");
        }
        GroupRole target = members.roleOf(groupId, targetId);
        if (target == null) {
            return GroupResult.fail(CODE_NOT_FOUND, "target is not a member of the group");
        }
        if (target == GroupRole.OWNER) {
            return GroupResult.fail(CODE_FORBIDDEN, "the owner's role cannot be changed");
        }
        members.setRole(groupId, targetId, role);
        notifyChange(groupId, ClientFrames.GROUP_ROLE_CHANGED, operatorId, targetId, groupAudience(groupId));
        return GroupResult.ok(info);
    }

    /**
     * 单人禁言。
     *
     * @param seconds 时长；{@code <= 0} 表示解除禁言
     */
    public GroupResult mute(String operatorId, String groupId, String targetId, long seconds) {
        GroupInfo info = activeGroup(groupId);
        if (info == null) {
            return GroupResult.fail(CODE_NOT_FOUND, "group not found");
        }
        GroupRole operator = members.roleOf(groupId, operatorId);
        GroupRole target = members.roleOf(groupId, targetId);
        if (operator == null || !operator.canManageMembers()) {
            return GroupResult.fail(CODE_FORBIDDEN, "only owner or admin can mute members");
        }
        if (target == null) {
            return GroupResult.fail(CODE_NOT_FOUND, "target is not a member of the group");
        }
        if (!operator.outranks(target)) {
            return GroupResult.fail(CODE_FORBIDDEN, "cannot mute a member whose role is " + target);
        }
        long until = seconds <= 0 ? 0L : System.currentTimeMillis() + seconds * 1000L;
        members.mute(groupId, targetId, until);
        notifyChange(groupId, until > 0 ? ClientFrames.GROUP_MUTED : ClientFrames.GROUP_UNMUTED,
                operatorId, targetId, groupAudience(groupId));
        return GroupResult.ok(info);
    }

    /**
     * 解散群：只有群主。
     *
     * <p><b>成员集合保留</b>：解散后历史消息仍可读，客户端要靠成员关系判断「这个会话我还有效」；
     * 清空成员会让历史会话直接消失，而不是显示为「群已解散」。
     */
    public GroupResult disband(String operatorId, String groupId) {
        GroupInfo info = activeGroup(groupId);
        if (info == null) {
            return GroupResult.fail(CODE_NOT_FOUND, "group not found");
        }
        if (!isOwner(operatorId, groupId)) {
            return GroupResult.fail(CODE_FORBIDDEN, "only the owner can disband the group");
        }
        if (!groups.dismiss(groupId)) {
            return GroupResult.fail(CODE_CONFLICT, "group is already dismissed");
        }
        notifyChange(groupId, ClientFrames.GROUP_DISMISSED, operatorId, null, groupAudience(groupId));
        log.info("[group] 解散 group={} operator={}", groupId, operatorId);
        return GroupResult.ok(info.dismissedAt(System.currentTimeMillis()));
    }

    // ==== 内部 ====

    /** 存在且未解散；否则 null（对调用方来说两者都是「不能操作」） */
    private GroupInfo activeGroup(String groupId) {
        if (!StringUtils.hasText(groupId)) {
            return null;
        }
        return groups.find(groupId).filter(info -> !info.dismissed()).orElse(null);
    }

    private boolean isOwner(String userId, String groupId) {
        return members.roleOf(groupId, userId) == GroupRole.OWNER;
    }

    private List<String> groupAudience(String groupId) {
        return members.membersOf(groupId);
    }

    private void notifyChange(String groupId, String change, String actorId, String targetId,
                              List<String> targets) {
        if (notifier == null) {
            return;
        }
        notifier.notifyChange(groupId, change, actorId, targetId, targets);
    }

    /**
     * 群 ID：12 位随机 base36。
     *
     * <p>刻意不用自增：群 ID 会出现在会话 ID（{@code g:{groupId}}）里并可被任何成员看到，
     * 自增等于把「下一个人猜得到的 ID」发出去——配合每日几万次的群消息写入，
     * 那是一类成本极低、影响却不小的探测面。
     */
    private static String newGroupId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, GROUP_ID_LENGTH);
    }

    /**
     * 治理动作的结果。
     *
     * @param code  0 = 成功
     * @param group 成功时的最新群资料（失败时为 null）
     */
    public record GroupResult(int code, String message, GroupInfo group) {

        static GroupResult ok(GroupInfo group) {
            return new GroupResult(CODE_OK, "ok", group);
        }

        static GroupResult fail(int code, String message) {
            return new GroupResult(code, message, null);
        }

        public boolean isOk() {
            return code == CODE_OK;
        }
    }

    /**
     * 群详情视图。
     *
     * @param roles 每个成员的角色
     * @param mutes 仍在禁言期内的成员（userId → 到期时间）
     */
    public record GroupDetail(GroupInfo group, Map<String, GroupRole> roles, Map<String, Long> mutes) {
    }
}
