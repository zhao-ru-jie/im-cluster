package com.github.im.server.group;

import com.github.im.server.message.ClientFrames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
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
 * <h2>七条不可让步的规则</h2>
 *
 * <ol>
 *   <li><b>群主不能被操作</b>：没人能踢群主、禁言群主——群主退出只能走解散；</li>
 *   <li><b>同级不能互踢</b>：管理员不能踢管理员，否则群会在一天内变成无主状态；</li>
 *   <li><b>群主不能直接退群</b>：返回 409，要求先解散——否则群变成没有 owner 的孤儿；</li>
 *   <li><b>解散是逻辑删除</b>：历史消息与群资料都留着，后续消息一律拒绝（可读不可写）；</li>
 *   <li><b>群 ID 不可枚举</b>：用 12 位随机串而不是自增，否则「猜一个群 ID 就能往里发消息」；</li>
 *   <li><b>入群有两条路，门槛不同</b>：成员直接拉人（熟人场景）与申请审批 / 邀请码
 *       （群 ID 被外人知道时）。普通成员生成的邀请码<b>强制要求审批</b>——否则他等于
 *       生成了一条「点一下就进人」的永久链接，把管理员的审批权直接绕过；</li>
 *   <li><b>转让群主先给后降</b>：最坏留下两个 OWNER（任何一方可再转让，自愈），
 *       而不是零个 OWNER（群永久无主，不可恢复）。</li>
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
    /** 邀请码已失效（过期 / 次数用尽）：与 404「不存在」分开，客户端文案不一样 */
    private static final int CODE_GONE = 410;
    /** 人数超限 */
    private static final int CODE_TOO_LARGE = 413;
    /** 能力未装配（入群申请存储缺失）：不是客户端的错，可重试 */
    private static final int CODE_UNAVAILABLE = 503;

    private static final int DEFAULT_MAX_MEMBERS = 200;
    /** 群 ID 长度：12 位 base36 ≈ 4.7e18 组合，扫不出来，也不至于长到影响 key 长度 */
    private static final int GROUP_ID_LENGTH = 12;

    /** 分页默认条数：与建群默认人数同量级，一页就能把常见群列完 */
    private static final int DEFAULT_PAGE_SIZE = 50;
    /** 分页上限：再大就退回「一次拉全量」，分页就白做了 */
    private static final int MAX_PAGE_SIZE = 500;

    /** 邀请码默认有效期（小时）：24h 太短、永久太长，一周是「拉人进群」这件事的自然寿命 */
    private static final long DEFAULT_INVITE_TTL_HOURS = 24L * 7;
    /** 邀请码有效期上限（小时）：30 天 */
    private static final long MAX_INVITE_TTL_HOURS = 24L * 30;

    private final GroupRepository groups;
    private final GroupMemberRepository members;
    /** 可空：单测与「纯存储」场景不需要下行事件 */
    private final GroupEventNotifier notifier;
    /** 可空：入群申请与邀请码；为空时入群相关动作返回 503，群本身的治理不受影响 */
    private final GroupJoinStore joins;
    /** 邀请链接前缀（客户端据此生成二维码）：默认 {@code im://group/join?code=} */
    private final String inviteLinkBase;

    public GroupService(GroupRepository groups, GroupMemberRepository members,
                        GroupEventNotifier notifier, GroupJoinStore joins,
                        @Value("${im.group.invite-link:im://group/join?code=}") String inviteLinkBase) {
        this.groups = groups;
        this.members = members;
        this.notifier = notifier;
        this.joins = joins;
        this.inviteLinkBase = inviteLinkBase == null || inviteLinkBase.isBlank()
                ? "im://group/join?code=" : inviteLinkBase;
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
        // 一次批量读禁言。此前是「每个成员各查一次 mutedUntil」——5000 人群的群详情
        // 就是 5000 次往返，而禁言表里通常只有个位数条目。
        Map<String, Long> mutes = new LinkedHashMap<>();
        members.mutesOf(groupId).forEach((userId, until) -> {
            if (roles.containsKey(userId)) {
                mutes.put(userId, until);
            }
        });
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

    /** 退群；群主必须先转让或解散（群不能没有主） */
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
                    "owner cannot leave; transfer ownership or disband the group first");
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
     * （见 {@link #transferOwnership}，它要同时改群资料的 {@code ownerId} 与两个成员角色），
     * 混在「设角色」里会让「谁是群主」出现两个真相来源。
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

    // ==== 成员列表（分页） ====

    /**
     * 成员列表：分页 + 每人的角色与禁言状态。
     *
     * <p>此前这个接口一次返回全部成员（含 roles / mutes 两张全量 map）：5000 人群就是
     * 一份 5000 项的响应，而客户端一屏只能显示十几行。分页之后服务端每页只做
     * 「一次成员切片 + 一次角色批量查 + 一次禁言批量查」。
     *
     * <p>顺序是 userId 字典序（见 {@link GroupMemberRepository#membersOf(String, int, int)}）：
     * offset 分页必须有一个稳定顺序，否则翻页会重复或漏人。
     *
     * @param offset 从 0 开始
     * @param limit  {@code <= 0} 用默认页大小（50），上限 500
     */
    public MemberPage members(String groupId, int offset, int limit) {
        Optional<GroupInfo> info = groups.find(groupId);
        if (info.isEmpty()) {
            return MemberPage.fail(CODE_NOT_FOUND, "group not found");
        }
        int safeOffset = Math.max(offset, 0);
        int safeLimit = pageSize(limit);
        List<String> page = members.membersOf(groupId, safeOffset, safeLimit);
        // 只查这一页的人：为 50 个人构造一张 5000 项的 map 没有意义
        Map<String, GroupRole> roles = members.rolesOf(groupId, page);
        Map<String, Long> mutes = members.mutesOf(groupId);
        long now = System.currentTimeMillis();
        List<MemberView> views = new ArrayList<>(page.size());
        for (String userId : page) {
            long until = mutes.getOrDefault(userId, 0L);
            views.add(new MemberView(userId,
                    roles.getOrDefault(userId, GroupRole.MEMBER),
                    until > now ? until : 0L));
        }
        long total = members.memberCount(groupId);
        boolean hasMore = safeOffset + views.size() < total;
        return new MemberPage(CODE_OK, "ok", views, (int) total, safeOffset, safeLimit, hasMore);
    }

    // ==== 入群申请（审批入群） ====

    /**
     * 提交入群申请。
     *
     * <p>已有另一条入群路径是「成员直接拉人」({@link #addMembers})——那条适合熟人之间，
     * 这条适合「群 ID 或群名被外人知道」的场景：不设门槛的话，任何人拿到群 ID 就能进来。
     *
     * <p>幂等：同一人重复申请不会堆出多条（Hash 按 applicantId 覆盖），
     * 也不刷新 {@code createdAt}——审批列表按「谁等了最久」排序。
     */
    public JoinRequestResult requestJoin(String groupId, String applicantId, String message) {
        if (!StringUtils.hasText(applicantId)) {
            return JoinRequestResult.fail(CODE_BAD_REQUEST, "userId is required");
        }
        GroupInfo info = activeGroup(groupId);
        if (info == null) {
            return JoinRequestResult.fail(CODE_NOT_FOUND, "group not found");
        }
        if (members.isMember(groupId, applicantId)) {
            // 已经是成员还申请，是客户端的重复动作：按冲突回，不要静默成功让他以为在排队
            return JoinRequestResult.fail(CODE_CONFLICT, "already a member of this group");
        }
        GroupJoinStore store = joinStore();
        if (store == null) {
            return JoinRequestResult.fail(CODE_UNAVAILABLE, "join requests are not available");
        }
        if (members.memberCount(groupId) >= info.maxMembers()) {
            // 满员时不必让人等着被拒：直接告诉他进不来，也省掉一条永远处理不了的申请
            return JoinRequestResult.fail(CODE_TOO_LARGE,
                    "group is full (" + info.maxMembers() + ")");
        }
        GroupJoinRequest existing = store.findRequest(groupId, applicantId);
        long now = System.currentTimeMillis();
        GroupJoinRequest request = GroupJoinRequest.pending(groupId, applicantId, message,
                existing == null ? now : existing.createdAt(), now);
        store.saveRequest(request);
        List<String> audience = approvers(groupId);
        audience.add(applicantId);
        notifyChange(groupId, ClientFrames.GROUP_JOIN_REQUESTED, applicantId, applicantId, audience);
        log.info("[group] 入群申请 group={} applicant={}", groupId, applicantId);
        return JoinRequestResult.ok(request);
    }

    /** 我的申请状态（申请人自己看；不含别人对这个群的申请） */
    public JoinRequestResult joinRequestOf(String groupId, String applicantId) {
        if (!StringUtils.hasText(applicantId)) {
            return JoinRequestResult.fail(CODE_BAD_REQUEST, "userId is required");
        }
        GroupJoinStore store = joinStore();
        if (store == null) {
            return JoinRequestResult.fail(CODE_UNAVAILABLE, "join requests are not available");
        }
        GroupJoinRequest request = store.findRequest(groupId, applicantId);
        if (request == null) {
            return JoinRequestResult.fail(CODE_NOT_FOUND, "no join request from " + applicantId);
        }
        return JoinRequestResult.ok(request);
    }

    /** 待审申请列表（群主 / 管理员；分页） */
    public JoinRequestPage pendingJoins(String operatorId, String groupId, int offset, int limit) {
        GroupInfo info = activeGroup(groupId);
        if (info == null) {
            return JoinRequestPage.fail(CODE_NOT_FOUND, "group not found");
        }
        GroupRole role = members.roleOf(groupId, operatorId);
        if (role == null || !role.canManageGroup()) {
            // 待审列表本身就是隐私（谁想进这个群），只给能处理它的人看
            return JoinRequestPage.fail(CODE_FORBIDDEN,
                    "only owner or admin can view join requests");
        }
        GroupJoinStore store = joinStore();
        if (store == null) {
            return JoinRequestPage.fail(CODE_UNAVAILABLE, "join requests are not available");
        }
        List<GroupJoinRequest> all = store.pendingRequests(groupId);
        int safeOffset = Math.max(offset, 0);
        int safeLimit = pageSize(limit);
        if (safeOffset >= all.size()) {
            return new JoinRequestPage(CODE_OK, "ok", List.of(), all.size(), safeOffset, safeLimit);
        }
        int to = (int) Math.min((long) safeOffset + safeLimit, all.size());
        return new JoinRequestPage(CODE_OK, "ok", all.subList(safeOffset, to),
                all.size(), safeOffset, safeLimit);
    }

    /** 通过入群申请：加成员 + 通知（申请人此刻才成为成员，必须显式并进接收方） */
    public GroupResult approveJoin(String operatorId, String groupId, String applicantId) {
        GroupInfo info = activeGroup(groupId);
        if (info == null) {
            return GroupResult.fail(CODE_NOT_FOUND, "group not found");
        }
        GroupRole role = members.roleOf(groupId, operatorId);
        if (role == null || !role.canManageGroup()) {
            return GroupResult.fail(CODE_FORBIDDEN, "only owner or admin can approve join requests");
        }
        GroupJoinStore store = joinStore();
        if (store == null) {
            return GroupResult.fail(CODE_UNAVAILABLE, "join requests are not available");
        }
        GroupJoinRequest request = store.findRequest(groupId, applicantId);
        if (request == null || !request.isPending()) {
            // 多个管理员同时看到同一条待审是常态，谁点慢一步都会落到这里
            return GroupResult.fail(CODE_NOT_FOUND, "no pending join request from " + applicantId);
        }
        GroupResult joined = joinUser(info, groupId, applicantId);
        if (!joined.isOk()) {
            return joined;
        }
        store.deleteRequest(groupId, applicantId);
        // 复制一份再改：membersOf 返回的列表在不同实现里可能是不可变的（内存实现用的 List.copyOf），
        // 直接 add 会炸在运行期——这类错误在单测里暴露，比在生产里暴露便宜得多
        List<String> audience = new ArrayList<>(groupAudience(groupId));
        audience.add(applicantId);
        notifyChange(groupId, ClientFrames.GROUP_JOIN_APPROVED, operatorId, applicantId, audience);
        log.info("[group] 通过入群申请 group={} applicant={} operator={}",
                groupId, applicantId, operatorId);
        return GroupResult.ok(info);
    }

    /** 拒绝入群申请：把记录留成 {@code REJECTED}，申请人查得到结论 */
    public GroupResult rejectJoin(String operatorId, String groupId, String applicantId) {
        GroupInfo info = activeGroup(groupId);
        if (info == null) {
            return GroupResult.fail(CODE_NOT_FOUND, "group not found");
        }
        GroupRole role = members.roleOf(groupId, operatorId);
        if (role == null || !role.canManageGroup()) {
            return GroupResult.fail(CODE_FORBIDDEN, "only owner or admin can reject join requests");
        }
        GroupJoinStore store = joinStore();
        if (store == null) {
            return GroupResult.fail(CODE_UNAVAILABLE, "join requests are not available");
        }
        GroupJoinRequest request = store.findRequest(groupId, applicantId);
        if (request == null || !request.isPending()) {
            return GroupResult.fail(CODE_NOT_FOUND, "no pending join request from " + applicantId);
        }
        store.saveRequest(request.withStatus(GroupJoinRequest.STATUS_REJECTED,
                System.currentTimeMillis()));
        // 只推申请人：拒绝这件事没必要广播给全体成员
        notifyChange(groupId, ClientFrames.GROUP_JOIN_REJECTED, operatorId, applicantId,
                List.of(applicantId));
        return GroupResult.ok(info);
    }

    // ==== 邀请码（邀请链接 / 二维码） ====

    /**
     * 生成邀请码。
     *
     * <p>二维码不单独建模型：它就是这条链接的图形化，客户端拿 {@code code} 或 {@code link}
     * 自己画。服务端只维护「码」。
     *
     * <p><b>权限上的一个分叉</b>：普通成员也能建码（拉人是群成员的自然权利，与
     * {@link #addMembers} 的宽松策略一致），但普通成员建的码<b>强制要求审批</b>——
     * 否则任何成员都能生成一条「点一下就把人放进来」的永久链接，等于绕过了管理员。
     * 群主 / 管理员建的码可以按参数决定是否直接入群。
     */
    public InviteResult createInvite(String operatorId, String groupId, long ttlHours,
                                     int maxUses, boolean approvalRequired) {
        GroupInfo info = activeGroup(groupId);
        if (info == null) {
            return InviteResult.fail(CODE_NOT_FOUND, "group not found");
        }
        GroupRole role = members.roleOf(groupId, operatorId);
        if (role == null) {
            return InviteResult.fail(CODE_FORBIDDEN, "operator is not a member of the group");
        }
        GroupJoinStore store = joinStore();
        if (store == null) {
            return InviteResult.fail(CODE_UNAVAILABLE, "invites are not available");
        }
        boolean operatorManages = role.canManageGroup();
        boolean needApproval = approvalRequired || !operatorManages;
        long ttl = ttlHours <= 0 ? DEFAULT_INVITE_TTL_HOURS : Math.min(ttlHours, MAX_INVITE_TTL_HOURS);
        long now = System.currentTimeMillis();
        GroupInvite invite = new GroupInvite(newInviteCode(), groupId, operatorId, now,
                now + ttl * 3600_000L, Math.max(maxUses, 0), 0, needApproval);
        store.saveInvite(invite);
        log.info("[group] 生成邀请码 group={} inviter={} ttlH={} maxUses={} approval={}",
                groupId, operatorId, ttl, invite.maxUses(), needApproval);
        return InviteResult.ok(invite, invite.link(inviteLinkBase));
    }

    /**
     * 用邀请码加入（或提交申请）。
     *
     * <p>两步校验：先读一次判「已是成员」或「需要审批」（这些情况不该消耗次数），
     * 再用一次原子脚本真正取用——限次校验与自增必须在同一段脚本里，否则
     * {@code maxUses=1} 的链接在并发扫码下会被用两次。
     */
    public JoinByInviteResult joinByInvite(String userId, String code) {
        if (!StringUtils.hasText(userId)) {
            return JoinByInviteResult.fail(CODE_BAD_REQUEST, "userId is required");
        }
        if (!StringUtils.hasText(code)) {
            return JoinByInviteResult.fail(CODE_BAD_REQUEST, "code is required");
        }
        GroupJoinStore store = joinStore();
        if (store == null) {
            return JoinByInviteResult.fail(CODE_UNAVAILABLE, "invites are not available");
        }
        GroupInvite invite = store.findInvite(code);
        if (invite == null) {
            return JoinByInviteResult.fail(CODE_NOT_FOUND, "invite code not found");
        }
        long now = System.currentTimeMillis();
        GroupInfo info = activeGroup(invite.groupId());
        if (info == null) {
            return JoinByInviteResult.fail(CODE_NOT_FOUND, "group not found");
        }
        if (members.isMember(invite.groupId(), userId)) {
            // 已在群里：直接告诉他成功，且不要再消耗一次名额
            return JoinByInviteResult.ok(invite.groupId(), info, false);
        }
        if (!invite.usable(now)) {
            return JoinByInviteResult.fail(CODE_GONE, invite.expired(now)
                    ? "invite code expired" : "invite code has reached its usage limit");
        }
        // 满员就先拦掉，不去消耗名额：一次注定失败的申请不该用掉一个 use
        if (members.memberCount(invite.groupId()) >= info.maxMembers()) {
            return JoinByInviteResult.fail(CODE_TOO_LARGE,
                    "group is full (" + info.maxMembers() + ")");
        }
        // 真正取用（原子）。需要审批的码也要消耗一次：码的「次数」限制的是
        // 「这条链接能带多少人来敲门」，不是「能放多少人进来」——否则一条限 1 次的码
        // 可以被无限次用来提交申请。
        GroupJoinStore.InviteUse use = store.useInvite(code, now);
        if (!use.ok()) {
            return JoinByInviteResult.fail(CODE_GONE, switch (use.status()) {
                case EXPIRED -> "invite code expired";
                case EXHAUSTED -> "invite code has reached its usage limit";
                default -> "invite code not found";
            });
        }
        if (invite.approvalRequired()) {
            // 需要审批的码走申请流：码只买到「提交一次申请」的资格，不是入场券
            JoinRequestResult applied = requestJoin(invite.groupId(), userId,
                    "via invite " + code.substring(0, Math.min(code.length(), 4)) + "***");
            if (applied.code() != CODE_OK) {
                return JoinByInviteResult.fail(applied.code(), applied.message());
            }
            return JoinByInviteResult.ok(invite.groupId(), info, true);
        }
        GroupResult joined = joinUser(info, invite.groupId(), userId);
        if (!joined.isOk()) {
            return JoinByInviteResult.fail(joined.code(), joined.message());
        }
        List<String> audience = new ArrayList<>(groupAudience(invite.groupId()));
        audience.add(userId);
        notifyChange(invite.groupId(), ClientFrames.GROUP_MEMBER_ADDED, userId, userId, audience);
        log.info("[group] 邀请码入群 group={} user={} code={}", invite.groupId(), userId, code);
        return JoinByInviteResult.ok(invite.groupId(), info, false);
    }

    /** 群当前有效的邀请码（群主 / 管理员） */
    public InvitePage invites(String operatorId, String groupId) {
        GroupInfo info = activeGroup(groupId);
        if (info == null) {
            return InvitePage.fail(CODE_NOT_FOUND, "group not found");
        }
        GroupRole role = members.roleOf(groupId, operatorId);
        if (role == null || !role.canManageGroup()) {
            return InvitePage.fail(CODE_FORBIDDEN, "only owner or admin can list invites");
        }
        GroupJoinStore store = joinStore();
        if (store == null) {
            return InvitePage.fail(CODE_UNAVAILABLE, "invites are not available");
        }
        return new InvitePage(CODE_OK, "ok", store.invitesOf(groupId), inviteLinkBase);
    }

    /** 吊销邀请码（群主 / 管理员）：链接立刻失效 */
    public GroupResult revokeInvite(String operatorId, String groupId, String code) {
        GroupInfo info = activeGroup(groupId);
        if (info == null) {
            return GroupResult.fail(CODE_NOT_FOUND, "group not found");
        }
        GroupRole role = members.roleOf(groupId, operatorId);
        if (role == null || !role.canManageGroup()) {
            return GroupResult.fail(CODE_FORBIDDEN, "only owner or admin can revoke invites");
        }
        GroupJoinStore store = joinStore();
        if (store == null) {
            return GroupResult.fail(CODE_UNAVAILABLE, "invites are not available");
        }
        store.revokeInvite(groupId, code);
        return GroupResult.ok(info);
    }

    // ==== 群主转让 ====

    /**
     * 转让群主。
     *
     * <p>这是「群」这套体系里唯一需要<b>改两个地方</b>的动作：{@code im_group.owner_id}
     * 与两个成员的角色。所以它不能混进 {@link #setRole}——那个方法明确拒绝把别人设成 OWNER，
     * 就是为了让「谁是群主」只有一个入口。
     *
     * <p><b>写入顺序不是随意的</b>：先给新群主 OWNER → 再改群资料 ownerId → 最后降原群主。
     * 中途失败的最坏结果是「两个 OWNER」（任何一方都能再转让一次，自愈）；
     * 反过来的顺序最坏结果是「零个 OWNER」，那意味着这个群永远没人能解散或再转让——
     * 一个不可恢复的状态。
     *
     * <p>原群主降为 {@code ADMIN} 而不是 {@code MEMBER}：转让通常发生在「退居二线」而不是
     * 「立刻走人」，直接降成普通成员会让他瞬间失去管理能力，新群主还得再操作一次。
     */
    public GroupResult transferOwnership(String operatorId, String groupId, String targetId) {
        GroupInfo info = activeGroup(groupId);
        if (info == null) {
            return GroupResult.fail(CODE_NOT_FOUND, "group not found");
        }
        if (!isOwner(operatorId, groupId)) {
            return GroupResult.fail(CODE_FORBIDDEN, "only the owner can transfer ownership");
        }
        if (!StringUtils.hasText(targetId) || targetId.equals(operatorId)) {
            return GroupResult.fail(CODE_BAD_REQUEST, "targetId must be another member");
        }
        if (members.roleOf(groupId, targetId) == null) {
            return GroupResult.fail(CODE_NOT_FOUND, "target is not a member of the group");
        }
        members.setRole(groupId, targetId, GroupRole.OWNER);
        GroupInfo updated = info.withOwner(targetId);
        groups.save(updated);
        members.setRole(groupId, operatorId, GroupRole.ADMIN);
        notifyChange(groupId, ClientFrames.GROUP_OWNER_TRANSFERRED, operatorId, targetId,
                groupAudience(groupId));
        log.info("[group] 转让群主 group={} from={} to={}", groupId, operatorId, targetId);
        return GroupResult.ok(updated);
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

    /**
     * 审批人：群主 + 全部管理员。
     *
     * <p>申请事件不能推「成员集合」：待审列表本身只有管理者能看，推给全体成员等于把
     * 「谁想进这个群」广播出去。审批人只能从角色表筛出来。
     */
    private List<String> approvers(String groupId) {
        List<String> result = new ArrayList<>();
        members.rolesOf(groupId).forEach((userId, role) -> {
            if (role != null && role.canManageGroup()) {
                result.add(userId);
            }
        });
        return result;
    }

    /**
     * 把人放进群：只做写入与人数校验，<b>不发事件</b>。
     *
     * <p>事件语义由调用方决定——审批通过是 {@code JOIN_APPROVED}，扫码入群是
     * {@code MEMBER_ADDED}。放在这里发会让一次入群推出两个语义不同的变更帧，
     * 客户端得靠猜来决定该刷新列表还是弹提示。
     */
    private GroupResult joinUser(GroupInfo info, String groupId, String userId) {
        if (members.isMember(groupId, userId)) {
            return GroupResult.ok(info);
        }
        if (members.memberCount(groupId) >= info.maxMembers()) {
            return GroupResult.fail(CODE_TOO_LARGE, "group is full (" + info.maxMembers() + ")");
        }
        members.addMembers(groupId, List.of(userId));
        return GroupResult.ok(info);
    }

    private GroupJoinStore joinStore() {
        return joins;
    }

    private static int pageSize(int limit) {
        return limit <= 0 ? DEFAULT_PAGE_SIZE : Math.min(limit, MAX_PAGE_SIZE);
    }

    /** 邀请码：10 位十六进制随机串，与群 ID 同一套生成方式（不可枚举即可，不需要密码学强度） */
    private static String newInviteCode() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, GroupInvite.CODE_LENGTH);
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

    /**
     * 成员列表的一行。
     *
     * @param mutedUntil 仍在禁言期内则为到期时间，否则 0
     */
    public record MemberView(String userId, GroupRole role, long mutedUntil) {
    }

    /**
     * 成员列表的一页。
     *
     * @param total   群成员总数（不是本页条数）
     * @param hasMore 后面还有没有下一页
     */
    public record MemberPage(int code, String message, List<MemberView> members,
                             int total, int offset, int limit, boolean hasMore) {

        static MemberPage fail(int code, String message) {
            return new MemberPage(code, message, List.of(), 0, 0, 0, false);
        }
    }

    /** 入群申请的结果 / 单条申请查询 */
    public record JoinRequestResult(int code, String message, GroupJoinRequest request) {

        static JoinRequestResult ok(GroupJoinRequest request) {
            return new JoinRequestResult(CODE_OK, "ok", request);
        }

        static JoinRequestResult fail(int code, String message) {
            return new JoinRequestResult(code, message, null);
        }
    }

    /** 待审申请的一页 */
    public record JoinRequestPage(int code, String message, List<GroupJoinRequest> requests,
                                  int total, int offset, int limit) {

        static JoinRequestPage fail(int code, String message) {
            return new JoinRequestPage(code, message, List.of(), 0, 0, 0);
        }
    }

    /**
     * 创建邀请码的结果。
     *
     * @param link 邀请链接：客户端可直接展示/复制，也可以据此生成二维码
     */
    public record InviteResult(int code, String message, GroupInvite invite, String link) {

        static InviteResult ok(GroupInvite invite, String link) {
            return new InviteResult(CODE_OK, "ok", invite, link);
        }

        static InviteResult fail(int code, String message) {
            return new InviteResult(code, message, null, null);
        }
    }

    /**
     * 群当前有效的邀请码。
     *
     * @param linkBase 链接前缀，客户端用它把每个 {@code code} 拼成完整链接/二维码
     */
    public record InvitePage(int code, String message, List<GroupInvite> invites, String linkBase) {

        static InvitePage fail(int code, String message) {
            return new InvitePage(code, message, List.of(), null);
        }
    }

    /**
     * 用邀请码加入的结果。
     *
     * @param approvalPending {@code true} 表示这个码需要审批，实际只是提交了一条申请
     */
    public record JoinByInviteResult(int code, String message, String groupId, GroupInfo group,
                                     boolean approvalPending) {

        static JoinByInviteResult ok(String groupId, GroupInfo group, boolean approvalPending) {
            return new JoinByInviteResult(CODE_OK, "ok", groupId, group, approvalPending);
        }

        static JoinByInviteResult fail(int code, String message) {
            return new JoinByInviteResult(code, message, null, null, false);
        }
    }
}
