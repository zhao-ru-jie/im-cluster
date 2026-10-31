package com.github.im.server.group;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 群治理规则的约定。
 *
 * <p>两个存储端口换成内存实现，因此<b>不需要 Redis/MySQL</b>：这里要锁的是「谁能做什么」，
 * 而不是 Redis 键长什么样——后者属于 {@code RedisGroupMemberRepository} 自己的关注点。
 *
 * <p>用例一一对应 {@link GroupService} 里那五条不可让步的规则：
 * <ol>
 *   <li>群主不能被踢、不能被禁言、不能被改角色；</li>
 *   <li>同级不能互踢（管理员不能踢管理员）；</li>
 *   <li>群主不能直接退群（409）；</li>
 *   <li>解散是逻辑删除：资料与成员都留着，消息一律拒绝；</li>
 *   <li>群 ID 不可枚举。</li>
 * </ol>
 *
 * <p>另外锁住一条容易被忽略的：{@code save} 是<b>整体覆盖</b>，
 * 所以任何一次治理动作都不能顺手把别的字段清空（改公告把群名刷掉这类事故）。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
class GroupServiceTest {

    private final InMemoryGroupRepository groups = new InMemoryGroupRepository();
    private final InMemoryGroupMemberRepository members = new InMemoryGroupMemberRepository();
    private final InMemoryGroupJoinStore joins = new InMemoryGroupJoinStore();
    /** notifier 传 null：这些用例只关心规则与状态，不需要下行事件 */
    private final GroupService service = new GroupService(groups, members, null, joins,
            "im://group/join?code=");

    // ==== 建群 ====

    @Test
    @DisplayName("建群：创建者即群主，群 ID 随机且不可枚举")
    void createMakesOwnerWithUnguessableId() {
        GroupService.GroupResult first = service.create("alice", "销售群", 0);
        GroupService.GroupResult second = service.create("alice", "销售群", 0);

        assertEquals(0, first.code(), "建群必须成功：" + first.message());
        String groupId = first.group().groupId();
        assertEquals(12, groupId.length(), "12 位 base36 才够挡住遍历式探测");
        assertNotEquals(groupId, second.group().groupId(), "群 ID 必须随机，自增等于把下一个群送出去");
        assertEquals(GroupRole.OWNER, members.roleOf(groupId, "alice"),
                "建群和「创建者是群主」必须是同一次写入的结果");
        assertEquals("销售群", first.group().name(), "群名要落进群资料，否则客户端无从展示");
    }

    @Test
    @DisplayName("人数上限：0 取默认值，超过硬边界被夹住")
    void createClampsMaxMembers() {
        String defaultLimit = service.create("alice", "g", 0).group().groupId();
        String overflow = service.create("alice", "g", 100_000).group().groupId();

        assertEquals(200, groups.find(defaultLimit).orElseThrow().maxMembers(),
                "0 是「没填」，不是「不准有人」");
        assertEquals(GroupInfo.MAX_MEMBERS_LIMIT, groups.find(overflow).orElseThrow().maxMembers(),
                "再大就不是群而是广播，扇出与成员表都撑不住");
    }

    // ==== 群资料 ====

    @Test
    @DisplayName("改名只有群主能做，且不冲掉公告与人数上限")
    void renameOnlyOwnerAndKeepsOtherFields() {
        String groupId = newGroup("alice", 0);
        members.addMembers(groupId, List.of("bob"));
        members.setRole(groupId, "bob", GroupRole.ADMIN);
        assertEquals(0, service.announce("alice", groupId, "周五团建").code());
        assertEquals(0, service.setMaxMembers("alice", groupId, 50).code());

        assertEquals(403, service.rename("bob", groupId, "新名字").code(),
                "管理员能管的已经够多了，改名留给群主");

        GroupService.GroupResult renamed = service.rename("alice", groupId, "新名字");
        assertEquals(0, renamed.code(), renamed.message());
        assertEquals("新名字", renamed.group().name());
        assertEquals("周五团建", renamed.group().announcement(), "save 是整体覆盖，改名不能把公告刷掉");
        assertEquals(50, renamed.group().maxMembers(), "同上：上限也不能被顺手覆盖回去");
    }

    @Test
    @DisplayName("公告：群主与管理员都能改，普通成员不能")
    void announcementForManagers() {
        String groupId = newGroup("alice", 0);
        members.addMembers(groupId, List.of("bob", "carol"));
        members.setRole(groupId, "bob", GroupRole.ADMIN);

        assertEquals(0, service.announce("bob", groupId, "管理员发的公告").code(),
                "改公告是最常被下放的一项，不该只有群主能做");
        assertEquals("管理员发的公告", groups.find(groupId).orElseThrow().announcement());
        assertEquals(403, service.announce("carol", groupId, "我要发公告").code());
    }

    @Test
    @DisplayName("人数上限只有群主能改，非法值直接 400")
    void maxMembersOnlyOwner() {
        String groupId = newGroup("alice", 0);
        members.addMembers(groupId, List.of("bob"));
        members.setRole(groupId, "bob", GroupRole.ADMIN);

        assertEquals(403, service.setMaxMembers("bob", groupId, 500).code(),
                "上限决定「谁能把这个群撑爆」，属于所有权动作");
        assertEquals(400, service.setMaxMembers("alice", groupId, 0).code(),
                "0 会被夹成默认值，治理动作里应当明确拒绝而不是悄悄改写");
    }

    // ==== 成员 ====

    @Test
    @DisplayName("拉人：去重后仍受上限约束，非成员不能拉人")
    void addMembersDedupsAndRespectsLimit() {
        String groupId = newGroup("alice", 2);

        assertEquals(403, service.addMembers("outsider", groupId, List.of("bob")).code(),
                "不在群里的人不能往里拉人，否则任何人都能往任意群塞人");

        // 群主 1 人 + 上限 2：同一个人重复出现只能算一个，否则就是 413
        assertEquals(0, service.addMembers("alice", groupId, List.of("bob", "bob")).code(),
                "重复 userId 必须先去重再算人数");

        assertEquals(413, service.addMembers("alice", groupId, List.of("carol")).code(),
                "满了就该 413，而不是先写进去再报警");
        assertEquals(2, members.membersOf(groupId).size(), "失败的动作不能留下半截成员");
    }

    @Test
    @DisplayName("踢人：管理员能踢普通成员，踢不动同级，更踢不动群主")
    void kickRequiresStrictlyHigherRole() {
        String groupId = newGroup("alice", 0);
        members.addMembers(groupId, List.of("bob", "carol", "dave"));
        members.setRole(groupId, "bob", GroupRole.ADMIN);
        members.setRole(groupId, "carol", GroupRole.ADMIN);

        assertEquals(403, service.kick("bob", groupId, "carol").code(),
                "同级互踢会让群在一天内变成无主状态");
        assertEquals(403, service.kick("bob", groupId, "alice").code(), "没人能踢群主");
        assertEquals(404, service.kick("bob", groupId, "outsider").code(),
                "踢一个本来就不在群里的人，先报 404 而不是 403");

        assertEquals(0, service.kick("bob", groupId, "dave").code());
        assertNull(members.roleOf(groupId, "dave"), "被踢之后不能残留成员资格");
        assertEquals("sender is not a member of group " + groupId, service.canSpeak(groupId, "dave"),
                "踢完还能发消息，等于没踢");
    }

    @Test
    @DisplayName("退群：群主 409，普通成员退完就不能发言")
    void leaveRejectsOwner() {
        String groupId = newGroup("alice", 0);
        members.addMembers(groupId, List.of("bob"));
        assertNull(service.canSpeak(groupId, "bob"), "前置条件：bob 现在能发言");

        assertEquals(409, service.leave("alice", groupId).code(),
                "群主直接退会留下没有 owner 的孤儿群，必须先解散或转让");

        assertEquals(0, service.leave("bob", groupId).code());
        assertEquals("sender is not a member of group " + groupId, service.canSpeak(groupId, "bob"));
    }

    @Test
    @DisplayName("设角色：只有群主，且不能直接设 OWNER（转让是另一个动作）")
    void setRoleOnlyOwnerAndNeverOwner() {
        String groupId = newGroup("alice", 0);
        members.addMembers(groupId, List.of("bob", "carol"));
        members.setRole(groupId, "bob", GroupRole.ADMIN);

        assertEquals(403, service.setRole("bob", groupId, "carol", GroupRole.ADMIN).code(),
                "管理员能设管理员，群主就失去了撤人的手段");
        assertEquals(400, service.setRole("alice", groupId, "carol", GroupRole.OWNER).code(),
                "转让要同时改 ownerId 和两个角色，混进 setRole 会出现两个「谁是群主」的真相来源");
        assertEquals(403, service.setRole("alice", groupId, "alice", GroupRole.MEMBER).code(),
                "群主不能把自己降级");

        assertEquals(0, service.setRole("alice", groupId, "carol", GroupRole.ADMIN).code());
        assertEquals(GroupRole.ADMIN, members.roleOf(groupId, "carol"));
        assertEquals(0, service.setRole("alice", groupId, "carol", GroupRole.MEMBER).code());
        assertEquals(GroupRole.MEMBER, members.roleOf(groupId, "carol"), "撤管理员必须真的降回来");
    }

    // ==== 禁言 ====

    @Test
    @DisplayName("单人禁言：到点之前不能发，seconds<=0 解除，群主禁不了")
    void muteAndUnmute() {
        String groupId = newGroup("alice", 0);
        members.addMembers(groupId, List.of("bob"));

        assertEquals(0, service.mute("alice", groupId, "bob", 600).code());
        assertTrue(service.canSpeak(groupId, "bob").startsWith("sender is muted until"),
                "只写进存储但准入不看，等于没禁");

        assertEquals(0, service.mute("alice", groupId, "bob", 0).code());
        assertNull(service.canSpeak(groupId, "bob"), "seconds<=0 是解除，不该继续拦");

        assertEquals(403, service.mute("alice", groupId, "alice", 600).code(), "群主不能被禁言");
        assertNull(service.canSpeak(groupId, "alice"), "群主自己必须永远能说话");
    }

    @Test
    @DisplayName("全员禁言：只拦普通成员，管理员照常发言")
    void muteAllBlocksMembersOnly() {
        String groupId = newGroup("alice", 0);
        members.addMembers(groupId, List.of("bob", "carol"));
        members.setRole(groupId, "bob", GroupRole.ADMIN);

        assertEquals(0, service.muteAll("bob", groupId, true).code(), "禁言是管理动作，管理员可以做");
        assertEquals("group is muted for all members", service.canSpeak(groupId, "carol"));
        assertNull(service.canSpeak(groupId, "bob"), "全员禁言把管理员也禁掉，群里就没人能维持秩序了");
        assertNull(service.canSpeak(groupId, "alice"));

        assertEquals(0, service.muteAll("bob", groupId, false).code());
        assertNull(service.canSpeak(groupId, "carol"));
    }

    @Test
    @DisplayName("个人禁言优先于全员禁言：拒绝原因要指向真正的那条")
    void personalMuteTakesPrecedence() {
        String groupId = newGroup("alice", 0);
        members.addMembers(groupId, List.of("bob"));
        assertEquals(0, service.mute("alice", groupId, "bob", 600).code());
        assertEquals(0, service.muteAll("alice", groupId, true).code());

        assertTrue(service.canSpeak(groupId, "bob").startsWith("sender is muted until"),
                "两处都命中时应当报更具体的那条，否则排查时只知道「群禁言了」");
    }

    // ==== 解散 ====

    @Test
    @DisplayName("解散是逻辑删除：成员与资料都留着，消息一律拒绝")
    void disbandIsLogicalDelete() {
        String groupId = newGroup("alice", 0);
        members.addMembers(groupId, List.of("bob"));

        assertEquals(403, service.disband("bob", groupId).code(), "只有群主能解散");

        GroupService.GroupResult result = service.disband("alice", groupId);
        assertEquals(0, result.code(), result.message());
        assertTrue(result.group().dismissed());
        assertTrue(groups.find(groupId).orElseThrow().dismissed(), "群资料必须还在：历史消息要可读");
        assertEquals(2, members.membersOf(groupId).size(), "成员集合保留，否则历史会话会直接消失");
        assertNotNull(service.detail(groupId).orElseThrow().roles().get("bob"),
                "detail 仍要能返回成员，客户端才能把会话显示成「群已解散」");
        assertEquals("group dismissed: " + groupId, service.canSpeak(groupId, "bob"));

        assertEquals(404, service.disband("alice", groupId).code(), "已解散的群再做治理动作一律 404");
        assertEquals(404, service.rename("alice", groupId, "复活").code());
    }

    // ==== 统一失败语义 ====

    @Test
    @DisplayName("群不存在：所有治理动作都是 404，而不是 500 或静默成功")
    void unknownGroupIsNotFound() {
        String nowhere = "nosuchgroup0";
        List<GroupService.GroupResult> results = List.of(
                service.rename("alice", nowhere, "x"),
                service.announce("alice", nowhere, "x"),
                service.muteAll("alice", nowhere, true),
                service.setMaxMembers("alice", nowhere, 10),
                service.addMembers("alice", nowhere, List.of("bob")),
                service.kick("alice", nowhere, "bob"),
                service.leave("alice", nowhere),
                service.setRole("alice", nowhere, "bob", GroupRole.ADMIN),
                service.mute("alice", nowhere, "bob", 60),
                service.disband("alice", nowhere));

        for (GroupService.GroupResult result : results) {
            assertEquals(404, result.code(), "群不存在必须是 404：" + result.message());
            assertNull(result.group(), "失败结果不该带一份编造的群资料");
        }
        assertTrue(service.detail(nowhere).isEmpty());
        // 准入按「非成员 → 群不存在 → 禁言」判：连成员都不是时先报非成员，
        // 顺便避免「群存在与否」从拒绝原因里漏出去
        assertEquals("sender is not a member of group " + nowhere, service.canSpeak(nowhere, "alice"));
    }

    // ==== 成员列表分页 ====

    @Test
    @DisplayName("成员分页：按 userId 字典序、total 是全量、hasMore 正确")
    void membersArePagedWithStableOrder() {
        String groupId = newGroup("alice", 0);
        service.addMembers("alice", groupId, List.of("bob", "carol", "dave", "erin"));

        GroupService.MemberPage first = service.members(groupId, 0, 2);
        assertEquals(0, first.code(), first.message());
        assertEquals(5, first.total(), "total 是群人数，不是本页条数");
        assertEquals(2, first.members().size());
        assertTrue(first.hasMore());
        assertEquals(List.of("alice", "bob"), first.members().stream()
                        .map(GroupService.MemberView::userId).toList(),
                "顺序必须稳定（userId 字典序），否则 offset 翻页会重复或漏人");
        assertEquals(GroupRole.OWNER, first.members().get(0).role());

        GroupService.MemberPage tail = service.members(groupId, 4, 2);
        assertEquals(1, tail.members().size());
        assertFalse(tail.hasMore());

        GroupService.MemberPage beyond = service.members(groupId, 99, 2);
        assertTrue(beyond.members().isEmpty());
        assertEquals(5, beyond.total(), "越界的页也要能报告总人数");
    }

    @Test
    @DisplayName("成员页只带本页的禁言状态")
    void memberPageCarriesMutes() {
        String groupId = newGroup("alice", 0);
        service.addMembers("alice", groupId, List.of("bob"));
        assertEquals(0, service.mute("alice", groupId, "bob", 600).code());

        GroupService.MemberPage page = service.members(groupId, 0, 10);
        Map<String, Long> mutes = new HashMap<>();
        page.members().forEach(member -> mutes.put(member.userId(), member.mutedUntil()));
        assertTrue(mutes.get("bob") > 0, "被禁言的人要带上到期时间");
        assertEquals(0L, mutes.get("alice"), "没被禁言的人是 0");
    }

    // ==== 入群申请 ====

    @Test
    @DisplayName("入群申请：提交 → 待审 → 通过后成为成员；重复审批按已处理返回")
    void joinRequestApproveFlow() {
        String groupId = newGroup("alice", 0);
        assertEquals(0, service.requestJoin(groupId, "bob", "想进群").code());

        GroupService.JoinRequestPage pending = service.pendingJoins("alice", groupId, 0, 10);
        assertEquals(1, pending.total());
        assertEquals("bob", pending.requests().get(0).applicantId());
        assertEquals(GroupJoinRequest.STATUS_PENDING, pending.requests().get(0).status());

        assertEquals(0, service.approveJoin("alice", groupId, "bob").code());
        assertTrue(members.isMember(groupId, "bob"));
        assertEquals(404, service.approveJoin("alice", groupId, "bob").code(),
                "多管理员同时审批会撞车，第二次必须按已处理回");
        assertEquals(0, service.pendingJoins("alice", groupId, 0, 10).total());
    }

    @Test
    @DisplayName("重复申请不堆条目，也不刷新首次申请时间")
    void repeatedJoinRequestIsIdempotent() {
        String groupId = newGroup("alice", 0);
        service.requestJoin(groupId, "bob", "第一次");
        long createdAt = joins.findRequest(groupId, "bob").createdAt();

        service.requestJoin(groupId, "bob", "第二次");
        GroupService.JoinRequestPage pending = service.pendingJoins("alice", groupId, 0, 10);
        assertEquals(1, pending.total(), "同一个人只能有一条申请");
        assertEquals(createdAt, pending.requests().get(0).createdAt(), "排序依据不能被刷新");
        assertEquals("第二次", pending.requests().get(0).message(), "附言倒是应该更新");
    }

    @Test
    @DisplayName("待审列表只有群主/管理员能看：普通成员 403")
    void pendingJoinsRequireManager() {
        String groupId = newGroup("alice", 0);
        service.addMembers("alice", groupId, List.of("bob"));
        service.requestJoin(groupId, "dave", "");

        assertEquals(403, service.pendingJoins("bob", groupId, 0, 10).code(),
                "「谁想进这个群」是隐私，不能给普通成员看");
        assertEquals(0, service.pendingJoins("alice", groupId, 0, 10).code());
    }

    @Test
    @DisplayName("满员时直接拒绝申请，不让人白等")
    void joinRequestRejectedWhenGroupIsFull() {
        String groupId = newGroup("alice", 2);
        service.addMembers("alice", groupId, List.of("bob"));

        assertEquals(413, service.requestJoin(groupId, "dave", "").code());
        assertEquals(0, service.pendingJoins("alice", groupId, 0, 10).total(),
                "进不来就别留一条永远处理不了的申请");
    }

    @Test
    @DisplayName("已是成员还申请 → 409")
    void joinRequestFromExistingMemberIsConflict() {
        String groupId = newGroup("alice", 0);
        service.addMembers("alice", groupId, List.of("bob"));
        assertEquals(409, service.requestJoin(groupId, "bob", "").code());
    }

    // ==== 邀请码（邀请链接 / 二维码） ====

    @Test
    @DisplayName("邀请码：管理员建的码直接入群；用满次数后 410，未知码 404")
    void inviteCodeJoinsDirectlyAndStopsAtLimit() {
        String groupId = newGroup("alice", 0);
        GroupService.InviteResult created = service.createInvite("alice", groupId, 24, 1, false);
        assertEquals(0, created.code(), created.message());
        assertTrue(created.link().endsWith(created.invite().code()), "链接要能直接给出去/画二维码");
        String code = created.invite().code();

        GroupService.JoinByInviteResult joined = service.joinByInvite("bob", code);
        assertEquals(0, joined.code(), joined.message());
        assertFalse(joined.approvalPending());
        assertTrue(members.isMember(groupId, "bob"));

        assertEquals(410, service.joinByInvite("carol", code).code(), "限 1 次的码不能再用");
        assertEquals(404, service.joinByInvite("dave", "not-a-code").code());
    }

    @Test
    @DisplayName("普通成员建的邀请码强制要求审批：用码只是提交申请")
    void memberCreatedInviteRequiresApproval() {
        String groupId = newGroup("alice", 0);
        service.addMembers("alice", groupId, List.of("bob"));

        GroupService.InviteResult created = service.createInvite("bob", groupId, 0, 1, false);
        assertEquals(0, created.code(), created.message());
        assertTrue(created.invite().approvalRequired(),
                "否则普通成员能生成一条「点一下就把人放进来」的链接，直接绕过管理员");

        GroupService.JoinByInviteResult joined = service.joinByInvite("carol", created.invite().code());
        assertEquals(0, joined.code());
        assertTrue(joined.approvalPending());
        assertFalse(members.isMember(groupId, "carol"), "需要审批的码不等于入场券");
        assertEquals(1, service.pendingJoins("alice", groupId, 0, 10).total());

        // 限 1 次的码：审批流也要计数，否则它会被无限次用来「敲门」
        assertEquals(410, service.joinByInvite("dave", created.invite().code()).code());
    }

    @Test
    @DisplayName("吊销邀请码后立刻失效；已在群里的成员用码是幂等成功且不消耗次数")
    void revokeInviteAndIdempotentJoin() {
        String groupId = newGroup("alice", 0);
        String code = service.createInvite("alice", groupId, 0, 1, false).invite().code();

        assertEquals(0, service.joinByInvite("bob", code).code());
        // bob 已是成员：再来一次算成功，但不能再扣一次名额（名额已被第一次用掉）
        GroupService.JoinByInviteResult again = service.joinByInvite("bob", code);
        assertEquals(0, again.code(), "已在群里应当幂等成功，而不是报「码已用尽」");

        assertEquals(0, service.revokeInvite("alice", groupId, code).code());
        assertEquals(404, service.joinByInvite("carol", code).code(), "吊销后按不存在处理");
        assertTrue(service.invites("alice", groupId).invites().isEmpty());
    }

    @Test
    @DisplayName("邀请码列表与吊销只有群主/管理员能做")
    void inviteManagementRequiresManager() {
        String groupId = newGroup("alice", 0);
        service.addMembers("alice", groupId, List.of("bob"));
        String code = service.createInvite("alice", groupId, 0, 0, false).invite().code();

        assertEquals(403, service.invites("bob", groupId).code());
        assertEquals(403, service.revokeInvite("bob", groupId, code).code());
    }

    // ==== 群主转让 ====

    @Test
    @DisplayName("转让群主：新群主 OWNER、原群主降 ADMIN、群资料同步，且解锁「原群主退群」")
    void transferOwnershipMovesRolesAndProfile() {
        String groupId = newGroup("alice", 0);
        service.addMembers("alice", groupId, List.of("bob", "carol"));

        assertEquals(0, service.transferOwnership("alice", groupId, "bob").code());
        assertEquals(GroupRole.OWNER, members.roleOf(groupId, "bob"));
        assertEquals(GroupRole.ADMIN, members.roleOf(groupId, "alice"),
                "降成普通成员会让他瞬间失去管理能力，通常是误操作");
        assertEquals("bob", groups.find(groupId).orElseThrow().ownerId(),
                "群资料的 ownerId 是转让的唯一权威，必须同步");

        // 转让之前这条路是 409（群不能没有主）
        assertEquals(0, service.leave("alice", groupId).code());
    }

    @Test
    @DisplayName("转让边界：只有群主能转、只能转给成员、不能转给自己；roles 接口仍不能造群主")
    void transferOwnershipGuardRails() {
        String groupId = newGroup("alice", 0);
        service.addMembers("alice", groupId, List.of("bob"));

        assertEquals(403, service.transferOwnership("bob", groupId, "alice").code());
        assertEquals(404, service.transferOwnership("alice", groupId, "stranger").code());
        assertEquals(400, service.transferOwnership("alice", groupId, "alice").code());
        assertEquals(400, service.setRole("alice", groupId, "bob", GroupRole.OWNER).code(),
                "设角色仍然不能造出第二个群主——转让是唯一入口");
        assertEquals(GroupRole.OWNER, members.roleOf(groupId, "alice"), "失败的转让不能留下副作用");
    }

    // ==== 辅助 ====

    private String newGroup(String ownerId, int maxMembers) {
        GroupService.GroupResult result = service.create(ownerId, "测试群", maxMembers);
        assertEquals(0, result.code(), "前置条件：建群失败 " + result.message());
        return result.group().groupId();
    }

    /** 内存版群资料存储：{@code save} 整体覆盖，与真实实现同语义 */
    private static final class InMemoryGroupRepository implements GroupRepository {

        private final Map<String, GroupInfo> store = new HashMap<>();

        @Override
        public Optional<GroupInfo> find(String groupId) {
            return Optional.ofNullable(store.get(groupId));
        }

        @Override
        public void save(GroupInfo info) {
            store.put(info.groupId(), info);
        }

        @Override
        public boolean dismiss(String groupId) {
            GroupInfo info = store.get(groupId);
            if (info == null || info.dismissed()) {
                return false;
            }
            store.put(groupId, info.dismissedAt(System.currentTimeMillis()));
            return true;
        }
    }

    /**
     * 内存版成员存储。
     *
     * <p>刻意复刻 {@code RedisGroupMemberRepository} 的两处语义：非成员的角色是 {@code null}
     * （而不是 MEMBER），以及移出成员时角色与禁言一起清——后者漏了就会出现
     * 「被踢掉的管理员再加回来直接带着管理员权限」这条越权路径。
     */
    private static final class InMemoryGroupMemberRepository implements GroupMemberRepository {

        private final Map<String, Set<String>> members = new HashMap<>();
        private final Map<String, Map<String, GroupRole>> roles = new HashMap<>();
        private final Map<String, Map<String, Long>> mutes = new HashMap<>();

        @Override
        public void addMembers(String groupId, Collection<String> userIds) {
            if (userIds == null || userIds.isEmpty()) {
                return;
            }
            members.computeIfAbsent(groupId, key -> new LinkedHashSet<>()).addAll(userIds);
        }

        @Override
        public void removeMember(String groupId, String userId) {
            Set<String> inGroup = members.get(groupId);
            if (inGroup != null) {
                inGroup.remove(userId);
            }
            Map<String, GroupRole> roleByUser = roles.get(groupId);
            if (roleByUser != null) {
                roleByUser.remove(userId);
            }
            Map<String, Long> muteByUser = mutes.get(groupId);
            if (muteByUser != null) {
                muteByUser.remove(userId);
            }
        }

        @Override
        public List<String> membersOf(String groupId) {
            return List.copyOf(members.getOrDefault(groupId, Set.of()));
        }

        @Override
        public List<String> groupsOf(String userId) {
            List<String> result = new ArrayList<>();
            members.forEach((groupId, inGroup) -> {
                if (inGroup.contains(userId)) {
                    result.add(groupId);
                }
            });
            return result;
        }

        @Override
        public boolean isMember(String groupId, String userId) {
            return userId != null && members.getOrDefault(groupId, Set.of()).contains(userId);
        }

        @Override
        public GroupRole roleOf(String groupId, String userId) {
            if (!isMember(groupId, userId)) {
                return null;
            }
            return roles.getOrDefault(groupId, Map.of()).getOrDefault(userId, GroupRole.MEMBER);
        }

        @Override
        public void setRole(String groupId, String userId, GroupRole role) {
            if (userId == null || role == null) {
                return;
            }
            Map<String, GroupRole> roleByUser = roles.computeIfAbsent(groupId, key -> new HashMap<>());
            if (role == GroupRole.MEMBER) {
                // MEMBER 是缺省值，不存：否则这张表会随群人数线性增长
                roleByUser.remove(userId);
            } else {
                roleByUser.put(userId, role);
            }
        }

        @Override
        public Map<String, GroupRole> rolesOf(String groupId) {
            Map<String, GroupRole> result = new HashMap<>();
            // 只回填仍在群里的：角色表可能残留已退群的人，不该凭空造出成员
            for (String userId : membersOf(groupId)) {
                result.put(userId,
                        roles.getOrDefault(groupId, Map.of()).getOrDefault(userId, GroupRole.MEMBER));
            }
            return result;
        }

        @Override
        public long mutedUntil(String groupId, String userId) {
            Long until = mutes.getOrDefault(groupId, Map.of()).get(userId);
            if (until == null || until <= System.currentTimeMillis()) {
                return 0L;
            }
            return until;
        }

        @Override
        public void mute(String groupId, String userId, long untilMillis) {
            if (userId == null) {
                return;
            }
            Map<String, Long> muteByUser = mutes.computeIfAbsent(groupId, key -> new HashMap<>());
            if (untilMillis <= System.currentTimeMillis()) {
                muteByUser.remove(userId);
            } else {
                muteByUser.put(userId, untilMillis);
            }
        }
    }

    /**
     * 内存版入群存储。
     *
     * <p>复刻 {@code RedisGroupJoinStore} 的两处关键语义：申请按 {@code (groupId, applicantId)}
     * 覆盖（同一人只有一条）、邀请码取用<b>先判后增</b>（限量码用满即拒）。
     */
    private static final class InMemoryGroupJoinStore implements GroupJoinStore {

        private final Map<String, Map<String, GroupJoinRequest>> requests = new HashMap<>();
        private final Map<String, GroupInvite> invites = new HashMap<>();

        @Override
        public void saveRequest(GroupJoinRequest request) {
            requests.computeIfAbsent(request.groupId(), key -> new HashMap<>())
                    .put(request.applicantId(), request);
        }

        @Override
        public GroupJoinRequest findRequest(String groupId, String applicantId) {
            return requests.getOrDefault(groupId, Map.of()).get(applicantId);
        }

        @Override
        public List<GroupJoinRequest> pendingRequests(String groupId) {
            List<GroupJoinRequest> pending = new ArrayList<>();
            for (GroupJoinRequest request : requests.getOrDefault(groupId, Map.of()).values()) {
                if (request.isPending()) {
                    pending.add(request);
                }
            }
            pending.sort(Comparator.comparingLong(GroupJoinRequest::createdAt)
                    .thenComparing(GroupJoinRequest::applicantId));
            return pending;
        }

        @Override
        public void deleteRequest(String groupId, String applicantId) {
            Map<String, GroupJoinRequest> byUser = requests.get(groupId);
            if (byUser != null) {
                byUser.remove(applicantId);
            }
        }

        @Override
        public void saveInvite(GroupInvite invite) {
            invites.put(invite.code(), invite);
        }

        @Override
        public GroupInvite findInvite(String code) {
            return invites.get(code);
        }

        @Override
        public List<GroupInvite> invitesOf(String groupId) {
            long now = System.currentTimeMillis();
            List<GroupInvite> result = new ArrayList<>();
            for (GroupInvite invite : invites.values()) {
                if (invite.groupId().equals(groupId) && invite.usable(now)) {
                    result.add(invite);
                }
            }
            result.sort(Comparator.comparingLong(GroupInvite::createdAt).reversed());
            return result;
        }

        @Override
        public void revokeInvite(String groupId, String code) {
            invites.remove(code);
        }

        @Override
        public InviteUse useInvite(String code, long now) {
            GroupInvite invite = invites.get(code);
            if (invite == null) {
                return InviteUse.of(InviteUse.Status.NOT_FOUND, null);
            }
            if (invite.expired(now)) {
                return InviteUse.of(InviteUse.Status.EXPIRED, invite);
            }
            if (invite.exhausted()) {
                return InviteUse.of(InviteUse.Status.EXHAUSTED, invite);
            }
            GroupInvite used = invite.withUses(invite.uses() + 1);
            invites.put(code, used);
            return InviteUse.of(InviteUse.Status.OK, used);
        }
    }
}
