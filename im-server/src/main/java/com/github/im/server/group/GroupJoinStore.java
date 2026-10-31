package com.github.im.server.group;

import java.util.List;

/**
 * 入群申请与邀请码的存储端口。
 *
 * <h2>为什么它不跟着 {@code im.storage} 切介质</h2>
 *
 * <p>{@link GroupRepository} / {@link GroupMemberRepository} 存的是<b>领域真相</b>（这个群叫什么、
 * 谁是成员），换 MySQL 或 Mongo 都不改变语义。本端口存的是<b>短生命周期协作状态</b>：
 * 待审申请和邀请码都会过期（默认 7 天），过期即无意义——和离线队列、客户端水位、seq 发号同类，
 * 那些在这套系统里一律只落在 Redis。把它们塞进业务库，代价是要为一个「7 天后自己消失」的表
 * 配一个清理任务，而清理任务是最容易没人维护的组件。
 *
 * <h2>申请：按群存的 Hash</h2>
 *
 * <p>审批人关心的是「这个群有哪些人在等」，所以正查是群维度；申请人查自己的状态时手里也有
 * {@code groupId}（他刚刚申请过），因此不需要反向索引——少一张表，也少一次双写不一致的机会。
 *
 * <h2>邀请码：码是唯一的入口</h2>
 *
 * <p>扫码的人手里只有码，所以 {@link #findInvite} / {@link #useInvite} 必须能<b>只凭码</b>
 * 一次定位。{@link #invitesOf} 是给管理员看「我这个群放出去了哪些链接」的辅助索引。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public interface GroupJoinStore {

    // ==== 入群申请 ====

    /** 覆盖写入一条申请（按 groupId + applicantId 唯一定位）：重复申请不会堆出多条 */
    void saveRequest(GroupJoinRequest request);

    /** @return 不存在返回 {@code null} */
    GroupJoinRequest findRequest(String groupId, String applicantId);

    /** 该群的待审申请，按 {@code createdAt} 升序（先来先审） */
    List<GroupJoinRequest> pendingRequests(String groupId);

    /** 删除一条申请（撤回 / 审批完成后清理）；不存在时静默成功 */
    void deleteRequest(String groupId, String applicantId);

    // ==== 邀请码 ====

    /** 写入（或覆盖）一个邀请码；过期时间由 {@code invite.expiresAt()} 决定 */
    void saveInvite(GroupInvite invite);

    /** @return 不存在或已被吊销返回 {@code null} */
    GroupInvite findInvite(String code);

    /** 该群当前仍有效的邀请码（已过期的不返回） */
    List<GroupInvite> invitesOf(String groupId);

    /** 吊销：码立即失效，并从群的索引里摘掉 */
    void revokeInvite(String groupId, String code);

    /**
     * 原子取用一次邀请码：校验有效期与次数上限，成功时把 {@code uses} 加一。
     *
     * <p><b>必须原子</b>：{@code maxUses=1} 的链接在并发扫码下若走「读-判-写」，
     * 两个请求都会看到 {@code uses=0} 而双双通过，限量就形同虚设。
     */
    InviteUse useInvite(String code, long now);

    /**
     * 取用结果。
     *
     * @param status {@link Status#OK} 时 {@code invite} 为已自增后的最新形态
     */
    record InviteUse(Status status, GroupInvite invite) {

        public enum Status {
            /** 校验通过，次数已自增 */
            OK,
            /** 码不存在或已被吊销 */
            NOT_FOUND,
            /** 已过期 */
            EXPIRED,
            /** 次数用尽 */
            EXHAUSTED
        }

        public static InviteUse of(Status status, GroupInvite invite) {
            return new InviteUse(status, invite);
        }

        public boolean ok() {
            return status == Status.OK;
        }
    }
}
