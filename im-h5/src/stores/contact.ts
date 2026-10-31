import { defineStore } from 'pinia';
import { ApiError } from '@/api/http';
import {
  DeviceApi,
  FriendRequestApi,
  GroupApi,
  GroupJoinApi,
  PresenceApi,
  RelationApi,
  type DeviceListResponse
} from '@/api/rest';
import { imSocket } from '@/api/ws';
import { cacheProfile, cachedProfiles } from '@/core/db';
import { groupIdOfConversation } from '@/core/format';
import { beep, vibrate } from '@/core/notify';
import type { UserProfile } from '@/types/local';
import type {
  DownlinkFrame,
  FriendRequestItem,
  GroupInfo,
  GroupInviteItem,
  GroupJoinRequestItem,
  GroupRole,
  GroupUpdateFrame,
  PresenceEntry,
  PresencePushFrame,
  PresenceSnapshotFrame,
  RelationRequestFrame,
  RelationUpdateFrame
} from '@/types/protocol';
import { useAuthStore } from './auth';
import { useImStore } from './im';
import { useUiStore } from './ui';

interface GroupMembers {
  members: string[];
  roles: Record<string, GroupRole>;
  mutes: Record<string, number>;
  size: number;
}

/** 申请列表顺序：最近变动的排前面（与服务端 {@code order by updated_at desc} 一致） */
function sortRequests(list: FriendRequestItem[]): FriendRequestItem[] {
  return [...list].sort((a, b) => b.updatedAt - a.updatedAt);
}

export const useContactStore = defineStore('contact', {
  state: () => ({
    friends: [] as string[],
    blocks: [] as string[],
    /** 收到的 / 发出的好友申请（服务端是权威源，这里只是它的一份镜像） */
    incoming: [] as FriendRequestItem[],
    outgoing: [] as FriendRequestItem[],
    requestsLoaded: false,
    /** userId -> 在线状态 */
    presence: {} as Record<string, PresenceEntry>,
    /** userId -> 本地资料（服务端没有资料接口，昵称头像只在本地） */
    profiles: {} as Record<string, UserProfile>,
    groups: {} as Record<string, GroupInfo>,
    groupMembers: {} as Record<string, GroupMembers>,
    /** groupId -> 待审入群申请（只有群主/管理员拿得到；未加载过的群不在表里） */
    joinRequests: {} as Record<string, GroupJoinRequestItem[]>,
    /** groupId -> 待审总数（分页接口的 total，可能大于本地这一页） */
    joinRequestTotals: {} as Record<string, number>,
    /** groupId -> 当前有效邀请码 */
    invites: {} as Record<string, GroupInviteItem[]>,
    /** 邀请链接前缀，由服务端下发（`im://group/join` 或 https 落地页） */
    inviteLinkBase: '',
    /** groupId -> 我自己在这群里的申请（null = 没有申请过） */
    myJoinRequests: {} as Record<string, GroupJoinRequestItem | null>,
    devices: [] as DeviceListResponse['devices'],
    devicesControllable: true,
    loading: false,
    bound: false
  }),

  getters: {
    friendCount: (state): number => state.friends.length,
    onlineFriends(state): string[] {
      return state.friends.filter((id) => state.presence[id]?.online);
    },
    /** 待我处理的申请：红点数字就用它的长度 */
    pendingIncoming(state): FriendRequestItem[] {
      return state.incoming.filter((item) => item.status === 'PENDING');
    },
    pendingIncomingCount(): number {
      return this.pendingIncoming.length;
    },
    /** 我发出、对方还没处理的申请 */
    pendingOutgoing(state): FriendRequestItem[] {
      return state.outgoing.filter((item) => item.status === 'PENDING');
    },
    displayName: (state) => (userId: string): string => {
      if (!userId) return '';
      return state.profiles[userId]?.nickname || userId;
    },
    avatarOf: (state) => (userId: string): string | undefined => state.profiles[userId]?.avatar,
    presenceOf: (state) => (userId: string): PresenceEntry | undefined => state.presence[userId],
    isOnline: (state) => (userId: string): boolean => !!state.presence[userId]?.online,
    groupOf: (state) => (groupId: string): GroupInfo | undefined => state.groups[groupId],
    /** 群在会话列表里的名字：优先真实群名，其次「群 {id}」 */
    groupNameOf: (state) => (conversationId: string): string => {
      const groupId = groupIdOfConversation(conversationId);
      return state.groups[groupId]?.name || `群 ${groupId}`;
    },
    membersOf: (state) => (groupId: string): GroupMembers | undefined => state.groupMembers[groupId],
    myRoleIn: (state) => (groupId: string): GroupRole => {
      const auth = useAuthStore();
      const roles = state.groupMembers[groupId]?.roles;
      if (!roles) return 'MEMBER';
      return roles[auth.userId] ?? 'MEMBER';
    },
    /** 群主或管理员才能看待审列表 / 管邀请码（服务端用同一个判定） */
    canManageGroup: (state) => (groupId: string): boolean => {
      const auth = useAuthStore();
      const role = state.groupMembers[groupId]?.roles?.[auth.userId];
      return role === 'OWNER' || role === 'ADMIN';
    },
    pendingJoinsOf: (state) => (groupId: string): GroupJoinRequestItem[] =>
      state.joinRequests[groupId] ?? [],
    invitesOf: (state) => (groupId: string): GroupInviteItem[] => state.invites[groupId] ?? [],
    myJoinRequestOf: (state) => (groupId: string): GroupJoinRequestItem | null | undefined =>
      state.myJoinRequests[groupId]
  },

  actions: {
    /** 注册帧监听（presence 推送），只注册一次 */
    bindFrames(): void {
      if (this.bound) return;
      this.bound = true;
      imSocket.onFrame((frame: DownlinkFrame) => {
        const type = (frame as { type?: string }).type;
        if (type === 'presence.push') {
          const push = frame as PresencePushFrame;
          this.applyPresence(push.userId, push.online, push.ts);
        } else if (type === 'presence.snapshot') {
          const snapshot = frame as PresenceSnapshotFrame;
          (snapshot.states ?? []).forEach((state) => this.applyPresence(state.userId, state.online, state.lastSeen, state));
        } else if (type === 'relation.request') {
          this.onRelationRequest(frame as RelationRequestFrame);
        } else if (type === 'relation.update') {
          this.onRelationUpdate(frame as RelationUpdateFrame);
        }
      });
    },

    /**
     * 有人申请加我：先插进列表（红点立刻出现），再响一声。
     *
     * <p>不在这里重新拉接口：推送帧已经带齐了渲染一条申请所需的全部字段，
     * 为它再打一次 HTTP 只会让「提示」慢一个往返；列表的权威刷新留给 {@link #loadRequests}。
     */
    onRelationRequest(push: RelationRequestFrame): void {
      const auth = useAuthStore();
      this.upsertRequest('incoming', {
        fromId: push.fromId,
        toId: auth.userId,
        status: 'PENDING',
        message: push.message ?? '',
        createdAt: push.ts,
        updatedAt: push.ts
      });
      beep();
      vibrate([20, 40, 20]);
      useUiStore().toast(`${this.displayName(push.fromId)} 申请添加你为好友`, 'info', 3000);
    },

    /**
     * 申请被处理。
     *
     * <p>{@code ACCEPTED} 时必须重拉好友列表：好友关系是服务端写的，本地只有一份镜像，
     * 不重拉就表现为「对方已同意，但通讯录里还看不到他」。
     */
    onRelationUpdate(update: RelationUpdateFrame): void {
      const name = this.displayName(update.peerId) || update.peerId;
      if (update.change === 'ACCEPTED') {
        this.markOutgoing(update.peerId, 'ACCEPTED');
        void this.loadFriends();
        useUiStore().success(`${name} 通过了你的好友申请`);
        return;
      }
      if (update.change === 'REJECTED') {
        this.markOutgoing(update.peerId, 'REJECTED');
        useUiStore().toast(`${name} 拒绝了你的好友申请`);
        return;
      }
      // CANCELLED：申请人撤回了，我列表里那条要消失
      this.incoming = this.incoming.filter((item) => item.fromId !== update.peerId);
    },

    applyPresence(userId: string, online: boolean, lastSeen = 0, raw?: PresenceEntry): void {
      const previous = this.presence[userId];
      this.presence[userId] = raw ?? {
        userId,
        online,
        gateways: previous?.gateways ?? 0,
        devices: previous?.devices ?? 0,
        lastSeen: lastSeen || previous?.lastSeen || 0
      };
    },

    async loadProfiles(): Promise<void> {
      this.profiles = await cachedProfiles();
    },

    async saveLocalProfile(profile: UserProfile): Promise<void> {
      this.profiles[profile.userId] = profile;
      await cacheProfile(profile);
    },

    /* ------------------------------ 好友 ------------------------------ */

    async loadFriends(): Promise<void> {
      const auth = useAuthStore();
      if (!auth.userId) return;
      try {
        const data = await RelationApi.friends(auth.userId);
        this.friends = data?.friends ?? [];
      } catch (e) {
        useUiStore().error((e as ApiError).message || '好友列表加载失败');
      }
    },

    /* ----------------------------- 好友申请 ----------------------------- */

    /** 拉取「收到的 + 发出的」申请（一个接口拿两屏，避免弱网下的半成品列表） */
    async loadRequests(): Promise<void> {
      const auth = useAuthStore();
      if (!auth.userId) return;
      try {
        const data = await FriendRequestApi.list(auth.userId);
        this.incoming = sortRequests(data?.incoming ?? []);
        this.outgoing = sortRequests(data?.outgoing ?? []);
        this.requestsLoaded = true;
      } catch (e) {
        useUiStore().error((e as ApiError).message || '好友申请加载失败');
      }
    },

    /**
     * 发送好友申请。
     *
     * <p>不再有「直接加好友」：对方同意之前，双方都进不了对方的好友列表。
     * 服务端返回的 {@code message} 优先展示（「已发送过申请」「你们已经是好友了」），
     * 这些文案比前端硬编码的准。
     */
    async sendFriendRequest(friendId: string, message = ''): Promise<boolean> {
      const auth = useAuthStore();
      const id = friendId.trim();
      if (!id || id === auth.userId) {
        useUiStore().error('请输入对方 userId（不能是自己）');
        return false;
      }
      try {
        const data = await FriendRequestApi.send(auth.userId, id, message.trim());
        if (data?.request) {
          this.upsertRequest('outgoing', data.request);
        }
        // 「对方也在申请你」时服务端已经互加，好友列表要跟着变
        if (data?.friends?.length) {
          this.friends = data.friends;
        }
        useUiStore().success(data?.message || '好友申请已发送');
        return true;
      } catch (e) {
        useUiStore().error((e as ApiError).message || '发送申请失败');
        return false;
      }
    },

    /** 同意：服务端会写双向好友边并通知申请人 */
    async acceptRequest(fromId: string): Promise<void> {
      const auth = useAuthStore();
      try {
        const data = await FriendRequestApi.accept(auth.userId, fromId);
        if (data?.friends) {
          this.friends = data.friends;
        }
        this.markIncoming(fromId, 'ACCEPTED');
        useUiStore().success(data?.message || '已添加为好友');
      } catch (e) {
        useUiStore().error((e as ApiError).message || '操作失败');
      }
    },

    async rejectRequest(fromId: string): Promise<void> {
      const auth = useAuthStore();
      try {
        const data = await FriendRequestApi.reject(auth.userId, fromId);
        this.markIncoming(fromId, 'REJECTED');
        useUiStore().toast(data?.message || '已拒绝该申请');
      } catch (e) {
        useUiStore().error((e as ApiError).message || '操作失败');
      }
    },

    /** 撤回自己发出的申请（只有 PENDING 能撤） */
    async cancelRequest(toId: string): Promise<void> {
      const auth = useAuthStore();
      try {
        await FriendRequestApi.cancel(auth.userId, toId);
        this.outgoing = this.outgoing.filter((item) => item.toId !== toId);
        useUiStore().toast('已撤回申请');
      } catch (e) {
        useUiStore().error((e as ApiError).message || '撤回失败');
      }
    },

    /** 按 (fromId) 插入或覆盖收到的申请 */
    upsertRequest(bucket: 'incoming' | 'outgoing', item: FriendRequestItem): void {
      const key = bucket === 'incoming' ? 'fromId' : 'toId';
      const list = this[bucket].filter((existing) => existing[key] !== item[key]);
      list.push(item);
      this[bucket] = sortRequests(list);
    },

    markIncoming(fromId: string, status: FriendRequestItem['status']): void {
      this.incoming = this.incoming.map((item) =>
        item.fromId === fromId ? { ...item, status, updatedAt: Date.now() } : item
      );
    },

    markOutgoing(toId: string, status: FriendRequestItem['status']): void {
      this.outgoing = this.outgoing.map((item) =>
        item.toId === toId ? { ...item, status, updatedAt: Date.now() } : item
      );
    },

    async removeFriend(friendId: string): Promise<void> {
      const auth = useAuthStore();
      try {
        const data = await RelationApi.removeFriend(auth.userId, friendId);
        this.friends = data.friends ?? this.friends.filter((id) => id !== friendId);
        useUiStore().success('已删除好友');
      } catch (e) {
        useUiStore().error((e as ApiError).message || '删除失败');
      }
    },

    async loadBlocks(): Promise<void> {
      const auth = useAuthStore();
      try {
        const data = await RelationApi.blocks(auth.userId);
        this.blocks = data?.blocked ?? [];
      } catch (e) {
        useUiStore().error((e as ApiError).message || '黑名单加载失败');
      }
    },

    async block(userId: string): Promise<void> {
      const auth = useAuthStore();
      try {
        const data = await RelationApi.block(auth.userId, userId);
        this.blocks = data.blocked ?? this.blocks;
        useUiStore().success('已拉黑');
      } catch (e) {
        useUiStore().error((e as ApiError).message || '拉黑失败');
      }
    },

    async unblock(userId: string): Promise<void> {
      const auth = useAuthStore();
      try {
        const data = await RelationApi.unblock(auth.userId, userId);
        this.blocks = data.blocked ?? this.blocks.filter((id) => id !== userId);
        useUiStore().success('已移出黑名单');
      } catch (e) {
        useUiStore().error((e as ApiError).message || '操作失败');
      }
    },

    /* ------------------------------ 在线状态 ------------------------------ */

    async loadPresence(userIds: string[]): Promise<void> {
      const ids = [...new Set(userIds.filter(Boolean))];
      if (!ids.length) return;
      try {
        const data = await PresenceApi.query(ids);
        (data.states ?? []).forEach((state) => this.applyPresence(state.userId, state.online, state.lastSeen, state));
      } catch {
        /* 状态是装饰品：失败就当作未知 */
      }
    },

    /** 走长连接查询，少一次 HTTP 建连（移动端很值） */
    requestPresence(userIds: string[]): void {
      const ids = [...new Set(userIds.filter(Boolean))];
      if (!ids.length) return;
      imSocket.send({ type: 'presence.get', userIds: ids });
    },

    /* ------------------------------- 群 ------------------------------- */

    async loadGroup(groupId: string, withMembers = true): Promise<void> {
      try {
        const detail = await GroupApi.detail(groupId);
        if (detail.group) this.groups[groupId] = detail.group;
        if (withMembers) {
          this.groupMembers[groupId] = {
            members: Object.keys(detail.roles ?? {}),
            roles: detail.roles ?? {},
            mutes: detail.mutes ?? {},
            size: detail.memberCount ?? 0
          };
          // roles 里就是成员全集（服务端把角色表当成员表用）
          if (this.groupMembers[groupId].members.length === 0) {
            const members = await GroupApi.members(groupId);
            this.groupMembers[groupId] = {
              members: members.members ?? [],
              roles: members.roles ?? {},
              mutes: members.mutes ?? {},
              size: members.size ?? 0
            };
          }
        }
      } catch (e) {
        const code = (e as ApiError).code;
        if (code !== 404) useUiStore().error((e as ApiError).message || '群信息加载失败');
      }
    },

    /** 会话列表里的群全部补齐详情（群名、公告、禁言状态） */
    async refreshAllGroups(): Promise<void> {
      const im = useImStore();
      const groupIds = im.conversations
        .filter((item) => item.type === 'GROUP')
        .map((item) => groupIdOfConversation(item.conversationId));
      const targets = [...new Set(groupIds)].filter((id) => !this.groups[id] || !this.groupMembers[id]);
      await Promise.all(targets.map((id) => this.loadGroup(id, false)));
    },

    async createGroup(name: string, members: string[] = []): Promise<GroupInfo | null> {
      const auth = useAuthStore();
      try {
        const res = await GroupApi.create(auth.userId, name || `${auth.userId} 的群`);
        const group = res.group;
        this.groups[group.groupId] = group;
        const all = [...new Set([auth.userId, ...members.filter(Boolean)])];
        if (all.length > 1) {
          await GroupApi.addMembers(group.groupId, auth.userId, all.filter((id) => id !== auth.userId));
        }
        await useImStore().refreshConversations(true);
        useUiStore().success('群已创建');
        return group;
      } catch (e) {
        useUiStore().error((e as ApiError).message || '建群失败');
        return null;
      }
    },

    async renameGroup(groupId: string, name: string): Promise<void> {
      await this.guard(() => GroupApi.rename(groupId, useAuthStore().userId, name), '已改名');
      await this.loadGroup(groupId, false);
    },

    async setAnnouncement(groupId: string, text: string): Promise<void> {
      await this.guard(() => GroupApi.announcement(groupId, useAuthStore().userId, text), '公告已更新');
      await this.loadGroup(groupId, false);
    },

    async setMuteAll(groupId: string, muted: boolean): Promise<void> {
      await this.guard(() => GroupApi.muteAll(groupId, useAuthStore().userId, muted), muted ? '已全员禁言' : '已解除禁言');
      await this.loadGroup(groupId, false);
    },

    async setMaxMembers(groupId: string, max: number): Promise<void> {
      await this.guard(() => GroupApi.maxMembers(groupId, useAuthStore().userId, max), '人数上限已更新');
      await this.loadGroup(groupId, false);
    },

    async addMembers(groupId: string, userIds: string[]): Promise<void> {
      const ids = userIds.map((id) => id.trim()).filter(Boolean);
      if (!ids.length) return;
      await this.guard(() => GroupApi.addMembers(groupId, useAuthStore().userId, ids), '已邀请成员');
      await this.loadGroup(groupId);
    },

    async removeMember(groupId: string, targetId: string): Promise<void> {
      await this.guard(() => GroupApi.removeMember(groupId, useAuthStore().userId, targetId), '已移出群聊');
      await this.loadGroup(groupId);
    },

    async leaveGroup(groupId: string): Promise<void> {
      await this.guard(() => GroupApi.leave(groupId, useAuthStore().userId), '已退出群聊');
      await useImStore().refreshConversations(true);
    },

    async setRole(groupId: string, targetId: string, role: GroupRole): Promise<void> {
      await this.guard(() => GroupApi.setRole(groupId, useAuthStore().userId, targetId, role), '角色已更新');
      await this.loadGroup(groupId);
    },

    async muteMember(groupId: string, targetId: string, seconds: number): Promise<void> {
      await this.guard(
        () => GroupApi.muteMember(groupId, useAuthStore().userId, targetId, seconds),
        seconds > 0 ? `已禁言 ${seconds} 秒` : '已解除禁言'
      );
      await this.loadGroup(groupId);
    },

    async disband(groupId: string): Promise<void> {
      await this.guard(() => GroupApi.disband(groupId, useAuthStore().userId), '群已解散');
      await useImStore().refreshConversations(true);
    },

    /* --------------------------- 入群申请 / 邀请码 --------------------------- */

    /**
     * 群变更下行。
     *
     * <p>不同 {@code change} 要刷的东西不一样：全量刷一遍等于每次改动都打三四个接口，
     * 而申请人根本看不到待审列表（那是 403）。
     */
    onGroupUpdate(frame: GroupUpdateFrame): void {
      const auth = useAuthStore();
      if (frame.change === 'JOIN_REQUESTED') {
        // 接收方 = 群主/管理员 + 申请人本人（他其它端的「已提交」状态）
        if (frame.targetId === auth.userId) void this.loadMyJoinRequest(frame.groupId);
        if (this.canManageGroup(frame.groupId)) void this.loadPendingJoins(frame.groupId);
        return;
      }
      if (frame.change === 'JOIN_APPROVED' || frame.change === 'JOIN_REJECTED') {
        if (frame.targetId && frame.targetId !== auth.userId) return;
        void this.loadMyJoinRequest(frame.groupId);
        if (frame.change === 'JOIN_APPROVED') {
          // 通过之后才成为成员：群资料要重新拉，会话列表里也会多一条
          void this.loadGroup(frame.groupId, false);
          void useImStore().refreshConversations(true);
        }
        return;
      }
      // 资料 / 成员 / 角色 / 禁言 / 群主转让
      void this.loadGroup(frame.groupId, this.groupMembers[frame.groupId] !== undefined);
    },

    /** 待审入群申请（仅群主/管理员；403 不是错误，只是没有这个视角） */
    async loadPendingJoins(groupId: string, offset = 0, limit = 50): Promise<void> {
      const auth = useAuthStore();
      try {
        const res = await GroupJoinApi.pending(groupId, auth.userId, offset, limit);
        if (res.code !== 0) {
          if (res.code !== 403 && res.code !== 404) useUiStore().error(res.message || '加载入群申请失败');
          this.joinRequests[groupId] = [];
          this.joinRequestTotals[groupId] = 0;
          return;
        }
        this.joinRequests[groupId] = res.requests ?? [];
        this.joinRequestTotals[groupId] = res.total ?? this.joinRequests[groupId].length;
      } catch (e) {
        useUiStore().error((e as ApiError).message || '加载入群申请失败');
      }
    },

    async approveJoin(groupId: string, applicantId: string): Promise<boolean> {
      const ok = await this.guard(
        () => GroupJoinApi.approve(groupId, applicantId, useAuthStore().userId),
        '已通过入群申请'
      );
      if (ok) {
        this.dropPendingJoin(groupId, applicantId);
        // 通过 = 多了一个成员：成员表与会话列表都要跟着变
        await this.loadGroup(groupId);
        void useImStore().refreshConversations(true);
      }
      return ok;
    },

    async rejectJoin(groupId: string, applicantId: string): Promise<boolean> {
      const ok = await this.guard(
        () => GroupJoinApi.reject(groupId, applicantId, useAuthStore().userId),
        '已拒绝入群申请'
      );
      if (ok) this.dropPendingJoin(groupId, applicantId);
      return ok;
    },

    dropPendingJoin(groupId: string, applicantId: string): void {
      const list = this.joinRequests[groupId];
      if (!list) return;
      this.joinRequests[groupId] = list.filter((item) => item.applicantId !== applicantId);
      this.joinRequestTotals[groupId] = Math.max(0, (this.joinRequestTotals[groupId] ?? list.length) - 1);
    },

    /** 直接提交入群申请（需要审批的群） */
    async requestJoin(groupId: string, message = ''): Promise<boolean> {
      const auth = useAuthStore();
      try {
        const res = await GroupJoinApi.request(groupId, auth.userId, message);
        this.myJoinRequests[groupId] = res.request ?? null;
        useUiStore().success('已提交申请，等待管理员通过');
        return true;
      } catch (e) {
        useUiStore().error((e as ApiError).message || '提交申请失败');
        return false;
      }
    },

    /** 我在这群里的申请状态（没申请过时服务端返回 404，按「无」记录） */
    async loadMyJoinRequest(groupId: string): Promise<void> {
      const auth = useAuthStore();
      try {
        const res = await GroupJoinApi.myRequest(groupId, auth.userId);
        this.myJoinRequests[groupId] = res.code === 0 ? res.request ?? null : null;
      } catch {
        /* 读接口抖动：保留上一次的结论，不要清成「没申请过」 */
      }
    },

    async loadInvites(groupId: string): Promise<void> {
      try {
        const res = await GroupJoinApi.invites(groupId, useAuthStore().userId);
        if (res.code !== 0) {
          if (res.code !== 403 && res.code !== 404) useUiStore().error(res.message || '加载邀请码失败');
          this.invites[groupId] = [];
          return;
        }
        this.invites[groupId] = res.invites ?? [];
        if (res.linkBase) this.inviteLinkBase = res.linkBase;
      } catch (e) {
        useUiStore().error((e as ApiError).message || '加载邀请码失败');
      }
    },

    async createInvite(
      groupId: string,
      options: { ttlHours?: number; maxUses?: number; approvalRequired?: boolean } = {}
    ): Promise<GroupInviteItem | null> {
      try {
        const res = await GroupJoinApi.createInvite(groupId, useAuthStore().userId, options);
        const invite = res.invite ?? null;
        if (invite) {
          // link 是服务端拼好的完整链接（客户端拿 code 生成二维码时也用它）
          if (res.link) invite.link = res.link;
          this.invites[groupId] = [invite, ...(this.invites[groupId] ?? [])];
        }
        useUiStore().success('邀请码已生成');
        return invite;
      } catch (e) {
        useUiStore().error((e as ApiError).message || '生成邀请码失败');
        return null;
      }
    },

    async revokeInvite(groupId: string, code: string): Promise<void> {
      try {
        await GroupJoinApi.revokeInvite(groupId, code, useAuthStore().userId);
        this.invites[groupId] = (this.invites[groupId] ?? []).filter((item) => item.code !== code);
        useUiStore().success('已吊销该邀请码');
      } catch (e) {
        useUiStore().error((e as ApiError).message || '吊销失败');
      }
    },

    /**
     * 用邀请码加入。
     *
     * <p>三种结果要分开处理：直接进群（刷新会话、进聊天）、需要审批（只是提交了申请）、
     * 失败（码过期/次数用尽/群满员，服务端给的文案直接展示）。
     */
    async joinByInvite(code: string): Promise<{ ok: boolean; approvalPending: boolean; groupId: string }> {
      const auth = useAuthStore();
      const trimmed = code.trim();
      if (!trimmed) {
        useUiStore().error('请输入邀请码');
        return { ok: false, approvalPending: false, groupId: '' };
      }
      try {
        const res = await GroupJoinApi.join(auth.userId, trimmed);
        if (res.group) this.groups[res.group.groupId] = res.group;
        if (res.approvalPending) {
          useUiStore().success('已提交申请，等待管理员通过');
        } else {
          useUiStore().success('已加入群聊');
          await this.loadGroup(res.groupId, true);
          await useImStore().refreshConversations(true);
        }
        return { ok: true, approvalPending: !!res.approvalPending, groupId: res.groupId ?? '' };
      } catch (e) {
        useUiStore().error((e as ApiError).message || '加入失败');
        return { ok: false, approvalPending: false, groupId: '' };
      }
    },

    async guard(call: () => Promise<{ code: number; message?: string; group?: GroupInfo }>, successText: string): Promise<boolean> {
      try {
        const res = await call();
        if (res.group) this.groups[res.group.groupId] = res.group;
        useUiStore().success(successText);
        return true;
      } catch (e) {
        useUiStore().error((e as ApiError).message || '操作失败');
        return false;
      }
    },

    /* ------------------------------ 设备管理 ------------------------------ */

    async loadDevices(): Promise<void> {
      const auth = useAuthStore();
      try {
        const data = await DeviceApi.list(auth.userId);
        this.devices = data?.devices ?? [];
      } catch (e) {
        useUiStore().error((e as ApiError).message || '设备列表加载失败');
      }
    },

    async kickDevice(deviceId: string, reason = 'kicked-by-server'): Promise<void> {
      const auth = useAuthStore();
      try {
        const res = await DeviceApi.kick(auth.userId, deviceId, reason);
        this.devicesControllable = res.controllable;
        if (res.code !== 0) useUiStore().error(res.message || '未受理');
        else if (res.kicked > 0) useUiStore().success(`已关闭 ${res.kicked} 条连接`);
        else useUiStore().toast('该设备当前不在线');
      } catch (e) {
        useUiStore().error((e as ApiError).message || '踢下线失败');
      } finally {
        await this.loadDevices();
      }
    },

    async kickOthers(): Promise<void> {
      const auth = useAuthStore();
      try {
        const res = await DeviceApi.kickOthers(auth.userId, auth.deviceId);
        this.devicesControllable = res.controllable;
        if (res.controllable === false) useUiStore().error('这个部署没有连接控制通道，踢下线未执行');
        else useUiStore().success(res.kicked > 0 ? `已退出 ${res.kicked} 台设备` : '没有其它在线设备');
      } catch (e) {
        useUiStore().error((e as ApiError).message || '操作失败');
      } finally {
        await this.loadDevices();
      }
    },

    async forgetDevice(deviceId: string): Promise<void> {
      const auth = useAuthStore();
      try {
        await DeviceApi.forget(auth.userId, deviceId);
        useUiStore().success('已移除设备');
      } catch (e) {
        useUiStore().error((e as ApiError).message || '移除失败');
      } finally {
        await this.loadDevices();
      }
    }
  }
});
