import { defineStore } from 'pinia';
import { ApiError } from '@/api/http';
import { DeviceApi, GroupApi, PresenceApi, RelationApi, type DeviceListResponse } from '@/api/rest';
import { imSocket } from '@/api/ws';
import { cacheProfile, cachedProfiles } from '@/core/db';
import { groupIdOfConversation } from '@/core/format';
import type { UserProfile } from '@/types/local';
import type {
  DownlinkFrame,
  GroupInfo,
  GroupRole,
  PresenceEntry,
  PresencePushFrame,
  PresenceSnapshotFrame
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

export const useContactStore = defineStore('contact', {
  state: () => ({
    friends: [] as string[],
    blocks: [] as string[],
    /** userId -> 在线状态 */
    presence: {} as Record<string, PresenceEntry>,
    /** userId -> 本地资料（服务端没有资料接口，昵称头像只在本地） */
    profiles: {} as Record<string, UserProfile>,
    groups: {} as Record<string, GroupInfo>,
    groupMembers: {} as Record<string, GroupMembers>,
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
    }
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
        }
      });
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

    async addFriend(friendId: string): Promise<boolean> {
      const auth = useAuthStore();
      const id = friendId.trim();
      if (!id || id === auth.userId) {
        useUiStore().error('请输入对方 userId（不能是自己）');
        return false;
      }
      try {
        const data = await RelationApi.addFriend(auth.userId, id);
        this.friends = data.friends ?? this.friends;
        useUiStore().success('已添加好友');
        return true;
      } catch (e) {
        useUiStore().error((e as ApiError).message || '添加失败');
        return false;
      }
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
