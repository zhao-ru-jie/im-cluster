import { api, withQuery } from './http';
import type {
  ConversationItem,
  ConvType,
  DeviceItem,
  FriendRequestItem,
  GroupInfo,
  GroupRole,
  MsgPushFrame,
  PresenceEntry
} from '@/types/protocol';

/**
 * im-server 的 REST 封装（全部接口都是「无鉴权 + 身份放在 query/body 里」）。
 *
 * 注意两点：
 * 1. 会话 ID / 群 ID 里带 `:`，路径段必须 `encodeURIComponent`；
 * 2. 写操作的业务错误是 200 + `code != 0`，由 `api()` 统一抛成 `ApiError`。
 */

/* ------------------------------- 会话 ------------------------------- */

export interface ConvListResponse {
  userId: string;
  limit: number;
  count: number;
  conversations: ConversationItem[];
}

export interface ReadResponse {
  conversationId: string;
  code: number;
  message?: string;
  readSeq: number;
  unread: number;
}

export interface FlagResponse {
  conversationId: string;
  code: number;
  message?: string;
  pinned: boolean;
  muted: boolean;
}

export interface HistoryResponse {
  conversationId: string;
  limit: number;
  count: number;
  nextBeforeSeq: number;
  hasMore: boolean;
  messages: MsgPushFrame[];
}

export const ConvApi = {
  list(userId: string, limit = 50): Promise<ConvListResponse> {
    return api<ConvListResponse>('GET', withQuery('/conv', { userId, limit }), undefined, { throwOnBizError: false });
  },
  history(conversationId: string, userId: string, beforeSeq = 0, limit = 30): Promise<HistoryResponse> {
    return api<HistoryResponse>(
      'GET',
      withQuery(`/conv/${encodeURIComponent(conversationId)}/messages`, { userId, beforeSeq, limit }),
      undefined,
      { throwOnBizError: false }
    );
  },
  read(conversationId: string, userId: string, seq: number): Promise<ReadResponse> {
    return api<ReadResponse>('POST', withQuery(`/conv/${encodeURIComponent(conversationId)}/read`, { userId, seq }));
  },
  flag(conversationId: string, userId: string, flag: { pinned?: boolean; muted?: boolean; deviceId?: string }): Promise<FlagResponse> {
    return api<FlagResponse>(
      'POST',
      withQuery(`/conv/${encodeURIComponent(conversationId)}/flag`, {
        userId,
        pinned: flag.pinned,
        muted: flag.muted,
        deviceId: flag.deviceId
      })
    );
  },
  recall(conversationId: string, seq: number, operatorId: string): Promise<{ code: number; message?: string }> {
    return api('POST', withQuery(`/conv/${encodeURIComponent(conversationId)}/messages/${seq}/recall`, { operatorId }));
  },
  remove(conversationId: string, seq: number, userId: string): Promise<{ code: number; message?: string }> {
    return api('POST', withQuery(`/conv/${encodeURIComponent(conversationId)}/messages/${seq}/delete`, { userId }));
  }
};

/* -------------------------------- 群 -------------------------------- */

export interface GroupMetaResponse {
  code: number;
  message?: string;
  group: GroupInfo;
}

export interface GroupDetailResponse extends GroupMetaResponse {
  memberCount: number;
  roles: Record<string, GroupRole>;
  mutes: Record<string, number>;
}

export interface GroupMembersResponse {
  code: number;
  message?: string;
  groupId: string;
  size: number;
  members: string[];
  roles: Record<string, GroupRole>;
  mutes: Record<string, number>;
}

export const GroupApi = {
  create(ownerId: string, name: string, maxMembers = 0): Promise<GroupMetaResponse> {
    return api<GroupMetaResponse>('POST', withQuery('/group', { ownerId, name, maxMembers }));
  },
  detail(groupId: string): Promise<GroupDetailResponse> {
    return api<GroupDetailResponse>('GET', `/group/${encodeURIComponent(groupId)}`);
  },
  members(groupId: string): Promise<GroupMembersResponse> {
    return api<GroupMembersResponse>('GET', `/group/${encodeURIComponent(groupId)}/members`);
  },
  rename(groupId: string, operatorId: string, name: string): Promise<GroupMetaResponse> {
    return api('POST', withQuery(`/group/${encodeURIComponent(groupId)}/rename`, { operatorId, name }));
  },
  announcement(groupId: string, operatorId: string, text: string): Promise<GroupMetaResponse> {
    return api('POST', withQuery(`/group/${encodeURIComponent(groupId)}/announcement`, { operatorId, text }));
  },
  muteAll(groupId: string, operatorId: string, muted: boolean): Promise<GroupMetaResponse> {
    return api('POST', withQuery(`/group/${encodeURIComponent(groupId)}/mute-all`, { operatorId, muted }));
  },
  maxMembers(groupId: string, operatorId: string, max: number): Promise<GroupMetaResponse> {
    return api('POST', withQuery(`/group/${encodeURIComponent(groupId)}/max-members`, { operatorId, max }));
  },
  addMembers(groupId: string, operatorId: string, userIds: string[]): Promise<GroupMetaResponse> {
    return api('POST', withQuery(`/group/${encodeURIComponent(groupId)}/members`, { operatorId }), userIds);
  },
  removeMember(groupId: string, operatorId: string, targetId: string): Promise<GroupMetaResponse> {
    return api('DELETE', withQuery(`/group/${encodeURIComponent(groupId)}/members/${encodeURIComponent(targetId)}`, { operatorId }));
  },
  leave(groupId: string, userId: string): Promise<GroupMetaResponse> {
    return api('POST', withQuery(`/group/${encodeURIComponent(groupId)}/leave`, { userId }));
  },
  setRole(groupId: string, operatorId: string, targetId: string, role: GroupRole): Promise<GroupMetaResponse> {
    return api('POST', withQuery(`/group/${encodeURIComponent(groupId)}/roles/${encodeURIComponent(targetId)}`, { operatorId, role }));
  },
  muteMember(groupId: string, operatorId: string, targetId: string, seconds: number): Promise<GroupMetaResponse> {
    return api('POST', withQuery(`/group/${encodeURIComponent(groupId)}/mute/${encodeURIComponent(targetId)}`, { operatorId, seconds }));
  },
  disband(groupId: string, operatorId: string): Promise<GroupMetaResponse> {
    return api('POST', withQuery(`/group/${encodeURIComponent(groupId)}/disband`, { operatorId }));
  }
};

/* ------------------------------- 关系链 ------------------------------- */

export interface FriendsResponse {
  userId: string;
  size: number;
  friends: string[];
}

export interface BlocksResponse {
  userId: string;
  size: number;
  blocked: string[];
}

export const RelationApi = {
  friends(userId: string): Promise<FriendsResponse> {
    return api<FriendsResponse>('GET', `/relation/${encodeURIComponent(userId)}/friends`, undefined, { throwOnBizError: false });
  },
  // 没有 addFriend：建立好友关系必须走「申请 + 对方同意」，
  // 服务端已经把直接写双向好友边的路由删掉了（见 RelationController 类注释）
  removeFriend(userId: string, friendId: string): Promise<FriendsResponse> {
    return api<FriendsResponse>('DELETE', `/relation/${encodeURIComponent(userId)}/friends/${encodeURIComponent(friendId)}`);
  },
  blocks(userId: string): Promise<BlocksResponse> {
    return api<BlocksResponse>('GET', `/relation/${encodeURIComponent(userId)}/blocks`, undefined, { throwOnBizError: false });
  },
  block(userId: string, blockedId: string): Promise<BlocksResponse> {
    return api<BlocksResponse>('POST', `/relation/${encodeURIComponent(userId)}/blocks`, [blockedId]);
  },
  unblock(userId: string, blockedId: string): Promise<BlocksResponse> {
    return api<BlocksResponse>('DELETE', `/relation/${encodeURIComponent(userId)}/blocks/${encodeURIComponent(blockedId)}`);
  }
};

/* ------------------------------ 好友申请 ------------------------------ */

export interface FriendRequestListResponse {
  code: number;
  message?: string;
  userId: string;
  incoming: FriendRequestItem[];
  outgoing: FriendRequestItem[];
}

export interface FriendActionResponse {
  code: number;
  message?: string;
  /** accept 时才带：动作之后的好友列表，省一次往返 */
  friends?: string[];
  request?: FriendRequestItem;
}

/**
 * 好友申请（申请 → 通知 → 同意/拒绝）。
 *
 * <p>没有 throwOnBizError:false：这里的写操作失败必须让调用方拿到
 * {@code ApiError.message}（「你们已经是好友了」「申请不存在或已处理」），
 * 这些是用户要看到的文案，不能被静默吞掉。
 */
export const FriendRequestApi = {
  send(userId: string, to: string, message = ''): Promise<FriendActionResponse> {
    return api<FriendActionResponse>('POST', `/relation/${encodeURIComponent(userId)}/requests`, { to, message });
  },
  list(userId: string): Promise<FriendRequestListResponse> {
    // 读接口：网络抖动时按「空列表」降级，避免通讯录整页报错
    return api<FriendRequestListResponse>('GET', `/relation/${encodeURIComponent(userId)}/requests`, undefined, { throwOnBizError: false });
  },
  accept(userId: string, fromId: string): Promise<FriendActionResponse> {
    return api<FriendActionResponse>('POST', `/relation/${encodeURIComponent(userId)}/requests/${encodeURIComponent(fromId)}/accept`);
  },
  reject(userId: string, fromId: string): Promise<FriendActionResponse> {
    return api<FriendActionResponse>('POST', `/relation/${encodeURIComponent(userId)}/requests/${encodeURIComponent(fromId)}/reject`);
  },
  cancel(userId: string, toId: string): Promise<FriendActionResponse> {
    return api<FriendActionResponse>('DELETE', `/relation/${encodeURIComponent(userId)}/requests/${encodeURIComponent(toId)}`);
  }
};

/* ------------------------------- 在线状态 ------------------------------- */

export const PresenceApi = {
  query(userIds: string[]): Promise<{ count: number; states: PresenceEntry[] }> {
    return api('GET', withQuery('/presence', { userIds: userIds.join(',') }), undefined, { throwOnBizError: false });
  }
};

/* ------------------------------- 设备 ------------------------------- */

export interface DeviceListResponse {
  userId: string;
  count: number;
  onlineCount: number;
  devices: DeviceItem[];
}

export interface KickResponse {
  code: number;
  message?: string;
  userId: string;
  kicked: number;
  deviceIds: string[];
  controllable: boolean;
}

export const DeviceApi = {
  list(userId: string): Promise<DeviceListResponse> {
    return api('GET', `/device/${encodeURIComponent(userId)}`, undefined, { throwOnBizError: false });
  },
  kick(userId: string, deviceId: string, reason = 'kicked-by-server'): Promise<KickResponse> {
    return api('POST', withQuery(`/device/${encodeURIComponent(userId)}/kick`, { deviceId, reason }));
  },
  kickOthers(userId: string, keepDeviceId: string, reason = 'logout-other-devices'): Promise<KickResponse> {
    return api('POST', withQuery(`/device/${encodeURIComponent(userId)}/kick-others`, { keepDeviceId, reason }));
  },
  forget(userId: string, deviceId: string): Promise<{ code: number; userId: string; deviceId: string }> {
    return api('DELETE', `/device/${encodeURIComponent(userId)}/${encodeURIComponent(deviceId)}`);
  }
};

/* --------------------------- REST 兜底发消息 --------------------------- */

export interface SendResponse {
  code: number;
  serverMsgId: string;
  seq: number;
  duplicate?: boolean;
}

/** 长连接不可用时的兜底通道（功能等价，但没有 msg.ack 之类的即时反馈） */
export function sendViaRest(input: {
  from: string;
  to: string;
  body: string;
  clientMsgId: string;
  convType: ConvType;
  contentType?: string;
  attrs?: string;
  clientTs?: number;
}): Promise<SendResponse> {
  return api<SendResponse>('POST', '/send', input);
}
