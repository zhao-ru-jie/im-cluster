import { api, withQuery } from './http';
import type {
  ConversationItem,
  ConvType,
  DeviceItem,
  FriendRequestItem,
  GroupInfo,
  GroupInviteItem,
  GroupJoinRequestItem,
  GroupRole,
  MsgPushFrame,
  PresenceEntry,
  ReadStatItem
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

/* ---------------------------- 入群申请 / 邀请码 ---------------------------- */

export interface JoinActionResponse {
  code: number;
  message?: string;
  request?: GroupJoinRequestItem;
}

export interface PendingJoinsResponse {
  code: number;
  message?: string;
  groupId: string;
  total: number;
  offset: number;
  limit: number;
  requests: GroupJoinRequestItem[];
}

export interface InvitesResponse {
  code: number;
  message?: string;
  groupId: string;
  /** 邀请链接前缀，服务端配置项决定（`im://group/join` / https 落地页） */
  linkBase: string;
  invites: GroupInviteItem[];
}

export interface InviteCreateResponse {
  code: number;
  message?: string;
  invite: GroupInviteItem;
  /** 直接给用户复制/生成二维码的完整链接 */
  link: string;
}

export interface JoinByInviteResponse {
  code: number;
  message?: string;
  groupId: string;
  /** true = 这个码要审批，本次只是提交了申请，不是「已加入」 */
  approvalPending: boolean;
  group?: GroupInfo;
}

/**
 * 扫码/点链接进来的唯一入口是「码」，没有独立的二维码接口。
 *
 * <p>不发 throwOnBizError:false 的那几个（提交申请、审批、生成码）：失败文案
 * （「你已经是群成员」「码已过期」「只有群主或管理员可以审批」）就是要给用户看的。
 */
export const GroupJoinApi = {
  /** 提交入群申请（需要审批的群；满员/已是成员会直接返回业务错误） */
  request(groupId: string, userId: string, message = ''): Promise<JoinActionResponse> {
    return api<JoinActionResponse>(
      'POST',
      withQuery(`/group/${encodeURIComponent(groupId)}/join-requests`, { userId, message })
    );
  },
  /** 我在这群里的申请状态（没有申请过时服务端返回 404，按「无」处理） */
  myRequest(groupId: string, userId: string): Promise<JoinActionResponse> {
    return api<JoinActionResponse>(
      'GET',
      `/group/${encodeURIComponent(groupId)}/join-requests/${encodeURIComponent(userId)}`,
      undefined,
      { throwOnBizError: false }
    );
  },
  /** 待审列表（仅群主/管理员；读接口抖动时降级成空列表，调用方按 code 判断） */
  pending(groupId: string, operatorId: string, offset = 0, limit = 50): Promise<PendingJoinsResponse> {
    return api<PendingJoinsResponse>(
      'GET',
      withQuery(`/group/${encodeURIComponent(groupId)}/join-requests`, { operatorId, offset, limit }),
      undefined,
      { throwOnBizError: false }
    );
  },
  approve(groupId: string, applicantId: string, operatorId: string): Promise<GroupMetaResponse> {
    return api<GroupMetaResponse>(
      'POST',
      withQuery(`/group/${encodeURIComponent(groupId)}/join-requests/${encodeURIComponent(applicantId)}/approve`, {
        operatorId
      })
    );
  },
  reject(groupId: string, applicantId: string, operatorId: string): Promise<GroupMetaResponse> {
    return api<GroupMetaResponse>(
      'POST',
      withQuery(`/group/${encodeURIComponent(groupId)}/join-requests/${encodeURIComponent(applicantId)}/reject`, {
        operatorId
      })
    );
  },
  createInvite(
    groupId: string,
    operatorId: string,
    options: { ttlHours?: number; maxUses?: number; approvalRequired?: boolean } = {}
  ): Promise<InviteCreateResponse> {
    return api<InviteCreateResponse>(
      'POST',
      withQuery(`/group/${encodeURIComponent(groupId)}/invites`, {
        operatorId,
        ttlHours: options.ttlHours ?? 0,
        maxUses: options.maxUses ?? 0,
        approvalRequired: options.approvalRequired ?? false
      })
    );
  },
  invites(groupId: string, operatorId: string): Promise<InvitesResponse> {
    return api<InvitesResponse>(
      'GET',
      withQuery(`/group/${encodeURIComponent(groupId)}/invites`, { operatorId }),
      undefined,
      { throwOnBizError: false }
    );
  },
  revokeInvite(groupId: string, code: string, operatorId: string): Promise<GroupMetaResponse & { code: number }> {
    return api(
      'DELETE',
      withQuery(`/group/${encodeURIComponent(groupId)}/invites/${encodeURIComponent(code)}`, { operatorId })
    );
  },
  /** 用邀请码加入（或提交申请，看返回的 approvalPending） */
  join(userId: string, code: string): Promise<JoinByInviteResponse> {
    return api<JoinByInviteResponse>('POST', withQuery('/group/join', { userId, code }));
  }
};

/* ------------------------------- 群已读 ------------------------------- */

export interface ReadStatResponse extends Partial<ReadStatItem> {
  code: number;
  message?: string;
}

export const ReadStatApi = {
  /**
   * 群聊已读聚合：`seq=0` 表示「最新一条」。
   *
   * <p>服务端有 2 秒快照缓存——滑动一屏连续问十几条只会打一次存储，
   * 所以这里不需要客户端再压一层缓存。
   */
  stat(conversationId: string, userId: string, seq = 0, limit = 50): Promise<ReadStatResponse> {
    return api<ReadStatResponse>(
      'GET',
      withQuery(`/conv/${encodeURIComponent(conversationId)}/read-stat`, { userId, seq, limit }),
      undefined,
      { throwOnBizError: false }
    );
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
