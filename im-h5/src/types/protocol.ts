/**
 * 与 im-cluster 服务端对齐的线协议类型。
 *
 * 这里只写「真正在线上跑」的字段，来源：
 * - 上行：`im-gateway/.../protocol/ClientFrames.java` + `SessionLifecycle` 的解析
 * - 下行：`im-server/.../message/ClientFrames.java` + `im-gateway/.../protocol/ClientFrames.java`
 * - REST：`im-server/.../controller/*Controller.java`
 *
 * 两个必须记住的坑：
 * 1. 数字字段必须发数字（服务端用 `asLong()` 读，`"5"` 会变成 0）；
 * 2. REST 的业务错误在响应体的 `code` 里，HTTP 通常仍是 200。
 */

export type ConvType = 'SINGLE' | 'GROUP';
export type ContentType = 'TEXT' | 'IMAGE' | 'VOICE' | 'VIDEO' | 'FILE' | 'CUSTOM';
export type GroupRole = 'MEMBER' | 'ADMIN' | 'OWNER';
export type MsgState = 'DELIVERED' | 'READ';

export type GroupChange =
  | 'CREATED'
  | 'UPDATED'
  | 'MEMBER_ADDED'
  | 'MEMBER_REMOVED'
  | 'MEMBER_LEFT'
  | 'ROLE_CHANGED'
  | 'MUTED'
  | 'UNMUTED'
  | 'DISMISSED';

/** 媒体/文件的附加信息。线上是 **JSON 字符串**，收发两端各自 stringify / parse */
export interface MediaAttrs {
  url?: string;
  name?: string;
  size?: number;
  duration?: number;
  width?: number;
  height?: number;
  thumb?: string;
}

/* -------------------------------------------------------------------------- */
/* 下行帧                                                                      */
/* -------------------------------------------------------------------------- */

export interface MsgPushFrame {
  type: 'msg.push';
  conversationId: string;
  convType: ConvType;
  seq: number;
  serverMsgId: string;
  clientMsgId: string;
  from: string;
  to: string;
  body: string;
  contentType: ContentType;
  attrs?: string;
  recalled?: boolean;
  ts: number;
}

export interface MsgAckFrame {
  type: 'msg.ack';
  clientMsgId: string;
  serverMsgId: string;
  seq: number;
  code: number;
  traceId?: string;
}

export interface MsgStateFrame {
  type: 'msg.state';
  conversationId: string;
  convType: ConvType;
  state: MsgState;
  seq: number;
  userId: string;
  ts: number;
}

export interface MsgUpdateFrame {
  type: 'msg.update';
  conversationId: string;
  convType: ConvType;
  seq: number;
  change: 'RECALLED' | 'DELETED';
  actorId: string;
  ts: number;
}

export interface GroupUpdateFrame {
  type: 'group.update';
  groupId: string;
  change: GroupChange;
  actorId: string;
  targetId: string;
  ts: number;
}

export interface TypingPushFrame {
  type: 'typing.push';
  conversationId: string;
  convType: ConvType;
  userId: string;
  typing: boolean;
  ts: number;
}

export interface PresencePushFrame {
  type: 'presence.push';
  userId: string;
  online: boolean;
  ts: number;
}

export interface PresenceEntry {
  userId: string;
  online: boolean;
  gateways: number;
  devices: number;
  lastSeen: number;
}

export interface PresenceSnapshotFrame {
  type: 'presence.snapshot';
  states: PresenceEntry[];
  ts: number;
}

export interface SessionLoginFrame {
  type: 'session.login';
  userId: string;
  deviceId: string;
  transport: string;
  ts: number;
}

export interface SessionKickFrame {
  type: 'session.kick';
  deviceId: string;
  reason: string;
  ts: number;
}

export interface SessionMigrateFrame {
  type: 'session.migrate';
  reason: string;
  retryAfterMs: number;
  ts: number;
}

export interface ConvUpdateFrame {
  type: 'conv.update';
  conversationId: string;
  pinned: boolean;
  muted: boolean;
  originDeviceId: string;
  ts: number;
}

/** 有人申请加你为好友（{@code relation.request}），推给被申请人 */
export interface RelationRequestFrame {
  type: 'relation.request';
  fromId: string;
  /** 验证消息，可能为空串 */
  message: string;
  ts: number;
}

/** 好友申请的处理结果（ACCEPTED / REJECTED 推给申请人，CANCELLED 推给被申请人） */
export type RelationChange = 'ACCEPTED' | 'REJECTED' | 'CANCELLED';

export interface RelationUpdateFrame {
  type: 'relation.update';
  /** 另一方：ACCEPTED/REJECTED 时是审批人，CANCELLED 时是申请人 */
  peerId: string;
  change: RelationChange;
  ts: number;
}

/** 好友申请单（服务端 {@code FriendRequest} 的同形 DTO） */
export type FriendRequestStatus = 'PENDING' | 'ACCEPTED' | 'REJECTED';

export interface FriendRequestItem {
  fromId: string;
  toId: string;
  status: FriendRequestStatus;
  message: string;
  /** 首次申请时间（重新申请不刷新） */
  createdAt: number;
  updatedAt: number;
}

export interface ErrorFrame {
  type: 'error';
  clientMsgId?: string | null;
  code: number;
  message: string;
}

export interface PingPongFrame {
  type: 'ping' | 'pong';
  ts: number;
}

export type DownlinkFrame =
  | MsgPushFrame
  | MsgAckFrame
  | MsgStateFrame
  | MsgUpdateFrame
  | GroupUpdateFrame
  | TypingPushFrame
  | PresencePushFrame
  | PresenceSnapshotFrame
  | SessionLoginFrame
  | SessionKickFrame
  | SessionMigrateFrame
  | ConvUpdateFrame
  | RelationRequestFrame
  | RelationUpdateFrame
  | ErrorFrame
  | PingPongFrame
  | { type: 'connected'; userId: string; transport: string; ts: number }
  | { type: string; [key: string]: unknown };

/* -------------------------------------------------------------------------- */
/* 上行帧                                                                      */
/* -------------------------------------------------------------------------- */

export interface MsgSendFrame {
  ver?: 1;
  type: 'msg.send';
  clientMsgId: string;
  to: string;
  body: string;
  convType?: ConvType;
  conversationId?: string;
  clientTs?: number;
  contentType?: ContentType;
  attrs?: string;
  traceId?: string;
}

export interface MsgRecvFrame {
  ver?: 1;
  type: 'msg.recv';
  acks: Array<{ conversationId: string; seq: number }>;
}

export interface MsgReadFrame {
  ver?: 1;
  type: 'msg.read';
  conversationId: string;
  seq: number;
}

export interface SyncFrame {
  ver?: 1;
  type: 'sync';
  watermarks: Record<string, number>;
  /**
   * 显式要求一次「新端回放」（最近 M 个会话 × 每个最近 N 条）。
   * 服务端只在设备第一次上线时自动回放；本地缓存清空但 deviceId 没变时要靠这个标志。
   */
  replay?: boolean;
}

export interface TypingFrame {
  ver?: 1;
  type: 'typing';
  conversationId?: string;
  to?: string;
  convType?: ConvType;
  typing?: boolean;
  clientTs?: number;
}

export interface PresenceGetFrame {
  ver?: 1;
  type: 'presence.get';
  userIds: string[];
}

export type UplinkFrame =
  | MsgSendFrame
  | MsgRecvFrame
  | MsgReadFrame
  | SyncFrame
  | TypingFrame
  | PresenceGetFrame
  | { ver?: 1; type: 'ping' | 'pong'; ts?: number };

/* -------------------------------------------------------------------------- */
/* REST DTO                                                                    */
/* -------------------------------------------------------------------------- */

export interface ConversationItem {
  conversationId: string;
  type: ConvType;
  lastSeq: number;
  lastTime: number;
  lastFrom: string;
  lastDigest: string;
  lastMsgId: string;
  readSeq: number;
  unread: number;
  pinned: boolean;
  muted: boolean;
}

export interface GroupInfo {
  groupId: string;
  name: string;
  ownerId: string;
  announcement: string;
  maxMembers: number;
  createdAt: number;
  mutedAll: boolean;
  dismissed: boolean;
}

export interface DeviceItem {
  deviceId: string;
  online: boolean;
  transport: string;
  gateway: string;
  loginAt: number;
}
