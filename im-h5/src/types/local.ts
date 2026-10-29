import type { ContentType, ConvType } from './protocol';

/**
 * 本地消息状态机：
 *
 * ```
 * sending ──ack──> sent ──对端 msg.state DELIVERED──> delivered ──READ──> read
 *    │
 *    └──超时/错误──> failed ──重发──> sending
 * ```
 *
 * `recalled` 是终态（服务端已经把 body 抹掉了，只能展示「已撤回」）。
 */
export type LocalMsgState = 'sending' | 'failed' | 'sent' | 'delivered' | 'read' | 'recalled';

export interface ChatMessage {
  /** 本地主键：`conversationId|clientMsgId`，未确认的消息没有 seq，只能靠它定位 */
  key: string;
  conversationId: string;
  convType: ConvType;
  seq: number;
  serverMsgId: string;
  clientMsgId: string;
  from: string;
  to: string;
  body: string;
  contentType: ContentType;
  /** 原始 JSON 字符串，渲染前再 parse（与线协议保持一致） */
  attrs?: string;
  recalled?: boolean;
  ts: number;
  status: LocalMsgState;
  /** 本地删除：立刻从列表消失，服务端那份由 `msg.update DELETED` 保证 */
  deleted?: boolean;
}

/** 用户资料。服务端没有资料接口，这里全部落在本地（换设备不同步） */
export interface UserProfile {
  userId: string;
  nickname: string;
  avatar?: string;
  signature?: string;
  updatedAt: number;
}

export interface ConversationDraft {
  conversationId: string;
  draft: string;
  updatedAt: number;
}
