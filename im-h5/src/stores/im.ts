import { defineStore } from 'pinia';
import { ApiError } from '@/api/http';
import { ConvApi, ReadStatApi, sendViaRest } from '@/api/rest';
import { imSocket, type SocketState } from '@/api/ws';
import { cacheConversations, cacheMessages, cachedConversations, cachedMessages, deleteCachedMessage } from '@/core/db';
import { digestOf, groupConversationId, singleConversationId } from '@/core/format';
import { beep, notifyMessage, vibrate } from '@/core/notify';
import type { ChatMessage, LocalMsgState } from '@/types/local';
import type {
  ContentType,
  ConversationItem,
  ConvType,
  ConvUpdateFrame,
  DownlinkFrame,
  ErrorFrame,
  GroupUpdateFrame,
  MsgAckFrame,
  MsgPushFrame,
  MsgStateFrame,
  MsgUpdateFrame,
  ReadStatItem,
  SessionKickFrame,
  SessionLoginFrame,
  SessionMigrateFrame,
  TypingPushFrame
} from '@/types/protocol';
import { useAuthStore } from './auth';
import { useContactStore } from './contact';
import { useUiStore } from './ui';

const ACK_TIMEOUT = 12_000;
const TYPING_TTL = 6_000;
const HISTORY_PAGE = 30;
/** 一次 msg.recv 批量确认的窗口：攒 80ms 再发，避免每条消息一个来回 */
const RECV_BATCH_DELAY = 80;

function uuid(): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') return crypto.randomUUID();
  return `${Date.now().toString(16)}-${Math.random().toString(16).slice(2, 10)}`;
}

function sortMessages(list: ChatMessage[]): ChatMessage[] {
  return list.sort((a, b) => {
    const sa = a.seq > 0 ? a.seq : Number.MAX_SAFE_INTEGER;
    const sb = b.seq > 0 ? b.seq : Number.MAX_SAFE_INTEGER;
    if (sa !== sb) return sa - sb;
    return a.ts - b.ts;
  });
}

/** 已见水位：会话 -> 最后一条 seq。服务端据此抬补发起点、并据此判断要不要回放 */
function watermarksOf(conversations: ConversationItem[]): Record<string, number> {
  const watermarks: Record<string, number> = {};
  conversations.forEach((item) => {
    watermarks[item.conversationId] = item.lastSeq;
  });
  return watermarks;
}

/** 待发送消息的归属（ack 帧里没有 conversationId，只能自己记） */
const pendingConv = new Map<string, string>();
/** 帧监听只绑一次：退出再登录时重复 onFrame 会让同一帧被处理两遍 */
let framesBound = false;
const ackTimers = new Map<string, number>();
const recvBuffer: Array<{ conversationId: string; seq: number }> = [];
let recvTimer = 0;
let typingTimers: Record<string, number> = {};

export const useImStore = defineStore('im', {
  state: () => ({
    socketState: 'idle' as SocketState,
    socketReason: '',
    retryInMs: 0,
    kickedReason: '',
    /** 当前打开的会话（用于「收到消息但正在看」时不提醒） */
    activeConversation: '',
    conversations: [] as ConversationItem[],
    messages: {} as Record<string, ChatMessage[]>,
    hasMore: {} as Record<string, boolean>,
    loadingHistory: false,
    typing: {} as Record<string, { userId: string; at: number }>,
    /** conversationId -> userId -> 该用户确认到的最大 seq */
    watermarks: {} as Record<string, Record<string, { delivered: number; read: number }>>,
    /** `conversationId#seq` -> 群已读聚合快照（按 seq 缓存：同一条消息反复点只查一次） */
    readStats: {} as Record<string, ReadStatItem>
  }),

  getters: {
    sortedConversations(state): ConversationItem[] {
      return [...state.conversations].sort((a, b) => {
        if (a.pinned !== b.pinned) return a.pinned ? -1 : 1;
        return b.lastTime - a.lastTime;
      });
    },
    unreadTotal(state): number {
      return state.conversations.reduce((sum, item) => sum + (item.muted ? 0 : item.unread), 0);
    },
    conversationOf: (state) => (conversationId: string): ConversationItem | undefined =>
      state.conversations.find((item) => item.conversationId === conversationId),
    messagesOf: (state) => (conversationId: string): ChatMessage[] => state.messages[conversationId] ?? [],
    /** 会话里最后一条「有 seq」的消息，用来做已读水位 */
    lastSeqOf: (state) => (conversationId: string): number => {
      const list = state.messages[conversationId] ?? [];
      return list.reduce((max, msg) => (msg.seq > max ? msg.seq : max), 0);
    },
    /** 已拉过的群已读快照；seq=0 表示「最新一条」，执行 query 前拿不到，所以查不到就返回 undefined */
    readStatOf: (state) => (conversationId: string, seq: number): ReadStatItem | undefined =>
      state.readStats[`${conversationId}#${seq}`]
  },

  actions: {
    /* ---------------------------- 连接生命周期 ---------------------------- */

    async bootstrap(): Promise<void> {
      const auth = useAuthStore();
      if (!auth.loggedIn) return;

      // 冷启动先上缓存，避免白屏等待网络
      const cached = await cachedConversations();
      if (cached.length && this.conversations.length === 0) {
        this.conversations = cached;
      }

      if (!framesBound) {
        framesBound = true;
        imSocket.onFrame((frame) => this.handleFrame(frame));
        imSocket.onState((state, info) => this.handleSocketState(state, info));
      }
      this.connect();
      void this.refreshConversations();
    },

    connect(): void {
      const auth = useAuthStore();
      if (!auth.userId) return;
      imSocket.connect({
        url: auth.endpoints.wsUrl,
        userId: auth.userId,
        deviceId: auth.deviceId,
        token: auth.token || undefined
      });
    },

    disconnect(): void {
      imSocket.close('logout');
      this.socketState = 'idle';
      this.activeConversation = '';
      this.conversations = [];
      this.messages = {};
      this.watermarks = {};
      pendingConv.clear();
      ackTimers.forEach((timer) => clearTimeout(timer));
      ackTimers.clear();
    },

    handleSocketState(state: SocketState, info: { reason?: string; retryInMs?: number }): void {
      const ui = useUiStore();
      this.socketState = state;
      this.socketReason = info.reason ?? '';
      this.retryInMs = info.retryInMs ?? 0;

      if (state === 'open') {
        this.kickedReason = '';
        this.retryInMs = 0;
        ui.clearBanner();
        this.onConnected();
        return;
      }
      if (state === 'kicked') {
        this.kickedReason = info.reason || 'kicked';
        ui.clearBanner();
        return;
      }
      if (state === 'connecting') {
        ui.setBanner('正在连接服务器…', 'info');
        return;
      }
      if (state === 'closed') {
        const seconds = this.retryInMs ? Math.max(1, Math.round(this.retryInMs / 1000)) : 0;
        ui.setBanner(
          seconds > 0 ? `网络已断开（${this.socketReason || '连接中断'}），${seconds} 秒后重试` : '网络已断开，正在重连…',
          'error'
        );
      }
    },

    /** 连上之后：上报已读水位，让服务端把断线期间的离线帧补下来 */
    onConnected(): void {
      const watermarks = watermarksOf(this.conversations);
      imSocket.send({ type: 'sync', watermarks });
      void this.refreshConversations(true);
      // 断线期间发出的消息可能还在队列里：给它们一次机会，超过超时时间的标记失败
      Object.keys(this.messages).forEach((convId) => {
        this.messages[convId]?.forEach((msg) => {
          if (msg.status === 'sending' && !ackTimers.has(msg.clientMsgId) && Date.now() - msg.ts > ACK_TIMEOUT) {
            msg.status = 'failed';
          }
        });
      });
    },

    /* ------------------------------ 帧分发 ------------------------------ */

    handleFrame(frame: DownlinkFrame): void {
      const type = (frame as { type?: string }).type;
      switch (type) {
        case 'msg.push':
          this.onPush(frame as MsgPushFrame);
          break;
        case 'msg.ack':
          this.onAck(frame as MsgAckFrame);
          break;
        case 'msg.state':
          this.onStateFrame(frame as MsgStateFrame);
          break;
        case 'msg.update':
          this.onUpdate(frame as MsgUpdateFrame);
          break;
        case 'conv.update': {
          const update = frame as ConvUpdateFrame;
          // 自己操作的回声：本地已经改过了
          if (update.originDeviceId && update.originDeviceId === useAuthStore().deviceId) break;
          const conv = this.conversationOf(update.conversationId);
          if (conv) {
            conv.pinned = update.pinned;
            conv.muted = update.muted;
            void cacheConversations([conv]);
          }
          break;
        }
        case 'typing.push': {
          const typing = frame as TypingPushFrame;
          if (typing.userId === useAuthStore().userId) break;
          const key = typing.conversationId;
          if (!typing.typing) {
            delete this.typing[key];
            if (typingTimers[key]) {
              clearTimeout(typingTimers[key]);
              delete typingTimers[key];
            }
            break;
          }
          this.typing[key] = { userId: typing.userId, at: Date.now() };
          if (typingTimers[key]) clearTimeout(typingTimers[key]);
          typingTimers[key] = window.setTimeout(() => {
            delete this.typing[key];
            delete typingTimers[key];
          }, TYPING_TTL);
          break;
        }
        case 'group.update': {
          const update = frame as GroupUpdateFrame;
          // 入群申请/审批这几类变更的做法与「改群名」不同，交给 contact store 分流
          useContactStore().onGroupUpdate(update);
          void useContactStore().refreshAllGroups();
          break;
        }
        case 'session.login': {
          const login = frame as SessionLoginFrame;
          const auth = useAuthStore();
          if (login.deviceId && login.deviceId !== auth.deviceId) {
            useUiStore().toast(`你的账号在「${login.deviceId}」上登录了`, 'info', 3000);
          }
          break;
        }
        case 'session.kick': {
          const kick = frame as SessionKickFrame;
          this.kickedReason = kick.reason || 'kicked';
          break;
        }
        case 'session.migrate': {
          const migrate = frame as SessionMigrateFrame;
          const base = Math.max(migrate.retryAfterMs || 0, 600);
          imSocket.reconnectAfter(Math.round(base * (0.8 + Math.random() * 0.4)), migrate.reason || 'migrate');
          useUiStore().setBanner('服务端正在迁移，正在重新连接…', 'info');
          break;
        }
        case 'error': {
          const error = frame as ErrorFrame;
          if (error.clientMsgId) {
            this.markFailed(error.clientMsgId, error.message);
          }
          if (error.code === 401) {
            this.kickedReason = error.message || '登录状态失效';
          } else if (error.code !== 0) {
            useUiStore().error(error.message || `发送失败（${error.code}）`);
          }
          break;
        }
        default:
          // 未知帧忽略即可：服务端加字段/加类型时老客户端不能崩
          break;
      }
    },

    /**
     * 主动要一次「新端回放」：服务端把最近 M 个会话的最近 N 条重新推一遍。
     *
     * <p>服务端只在这台设备<b>第一次上线</b>时自动回放。本地 IndexedDB 被清空
     * （用户清缓存、换设备但复用了同一个 deviceId）时自动路径不会触发，
     * 「重新同步最近消息」这类按钮就调这里。
     *
     * <p>重复回放是安全的：回放帧与实时帧同构，`mergeMessage` 按 seq / clientMsgId 合并。
     */
    requestReplay(): void {
      imSocket.send({ type: 'sync', watermarks: watermarksOf(this.conversations), replay: true });
    },

    /* ---------------------------- 收消息/回执 ---------------------------- */

    onPush(frame: MsgPushFrame): void {
      const auth = useAuthStore();
      const self = auth.userId;
      const isSelf = frame.from === self;
      const convId = frame.conversationId;

      const message: ChatMessage = {
        key: `${convId}|${frame.clientMsgId || frame.serverMsgId}`,
        conversationId: convId,
        convType: frame.convType,
        seq: frame.seq,
        serverMsgId: frame.serverMsgId,
        clientMsgId: frame.clientMsgId || '',
        from: frame.from,
        to: frame.to,
        body: frame.recalled ? '' : frame.body,
        contentType: frame.contentType,
        attrs: frame.recalled ? undefined : frame.attrs,
        recalled: frame.recalled ?? false,
        ts: frame.ts || Date.now(),
        status: frame.recalled ? 'recalled' : 'sent'
      };

      this.mergeMessage(message);
      this.touchConversation(frame);
      void cacheMessages([message]);
      this.queueRecv(convId, frame.seq);

      const currentVisible = this.activeConversation === convId && document.visibilityState === 'visible';
      if (!isSelf) {
        if (currentVisible) {
          this.markRead(convId);
        } else {
          beep();
          vibrate([20, 40, 20]);
          const contact = useContactStore();
          const title = frame.convType === 'GROUP' ? contact.groupNameOf(convId) : contact.displayName(frame.from);
          notifyMessage(title, digestOf(message), () => {
            this.activeConversation = convId;
            location.hash = `#/chat/${encodeURIComponent(convId)}?type=${frame.convType}`;
          });
        }
      }
      // 自己其它端发的消息（多端同步）：把本地 pending 的那条合并掉，不重复提醒
      if (isSelf) {
        const local = this.messagesOf(convId).find((msg) => msg.clientMsgId === frame.clientMsgId);
        if (local) {
          local.seq = frame.seq;
          local.serverMsgId = frame.serverMsgId;
          local.status = 'sent';
          this.clearAckTimer(frame.clientMsgId);
        }
      }
    },

    /** 合并：同一 seq 或同一 clientMsgId 只留一条（离线补发、多端回显都会撞上） */
    mergeMessage(incoming: ChatMessage): void {
      const list = this.messages[incoming.conversationId] ?? (this.messages[incoming.conversationId] = []);
      const existing = list.find(
        (msg) =>
          (incoming.clientMsgId && msg.clientMsgId === incoming.clientMsgId) ||
          (incoming.seq > 0 && msg.seq === incoming.seq)
      );
      if (existing) {
        existing.seq = incoming.seq || existing.seq;
        existing.serverMsgId = incoming.serverMsgId || existing.serverMsgId;
        existing.ts = incoming.ts || existing.ts;
        existing.body = incoming.body;
        existing.contentType = incoming.contentType;
        existing.attrs = incoming.attrs;
        existing.recalled = incoming.recalled;
        if (incoming.recalled) existing.status = 'recalled';
        else if (existing.status === 'sending' || existing.status === 'failed') existing.status = 'sent';
        sortMessages(list);
        return;
      }
      list.push(incoming);
      sortMessages(list);
    },

    touchConversation(frame: MsgPushFrame): void {
      const conv = this.conversationOf(frame.conversationId);
      if (!conv) {
        void this.refreshConversations(true);
        return;
      }
      conv.lastSeq = Math.max(conv.lastSeq, frame.seq);
      conv.lastTime = Math.max(conv.lastTime, frame.ts || Date.now());
      conv.lastDigest = digestOf({
        contentType: frame.contentType,
        body: frame.recalled ? '' : frame.body,
        attrs: frame.recalled ? undefined : frame.attrs,
        recalled: frame.recalled
      });
      conv.lastFrom = frame.from;
      conv.lastMsgId = frame.serverMsgId;
      if (frame.from !== useAuthStore().userId) {
        conv.unread = Math.max(0, conv.lastSeq - conv.readSeq);
      }
      void cacheConversations([conv]);
    },

    queueRecv(conversationId: string, seq: number): void {
      if (seq <= 0) return;
      recvBuffer.push({ conversationId, seq });
      if (recvTimer) return;
      recvTimer = window.setTimeout(() => {
        recvTimer = 0;
        const acks = recvBuffer.splice(0, recvBuffer.length);
        if (acks.length) imSocket.send({ type: 'msg.recv', acks });
      }, RECV_BATCH_DELAY);
    },

    onAck(frame: MsgAckFrame): void {
      const convId = pendingConv.get(frame.clientMsgId);
      if (!convId) return;
      const msg = this.messagesOf(convId).find((item) => item.clientMsgId === frame.clientMsgId);
      this.clearAckTimer(frame.clientMsgId);
      pendingConv.delete(frame.clientMsgId);
      if (!msg) return;

      if (frame.code !== 0) {
        msg.status = 'failed';
        useUiStore().error(`发送失败（${frame.code}）`);
        return;
      }
      msg.seq = frame.seq;
      msg.serverMsgId = frame.serverMsgId;
      msg.status = 'sent';
      sortMessages(this.messages[convId]);
      void cacheMessages([msg]);

      const conv = this.conversationOf(convId);
      if (conv) {
        conv.lastSeq = Math.max(conv.lastSeq, frame.seq);
        conv.lastTime = Date.now();
        void cacheConversations([conv]);
      }
    },

    onStateFrame(frame: MsgStateFrame): void {
      const self = useAuthStore().userId;
      if (frame.userId === self) return; // 自己的已读由本地推进
      const perConv = this.watermarks[frame.conversationId] ?? (this.watermarks[frame.conversationId] = {});
      const entry = perConv[frame.userId] ?? (perConv[frame.userId] = { delivered: 0, read: 0 });
      if (frame.state === 'READ') {
        entry.read = Math.max(entry.read, frame.seq);
        entry.delivered = Math.max(entry.delivered, frame.seq);
      } else {
        entry.delivered = Math.max(entry.delivered, frame.seq);
      }
      this.recomputeStatuses(frame.conversationId);
    },

    /** 目标是「每条消息取所有回执里最高的那一档」：READ 覆盖 DELIVERED，DELIVERED 覆盖 sent */
    recomputeStatuses(conversationId: string): void {
      const perConv = this.watermarks[conversationId];
      if (!perConv) return;
      const entries = Object.values(perConv);
      if (!entries.length) return;
      const maxDelivered = Math.max(...entries.map((entry) => entry.delivered));
      const maxRead = Math.max(...entries.map((entry) => entry.read));
      const self = useAuthStore().userId;

      (this.messages[conversationId] ?? []).forEach((msg) => {
        if (msg.from !== self || msg.seq <= 0) return;
        if (msg.status === 'recalled') return;
        let next: LocalMsgState = msg.status === 'read' || msg.status === 'delivered' ? 'sent' : msg.status;
        if (maxDelivered >= msg.seq) next = 'delivered';
        if (maxRead >= msg.seq) next = 'read';
        if (msg.status !== 'failed' && msg.status !== 'sending') msg.status = next;
      });
    },

    onUpdate(frame: MsgUpdateFrame): void {
      const list = this.messages[frame.conversationId];
      if (!list) return;
      const msg = list.find((item) => item.seq === frame.seq);
      if (!msg) return;
      if (frame.change === 'RECALLED') {
        msg.recalled = true;
        msg.status = 'recalled';
        msg.body = '';
        msg.attrs = undefined;
        void cacheMessages([msg]);
        const conv = this.conversationOf(frame.conversationId);
        if (conv) conv.lastDigest = '[消息已撤回]';
      } else {
        msg.deleted = true;
        this.messages[frame.conversationId] = list.filter((item) => item.key !== msg.key);
        void deleteCachedMessage(msg.key);
      }
    },

    /* ------------------------------ 会话列表 ------------------------------ */

    async refreshConversations(silent = false): Promise<void> {
      const auth = useAuthStore();
      if (!auth.userId) return;
      try {
        const data = await ConvApi.list(auth.userId, 100);
        const list = Array.isArray(data?.conversations) ? data.conversations : [];
        // 服务端是标记（pinned/muted）与未读的权威来源
        this.conversations = list;
        await cacheConversations(list);
      } catch (e) {
        if (!silent) useUiStore().error((e as ApiError).message || '会话列表加载失败');
      }
    },

    /** 从联系人/搜索结果进入会话时，先造一个空会话，UI 才有标题可显示 */
    ensureConversation(conversationId: string, convType: ConvType): ConversationItem {
      const hit = this.conversationOf(conversationId);
      if (hit) return hit;
      const stub: ConversationItem = {
        conversationId,
        type: convType,
        lastSeq: 0,
        lastTime: Date.now(),
        lastFrom: '',
        lastDigest: '',
        lastMsgId: '',
        readSeq: 0,
        unread: 0,
        pinned: false,
        muted: false
      };
      this.conversations.push(stub);
      return stub;
    },

    async setFlag(conversationId: string, flag: { pinned?: boolean; muted?: boolean }): Promise<void> {
      const auth = useAuthStore();
      const conv = this.conversationOf(conversationId);
      try {
        const res = await ConvApi.flag(conversationId, auth.userId, { ...flag, deviceId: auth.deviceId });
        if (conv) {
          conv.pinned = res.pinned;
          conv.muted = res.muted;
          await cacheConversations([conv]);
        }
      } catch (e) {
        useUiStore().error((e as ApiError).message || '设置失败');
      }
    },

    /* ------------------------------ 历史消息 ------------------------------ */

    async openConversation(conversationId: string, convType: ConvType): Promise<void> {
      this.activeConversation = conversationId;
      this.ensureConversation(conversationId, convType);

      if (!this.messages[conversationId]) {
        const cached = await cachedMessages(conversationId, 60);
        this.messages[conversationId] = sortMessages(cached);
        this.hasMore[conversationId] = true;
      }
      // 缓存先渲染，再向服务端对齐最新一页（弱网下这一步失败也不影响看历史）
      try {
        const auth = useAuthStore();
        const page = await ConvApi.history(conversationId, auth.userId, 0, HISTORY_PAGE);
        const incoming = (page.messages ?? []).map((raw) => this.toLocal(raw));
        incoming.forEach((msg) => this.mergeMessage(msg));
        this.hasMore[conversationId] = page.hasMore ?? incoming.length >= HISTORY_PAGE;
        await cacheMessages(incoming);
        this.markRead(conversationId);
      } catch {
        /* 离线/服务端抖动：静默降级为「只看缓存」 */
      }
    },

    async loadMore(conversationId: string): Promise<void> {
      if (this.loadingHistory || this.hasMore[conversationId] === false) return;
      const list = this.messages[conversationId] ?? [];
      const oldest = list.find((msg) => msg.seq > 0);
      if (!oldest) return;
      this.loadingHistory = true;
      try {
        const auth = useAuthStore();
        const page = await ConvApi.history(conversationId, auth.userId, oldest.seq, HISTORY_PAGE);
        const incoming = (page.messages ?? []).map((raw) => this.toLocal(raw));
        incoming.forEach((msg) => this.mergeMessage(msg));
        this.hasMore[conversationId] = page.hasMore ?? incoming.length >= HISTORY_PAGE;
        await cacheMessages(incoming);
      } catch (e) {
        useUiStore().error((e as ApiError).message || '历史消息加载失败');
      } finally {
        this.loadingHistory = false;
      }
    },

    toLocal(raw: MsgPushFrame): ChatMessage {
      return {
        key: `${raw.conversationId}|${raw.clientMsgId || raw.serverMsgId}`,
        conversationId: raw.conversationId,
        convType: raw.convType,
        seq: raw.seq,
        serverMsgId: raw.serverMsgId,
        clientMsgId: raw.clientMsgId ?? '',
        from: raw.from,
        to: raw.to,
        body: raw.recalled ? '' : raw.body,
        contentType: raw.contentType,
        attrs: raw.recalled ? undefined : raw.attrs,
        recalled: raw.recalled ?? false,
        ts: raw.ts,
        status: raw.recalled ? 'recalled' : 'sent'
      };
    },

    /* ------------------------------ 发送消息 ------------------------------ */

    async sendText(to: string, convType: ConvType, body: string): Promise<ChatMessage | null> {
      const text = body.trim();
      if (!text) return null;
      return this.sendPayload(to, convType, { contentType: 'TEXT', body: text, attrs: undefined });
    },

    async sendMedia(to: string, convType: ConvType, payload: { contentType: ContentType; body: string; attrs?: string }): Promise<ChatMessage | null> {
      return this.sendPayload(to, convType, payload);
    },

    /**
     * 发送入口（文本/图片/语音/视频/文件都走这里）。
     *
     * 关键点：
     * - `clientMsgId` 是幂等键（`from|clientMsgId`），重发不会被服务端插两条；
     * - 先落本地（`sending`）再发帧，UI 立刻有气泡；
     * - 长连接不可用时帧会进 `ImSocket` 的发送队列，重连后自动补发。
     */
    async sendPayload(
      to: string,
      convType: ConvType,
      payload: { contentType: ContentType; body: string; attrs?: string }
    ): Promise<ChatMessage | null> {
      const auth = useAuthStore();
      const self = auth.userId;
      if (!self) return null;
      const conversationId = convType === 'GROUP' ? groupConversationId(to) : singleConversationId(self, to);
      const clientMsgId = uuid();
      const now = Date.now();

      const message: ChatMessage = {
        key: `${conversationId}|${clientMsgId}`,
        conversationId,
        convType,
        seq: 0,
        serverMsgId: '',
        clientMsgId,
        from: self,
        to,
        body: payload.body,
        contentType: payload.contentType,
        attrs: payload.attrs,
        ts: now,
        status: 'sending'
      };

      this.ensureConversation(conversationId, convType);
      this.messages[conversationId] = this.messages[conversationId] ?? [];
      this.messages[conversationId].push(message);
      sortMessages(this.messages[conversationId]);

      const conv = this.conversationOf(conversationId);
      if (conv) {
        conv.lastTime = now;
        conv.lastFrom = self;
        conv.lastDigest = digestOf(message);
      }
      await cacheMessages([message]);

      pendingConv.set(clientMsgId, conversationId);
      this.armAckTimer(clientMsgId);

      imSocket.send({
        type: 'msg.send',
        clientMsgId,
        to,
        body: payload.body,
        convType,
        conversationId,
        clientTs: now,
        contentType: payload.contentType,
        attrs: payload.attrs
      });
      return message;
    },

    /** 重发：复用原 clientMsgId，服务端按幂等键去重 */
    async resend(message: ChatMessage): Promise<void> {
      message.status = 'sending';
      message.ts = Date.now();
      pendingConv.set(message.clientMsgId, message.conversationId);
      this.armAckTimer(message.clientMsgId);
      imSocket.send({
        type: 'msg.send',
        clientMsgId: message.clientMsgId,
        to: message.to,
        body: message.body,
        convType: message.convType,
        conversationId: message.conversationId,
        clientTs: message.ts,
        contentType: message.contentType,
        attrs: message.attrs
      });
      await cacheMessages([message]);
    },

    armAckTimer(clientMsgId: string): void {
      this.clearAckTimer(clientMsgId);
      ackTimers.set(
        clientMsgId,
        window.setTimeout(() => {
          ackTimers.delete(clientMsgId);
          this.markFailed(clientMsgId, '发送超时');
        }, ACK_TIMEOUT)
      );
    },

    clearAckTimer(clientMsgId: string): void {
      const timer = ackTimers.get(clientMsgId);
      if (timer) {
        clearTimeout(timer);
        ackTimers.delete(clientMsgId);
      }
    },

    markFailed(clientMsgId: string, reason?: string): void {
      const convId = pendingConv.get(clientMsgId);
      const msg = convId ? this.messagesOf(convId).find((item) => item.clientMsgId === clientMsgId) : undefined;
      this.clearAckTimer(clientMsgId);
      if (msg && msg.status !== 'sent' && msg.status !== 'read' && msg.status !== 'delivered') {
        msg.status = 'failed';
        void cacheMessages([msg]);
        if (reason) useUiStore().toast(`${reason}，点气泡右侧红点可重发`, 'error', 2600);
      }
    },

    /** REST 兜底发送：长连接彻底不可用（比如企业网络禁 WS）时由用户手动触发 */
    async sendViaHttpFallback(message: ChatMessage): Promise<void> {
      const auth = useAuthStore();
      try {
        const res = await sendViaRest({
          from: auth.userId,
          to: message.to,
          body: message.body,
          clientMsgId: message.clientMsgId,
          convType: message.convType,
          contentType: message.contentType,
          attrs: message.attrs,
          clientTs: message.ts
        });
        message.seq = res.seq;
        message.serverMsgId = res.serverMsgId;
        message.status = 'sent';
        await cacheMessages([message]);
      } catch (e) {
        message.status = 'failed';
        useUiStore().error((e as ApiError).message || '发送失败');
      }
    },

    /* ------------------------------ 已读/撤回 ------------------------------ */

    markRead(conversationId: string): void {
      const conv = this.conversationOf(conversationId);
      if (!conv) return;
      const list = this.messages[conversationId] ?? [];
      const maxSeq = list.reduce((max, msg) => (msg.seq > max ? msg.seq : max), conv.lastSeq);
      if (maxSeq <= conv.readSeq) {
        conv.unread = 0;
        return;
      }
      conv.readSeq = maxSeq;
      conv.unread = 0;
      void cacheConversations([conv]);
      // 长连接可用时走 WS（省一次 TLS 往返），否则退化为 REST
      if (imSocket.isOpen()) {
        imSocket.send({ type: 'msg.read', conversationId, seq: maxSeq });
      } else {
        const auth = useAuthStore();
        ConvApi.read(conversationId, auth.userId, maxSeq).catch(() => {
          /* 下次打开会话会再推进一次 */
        });
      }
    },

    /**
     * 拉一条消息的群已读聚合（`seq=0` = 会话最新一条）。
     *
     * <p>群里没有逐条回执帧（扇出是「成员数 × 消息数」），已读人数只能按需聚合；
     * 服务端有 2 秒快照缓存，这里不再叠客户端缓存，只把结果按 seq 存一份给 UI 反复渲染。
     */
    async loadReadStat(conversationId: string, seq = 0): Promise<ReadStatItem | null> {
      const auth = useAuthStore();
      try {
        const res = await ReadStatApi.stat(conversationId, auth.userId, seq);
        if (res.code !== 0 || !res.conversationId) {
          useUiStore().toast(res.message || '已读情况暂时查不到', 'error');
          return null;
        }
        const stat = res as ReadStatItem;
        this.readStats[`${conversationId}#${stat.seq}`] = stat;
        return stat;
      } catch (e) {
        useUiStore().error((e as ApiError).message || '已读情况加载失败');
        return null;
      }
    },

    async recall(message: ChatMessage): Promise<void> {
      if (!message.seq) return;
      const auth = useAuthStore();
      try {
        await ConvApi.recall(message.conversationId, message.seq, auth.userId);
        message.recalled = true;
        message.status = 'recalled';
        message.body = '';
        message.attrs = undefined;
        await cacheMessages([message]);
      } catch (e) {
        useUiStore().error((e as ApiError).message || '撤回失败');
      }
    },

    async removeMessage(message: ChatMessage): Promise<void> {
      const remove = (): void => {
        const list = this.messages[message.conversationId] ?? [];
        this.messages[message.conversationId] = list.filter((item) => item.key !== message.key);
        void deleteCachedMessage(message.key);
      };
      if (!message.seq) {
        remove();
        return;
      }
      const auth = useAuthStore();
      try {
        await ConvApi.remove(message.conversationId, message.seq, auth.userId);
        remove();
      } catch (e) {
        // 404 说明服务端已经没了，本地也删掉
        if ((e as ApiError).code === 404) remove();
        else useUiStore().error((e as ApiError).message || '删除失败');
      }
    },

    /**
     * 上报「正在输入」。
     *
     * <p>节流与「什么时候算停止输入」都在 {@code ChatComposer}（4s 重发 / 静默 3s 补 false），
     * 这里只负责把帧发出去；服务端 {@code TypingNotifier} 再按 3s 窗口去抖后才扇出给对端。
     */
    setTyping(conversationId: string, convType: ConvType, typing: boolean): void {
      imSocket.send({ type: 'typing', conversationId, convType, typing, clientTs: Date.now() });
    }
  }
});
