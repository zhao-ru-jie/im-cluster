import type { DownlinkFrame, UplinkFrame } from '@/types/protocol';

export type SocketState = 'idle' | 'connecting' | 'open' | 'closed' | 'kicked';

export interface SocketIdentity {
  /** ws://host:8081/ws */
  url: string;
  userId: string;
  deviceId: string;
  token?: string;
}

export interface StateInfo {
  /** 断线原因（`session.kick` 的 reason / 网络错误信息） */
  reason?: string;
  /** 第几次重连尝试，0 = 首次连接 */
  attempt?: number;
  /** 下一次重连的等待时间，UI 可以显示「3 秒后重试」 */
  retryInMs?: number;
}

type FrameListener = (frame: DownlinkFrame) => void;
type StateListener = (state: SocketState, info: StateInfo) => void;

const OUTBOX_LIMIT = 200;
const MAX_BACKOFF = 15_000;
const BASE_BACKOFF = 500;
/** 超过这个时间没收到任何帧，就认为连接已经是「僵尸」——移动网络切换时非常常见 */
const ZOMBIE_MS = 75_000;
/** 主动心跳间隔：既刷新对端活跃时间，也用来探测半开连接 */
const PING_INTERVAL = 30_000;

/**
 * 长连接管理器：重连、心跳、发送队列都在这一层。
 *
 * 对上只暴露三件事：`onFrame` / `onState` / `send`，业务状态全部由 store 处理。
 *
 * 几个刻意的设计：
 * 1. **未连接时 send 不丢**：入队（上限 200 条），连接可用后按原顺序 flush；
 * 2. **`session.kick` 不再重连**：被踢后重连只会被再踢一次，必须由用户换 deviceId 重新登录；
 * 3. **`session.migrate` 立刻重连但不清退避**：摘流/缩容是正常运维动作，按服务端给的
 *    `retryAfterMs` + 抖动连接即可；
 * 4. **僵尸连接靠「收帧时间」判活**，而不是只看 `readyState`——切基站时 socket 常常还是 OPEN。
 */
export class ImSocket {
  private ws: WebSocket | null = null;
  private identity: SocketIdentity | null = null;
  private state: SocketState = 'idle';
  private attempts = 0;
  private reconnectTimer: number | null = null;
  private heartbeatTimer: number | null = null;
  private lastFrameAt = 0;
  private manuallyClosed = false;
  private outbox: string[] = [];

  private readonly frameListeners = new Set<FrameListener>();
  private readonly stateListeners = new Set<StateListener>();

  constructor() {
    if (typeof window !== 'undefined') {
      window.addEventListener('online', this.onNetworkBack);
      document.addEventListener('visibilitychange', this.onVisibilityChange);
    }
  }

  getState(): SocketState {
    return this.state;
  }

  isOpen(): boolean {
    return this.state === 'open' && this.ws?.readyState === 1;
  }

  getIdentity(): SocketIdentity | null {
    return this.identity;
  }

  onFrame(listener: FrameListener): () => void {
    this.frameListeners.add(listener);
    return () => this.frameListeners.delete(listener);
  }

  onState(listener: StateListener): () => void {
    this.stateListeners.add(listener);
    listener(this.state, {});
    return () => this.stateListeners.delete(listener);
  }

  connect(identity: SocketIdentity): void {
    this.identity = identity;
    this.manuallyClosed = false;
    this.clearTimers();
    this.open();
  }

  /** 主动断开（退出登录 / 卸载页面） */
  close(reason = 'closed-by-client'): void {
    this.manuallyClosed = true;
    this.clearTimers();
    this.attempts = 0;
    this.outbox = [];
    const ws = this.ws;
    this.ws = null;
    if (ws) {
      try {
        ws.close(1000, reason);
      } catch {
        /* 忽略：已经关掉了 */
      }
    }
    this.setState('idle', { reason });
  }

  /**
   * 发送一帧。返回 false 表示进了队列（说明当前不在线）。
   * msg.recv / msg.read 这类「过期即无意义」的帧，调用方可以忽略返回值。
   */
  send(frame: UplinkFrame | Record<string, unknown>): boolean {
    const text = JSON.stringify({ ver: 1, ...frame });
    if (this.isOpen() && this.ws) {
      try {
        this.ws.send(text);
        return true;
      } catch {
        /* 落到队列里，等重连后重发 */
      }
    }
    if (this.outbox.length >= OUTBOX_LIMIT) this.outbox.shift();
    this.outbox.push(text);
    return false;
  }

  /**
   * 按服务端 `retryAfterMs` 重连（`session.migrate`）。
   *
   * 不重置退避次数：迁移可能是连续两次（先摘流再缩容），不该被当成「网络恢复」。
   */
  reconnectAfter(delayMs: number, reason = 'migrate'): void {
    const ws = this.ws;
    this.ws = null;
    if (ws) {
      try {
        ws.close(1000, reason);
      } catch {
        /* ignore */
      }
    }
    this.setState('closed', { reason, retryInMs: delayMs });
    this.scheduleReconnect(delayMs);
  }

  /* ------------------------------ 内部实现 ------------------------------ */

  private open(): void {
    if (!this.identity) return;
    const { url, userId, deviceId, token } = this.identity;
    this.setState('connecting', { attempt: this.attempts });

    const query = new URLSearchParams({ userId, deviceId });
    if (token) query.set('token', token);
    // https 页面必须用 wss（微信里混用会被直接拦掉），这里顺手纠正一下协议
    const safeUrl = location.protocol === 'https:' ? url.replace(/^ws:/, 'wss:') : url;

    let ws: WebSocket;
    try {
      ws = new WebSocket(`${safeUrl}?${query.toString()}`);
    } catch (e) {
      this.setState('closed', { reason: (e as Error).message });
      this.scheduleReconnect();
      return;
    }
    this.ws = ws;
    this.lastFrameAt = Date.now();

    ws.onopen = () => {
      this.attempts = 0;
      this.lastFrameAt = Date.now();
      this.setState('open', {});
      this.startHeartbeat();
      this.flushOutbox();
    };

    ws.onmessage = (event: MessageEvent) => {
      this.lastFrameAt = Date.now();
      if (typeof event.data !== 'string') return;
      let frame: DownlinkFrame;
      try {
        frame = JSON.parse(event.data) as DownlinkFrame;
      } catch {
        // 脏帧不要断连：服务端升级加字段时老客户端也得活着
        return;
      }
      this.handleControlFrame(frame);
      this.frameListeners.forEach((listener) => {
        try {
          listener(frame);
        } catch (e) {
          console.error('[ws] frame handler failed', e);
        }
      });
    };

    ws.onerror = () => {
      // onerror 之后一定会有 onclose，这里只记录，不重复调度
      this.setState('closed', { reason: '网络异常' });
    };

    ws.onclose = (event: CloseEvent) => {
      this.stopHeartbeat();
      if (this.ws === ws) this.ws = null;
      if (this.manuallyClosed || this.state === 'kicked') return;
      const reason = event.reason || (event.code === 1006 ? '网络中断' : `connection closed (${event.code})`);
      this.setState('closed', { reason });
      this.scheduleReconnect();
    };
  }

  /** ping/pong 在连接层就消化掉，业务层不必关心 */
  private handleControlFrame(frame: DownlinkFrame): void {
    const type = (frame as { type?: string }).type;
    if (type === 'ping') {
      this.send({ type: 'pong', ts: Date.now() });
    } else if (type === 'session.kick') {
      const reason = (frame as { reason?: string }).reason ?? 'kicked';
      this.manuallyClosed = true;
      this.clearTimers();
      this.setState('kicked', { reason });
    }
  }

  private flushOutbox(): void {
    if (!this.isOpen() || !this.ws) return;
    const pending = this.outbox;
    this.outbox = [];
    pending.forEach((text) => {
      try {
        this.ws?.send(text);
      } catch {
        this.outbox.push(text);
      }
    });
  }

  private scheduleReconnect(delayMs?: number): void {
    if (this.manuallyClosed || !this.identity) return;
    this.clearTimers();
    const wait = delayMs ?? this.backoff();
    this.setState(this.state, { retryInMs: wait });
    this.reconnectTimer = window.setTimeout(() => {
      this.reconnectTimer = null;
      this.open();
    }, wait);
  }

  private backoff(): number {
    const raw = Math.min(MAX_BACKOFF, BASE_BACKOFF * Math.pow(2, this.attempts));
    this.attempts += 1;
    // ±30% 抖动：大量客户端同时断线时不要齐步走
    const jitter = raw * (0.7 + Math.random() * 0.6);
    return Math.round(jitter);
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    this.heartbeatTimer = window.setInterval(() => {
      if (!this.isOpen()) return;
      if (Date.now() - this.lastFrameAt > ZOMBIE_MS) {
        // 半开连接：主动丢掉，交给重连逻辑
        const ws = this.ws;
        this.ws = null;
        try {
          ws?.close(4000, 'zombie');
        } catch {
          /* ignore */
        }
        this.setState('closed', { reason: '连接已失活' });
        this.scheduleReconnect();
        return;
      }
      this.send({ type: 'ping', ts: Date.now() });
    }, PING_INTERVAL);
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer !== null) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  private clearTimers(): void {
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.stopHeartbeat();
  }

  private setState(state: SocketState, info: StateInfo): void {
    this.state = state;
    this.stateListeners.forEach((listener) => {
      try {
        listener(state, info);
      } catch (e) {
        console.error('[ws] state handler failed', e);
      }
    });
  }

  private readonly onNetworkBack = (): void => {
    // 网络恢复：立刻重连一次，别等退避到 15s
    if (this.manuallyClosed || this.ws) return;
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.attempts = 0;
    this.open();
  };

  private readonly onVisibilityChange = (): void => {
    // 移动端切回前台时连接经常已经死了，但 readyState 仍是 OPEN
    if (document.visibilityState === 'visible' && !this.manuallyClosed) {
      if (!this.ws) {
        this.attempts = 0;
        this.open();
      } else if (Date.now() - this.lastFrameAt > PING_INTERVAL) {
        this.send({ type: 'ping', ts: Date.now() });
      }
    }
  };
}

/** 单例：整个应用只维持一条长连接 */
export const imSocket = new ImSocket();
