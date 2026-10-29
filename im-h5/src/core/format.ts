import type { MediaAttrs } from '@/types/protocol';
import type { ChatMessage } from '@/types/local';

const pad = (n: number): string => (n < 10 ? `0${n}` : String(n));

function isSameDay(a: number, b: number): boolean {
  const da = new Date(a);
  const db = new Date(b);
  return da.getFullYear() === db.getFullYear() && da.getMonth() === db.getMonth() && da.getDate() === db.getDate();
}

/** 会话列表右上角的时间：今天 HH:mm、昨天、本周 星期三、更早 MM-DD */
export function formatListTime(ts: number): string {
  if (!ts) return '';
  const now = Date.now();
  const date = new Date(ts);
  if (isSameDay(ts, now)) return `${pad(date.getHours())}:${pad(date.getMinutes())}`;
  const yesterday = now - 24 * 3600 * 1000;
  if (isSameDay(ts, yesterday)) return '昨天';
  const diffDays = Math.floor((now - ts) / (24 * 3600 * 1000));
  if (diffDays < 7) return ['周日', '周一', '周二', '周三', '周四', '周五', '周六'][date.getDay()];
  return `${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
}

/** 聊天页中间的时间分隔条：今天 HH:mm / 昨天 HH:mm / MM月DD日 HH:mm / YYYY年… */
export function formatChatTime(ts: number): string {
  if (!ts) return '';
  const date = new Date(ts);
  const hm = `${pad(date.getHours())}:${pad(date.getMinutes())}`;
  if (isSameDay(ts, Date.now())) return hm;
  if (isSameDay(ts, Date.now() - 24 * 3600 * 1000)) return `昨天 ${hm}`;
  if (date.getFullYear() === new Date().getFullYear()) {
    return `${date.getMonth() + 1}月${date.getDate()}日 ${hm}`;
  }
  return `${date.getFullYear()}年${date.getMonth() + 1}月${date.getDate()}日 ${hm}`;
}

export function formatDuration(ms: number): string {
  const total = Math.max(0, Math.round(ms / 1000));
  const minutes = Math.floor(total / 60);
  const seconds = total % 60;
  return `${minutes}:${pad(seconds)}`;
}

export function formatSize(bytes = 0): string {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)}KB`;
  return `${(bytes / 1024 / 1024).toFixed(1)}MB`;
}

/** 头像占位文字：取 userId 最后两个字符，中文名取前两个 */
export function avatarText(name: string): string {
  if (!name) return '?';
  const trimmed = name.trim();
  if (/^[\u4e00-\u9fa5]/.test(trimmed)) return trimmed.slice(0, 1);
  return trimmed.slice(0, 2).toUpperCase();
}

const AVATAR_COLORS = ['#5b8ff9', '#61c17b', '#f6a94a', '#e8684a', '#9270ca', '#4fb0c6', '#d96ba1'];

/** 同一个 userId 永远同一个颜色，省掉服务端头像时也有辨识度 */
export function avatarColor(seed: string): string {
  let hash = 0;
  for (let i = 0; i < seed.length; i += 1) hash = (hash * 31 + seed.charCodeAt(i)) % 100000;
  return AVATAR_COLORS[hash % AVATAR_COLORS.length];
}

export function parseAttrs(attrs?: string): MediaAttrs {
  if (!attrs) return {};
  try {
    const parsed = JSON.parse(attrs) as MediaAttrs;
    return parsed && typeof parsed === 'object' ? parsed : {};
  } catch {
    return {};
  }
}

export function stringifyAttrs(attrs: MediaAttrs): string {
  return JSON.stringify(attrs);
}

/** 会话列表里显示的摘要（服务端 lastDigest 只有 64 字符，本地消息用它兜底） */
export function digestOf(message: Pick<ChatMessage, 'contentType' | 'body' | 'attrs'> & { recalled?: boolean }): string {
  if (message.recalled) return '[消息已撤回]';
  const attrs = parseAttrs(message.attrs);
  switch (message.contentType) {
    case 'IMAGE':
      return '[图片]';
    case 'VOICE':
      return message.body || `[语音]${attrs.duration ? Math.round(attrs.duration / 1000) + '"' : ''}`;
    case 'VIDEO':
      return '[视频]';
    case 'FILE':
      return message.body || `[文件] ${attrs.name ?? ''}`.trim();
    default:
      return (message.body || '').slice(0, 60);
  }
}

/** 会话 ID 的真正名字：单聊取对方 userId，群聊是 g:{groupId} */
export function peerOfConversation(conversationId: string, selfId: string): string {
  if (conversationId.startsWith('s:')) {
    const parts = conversationId.slice(2).split(':');
    return parts.find((part) => part !== selfId) ?? parts[0] ?? conversationId;
  }
  return conversationId;
}

export function groupIdOfConversation(conversationId: string): string {
  return conversationId.startsWith('g:') ? conversationId.slice(2) : conversationId;
}

export function singleConversationId(a: string, b: string): string {
  // 与服务端 MessageEnvelope 的约定一致：两个 userId 排序后拼
  return a <= b ? `s:${a}:${b}` : `s:${b}:${a}`;
}

export function groupConversationId(groupId: string): string {
  return `g:${groupId}`;
}

export function clampText(text: string, max: number): string {
  return text.length <= max ? text : `${text.slice(0, max)}…`;
}
