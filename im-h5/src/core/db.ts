import type { ChatMessage, UserProfile } from '@/types/local';
import type { ConversationItem } from '@/types/protocol';

/**
 * 本地缓存（IndexedDB）。
 *
 * 为什么必须有：服务端的历史接口是**按会话分页**的，切会话就重新拉一次会很慢；
 * 而 H5 用户大部分时间是「进聊天页 → 看最近几条 → 上滑补历史」。
 * 把最近的消息与会话快照落到本地，冷启动可以先行渲染，再静默对齐。
 *
 * 微信内置浏览器 / 隐私模式下 IndexedDB 可能不可用，这里降级为内存实现：
 * 功能不受影响（只是刷新后丢缓存），不会抛错让页面白屏。
 */

const DB_NAME = 'im-h5';
const DB_VERSION = 1;

const STORE_MESSAGES = 'messages';
const STORE_CONVERSATIONS = 'conversations';
const STORE_PROFILES = 'profiles';

let dbPromise: Promise<IDBDatabase | null> | null = null;

function openDb(): Promise<IDBDatabase | null> {
  if (dbPromise) return dbPromise;
  dbPromise = new Promise((resolve) => {
    if (typeof indexedDB === 'undefined') {
      resolve(null);
      return;
    }
    let request: IDBOpenDBRequest;
    try {
      request = indexedDB.open(DB_NAME, DB_VERSION);
    } catch {
      resolve(null);
      return;
    }
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(STORE_MESSAGES)) {
        const store = db.createObjectStore(STORE_MESSAGES, { keyPath: 'key' });
        store.createIndex('byConv', 'conversationId', { unique: false });
        store.createIndex('byConvSeq', ['conversationId', 'seq'], { unique: false });
        store.createIndex('byTs', 'ts', { unique: false });
      }
      if (!db.objectStoreNames.contains(STORE_CONVERSATIONS)) {
        db.createObjectStore(STORE_CONVERSATIONS, { keyPath: 'conversationId' });
      }
      if (!db.objectStoreNames.contains(STORE_PROFILES)) {
        db.createObjectStore(STORE_PROFILES, { keyPath: 'userId' });
      }
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => resolve(null);
    request.onblocked = () => resolve(null);
  });
  return dbPromise;
}

/* ---------------------------- 内存降级实现 ---------------------------- */

const memMessages = new Map<string, ChatMessage>();
const memConversations = new Map<string, ConversationItem>();
const memProfiles = new Map<string, UserProfile>();

function tx<T>(
  storeName: string,
  mode: IDBTransactionMode,
  run: (store: IDBObjectStore) => IDBRequest<T> | null
): Promise<T | null> {
  return openDb().then(
    (db) =>
      new Promise<T | null>((resolve) => {
        if (!db) {
          resolve(null);
          return;
        }
        try {
          const transaction = db.transaction(storeName, mode);
          const request = run(transaction.objectStore(storeName));
          if (!request) {
            resolve(null);
            return;
          }
          request.onsuccess = () => resolve(request.result as T);
          request.onerror = () => resolve(null);
        } catch {
          resolve(null);
        }
      })
  );
}

/* ------------------------------ 消息 ------------------------------ */

export async function cacheMessages(messages: ChatMessage[]): Promise<void> {
  if (messages.length === 0) return;
  messages.forEach((m) => memMessages.set(m.key, m));
  const db = await openDb();
  if (!db) return;
  try {
    const transaction = db.transaction(STORE_MESSAGES, 'readwrite');
    const store = transaction.objectStore(STORE_MESSAGES);
    messages.forEach((m) => store.put(m));
  } catch {
    /* 缓存失败不影响主流程 */
  }
}

export async function deleteCachedMessage(key: string): Promise<void> {
  memMessages.delete(key);
  await tx(STORE_MESSAGES, 'readwrite', (store) => store.delete(key));
}

/** 某个会话最近的消息（按 seq/ts 升序返回） */
export async function cachedMessages(conversationId: string, limit = 60): Promise<ChatMessage[]> {
  const db = await openDb();
  if (!db) {
    return [...memMessages.values()]
      .filter((m) => m.conversationId === conversationId)
      .sort(bySeq)
      .slice(-limit);
  }
  const all = await new Promise<ChatMessage[]>((resolve) => {
    try {
      const transaction = db.transaction(STORE_MESSAGES, 'readonly');
      const index = transaction.objectStore(STORE_MESSAGES).index('byConv');
      const request = index.getAll(IDBKeyRange.only(conversationId));
      request.onsuccess = () => resolve((request.result as ChatMessage[]) ?? []);
      request.onerror = () => resolve([]);
    } catch {
      resolve([]);
    }
  });
  return all.sort(bySeq).slice(-limit);
}

function bySeq(a: ChatMessage, b: ChatMessage): number {
  if (a.seq === b.seq) return a.ts - b.ts;
  return a.seq - b.seq;
}

/** 本地搜索：服务端没有搜索接口，只能搜已缓存的消息 */
export async function searchCachedMessages(keyword: string, limit = 50): Promise<ChatMessage[]> {
  const kw = keyword.trim().toLowerCase();
  if (!kw) return [];
  const db = await openDb();
  let source: ChatMessage[];
  if (!db) {
    source = [...memMessages.values()];
  } else {
    source = await new Promise<ChatMessage[]>((resolve) => {
      try {
        const transaction = db.transaction(STORE_MESSAGES, 'readonly');
        const request = transaction.objectStore(STORE_MESSAGES).getAll();
        request.onsuccess = () => resolve((request.result as ChatMessage[]) ?? []);
        request.onerror = () => resolve([]);
      } catch {
        resolve([]);
      }
    });
  }
  return source
    .filter((m) => !m.deleted && !m.recalled && (m.body || '').toLowerCase().includes(kw))
    .sort((a, b) => b.ts - a.ts)
    .slice(0, limit);
}

/* ------------------------------ 会话 ------------------------------ */

export async function cacheConversations(items: ConversationItem[]): Promise<void> {
  items.forEach((item) => memConversations.set(item.conversationId, item));
  const db = await openDb();
  if (!db) return;
  try {
    const transaction = db.transaction(STORE_CONVERSATIONS, 'readwrite');
    const store = transaction.objectStore(STORE_CONVERSATIONS);
    items.forEach((item) => store.put(item));
  } catch {
    /* ignore */
  }
}

export async function cachedConversations(): Promise<ConversationItem[]> {
  const db = await openDb();
  if (!db) return [...memConversations.values()];
  const rows = await new Promise<ConversationItem[]>((resolve) => {
    try {
      const transaction = db.transaction(STORE_CONVERSATIONS, 'readonly');
      const request = transaction.objectStore(STORE_CONVERSATIONS).getAll();
      request.onsuccess = () => resolve((request.result as ConversationItem[]) ?? []);
      request.onerror = () => resolve([]);
    } catch {
      resolve([]);
    }
  });
  return rows.length ? rows : [...memConversations.values()];
}

/* ------------------------------ 资料 ------------------------------ */

export async function cacheProfile(profile: UserProfile): Promise<void> {
  memProfiles.set(profile.userId, profile);
  await tx(STORE_PROFILES, 'readwrite', (store) => store.put(profile) as unknown as IDBRequest<UserProfile>);
}

export async function cachedProfile(userId: string): Promise<UserProfile | null> {
  const hit = await tx<UserProfile>(STORE_PROFILES, 'readonly', (store) => store.get(userId));
  return hit ?? memProfiles.get(userId) ?? null;
}

export async function cachedProfiles(): Promise<Record<string, UserProfile>> {
  const rows = (await tx<UserProfile[]>(STORE_PROFILES, 'readonly', (store) => store.getAll())) ?? [];
  const map: Record<string, UserProfile> = {};
  memProfiles.forEach((value, key) => {
    map[key] = value;
  });
  rows.forEach((row) => {
    map[row.userId] = row;
  });
  return map;
}

/** 退出登录：只清当前账号相关内容（多账号共用一个库时会保留别人的） */
export async function clearAll(): Promise<void> {
  memMessages.clear();
  memConversations.clear();
  const db = await openDb();
  if (!db) return;
  ['messages', 'conversations'].forEach((name) => {
    try {
      db.transaction(name, 'readwrite').objectStore(name).clear();
    } catch {
      /* ignore */
    }
  });
}
