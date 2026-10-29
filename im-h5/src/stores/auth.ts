import { defineStore } from 'pinia';
import { setRestBase } from '@/api/http';
import { cacheProfile, cachedProfile } from '@/core/db';
import type { UserProfile } from '@/types/local';

const AUTH_KEY = 'im.h5.auth';

export interface Endpoints {
  /** 空串 = 与页面同源 */
  restBase: string;
  /** ws://host:8081/ws */
  wsUrl: string;
}

interface PersistedAuth {
  userId: string;
  deviceId: string;
  token: string;
  endpoints: Endpoints;
}

/**
 * 默认接口地址。
 *
 * - 页面被 im-server 直接托管（同源，端口 8080/80/443）→ REST 用同源；
 * - 开发/分离部署（Vite 5173、静态服务器）→ 打同主机的 8080；
 * - 想改成反代同源，登录页把它清空即可。
 */
export function defaultEndpoints(): Endpoints {
  const host = location.hostname || '127.0.0.1';
  const httpProto = location.protocol === 'https:' ? 'https:' : 'http:';
  const wsProto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const sameOriginRest = ['', '80', '443', '8080'].includes(location.port) ? '' : `${httpProto}//${host}:8080`;
  const envRest = import.meta.env.VITE_REST_BASE as string | undefined;
  const envWs = import.meta.env.VITE_WS_BASE as string | undefined;
  return {
    restBase: envRest ?? sameOriginRest,
    wsUrl: envWs ?? `${wsProto}//${host}:8081/ws`
  };
}

function loadPersisted(): PersistedAuth | null {
  try {
    const raw = localStorage.getItem(AUTH_KEY);
    return raw ? (JSON.parse(raw) as PersistedAuth) : null;
  } catch {
    return null;
  }
}

export const useAuthStore = defineStore('auth', {
  state: () => ({
    userId: '',
    deviceId: 'web',
    token: '',
    endpoints: defaultEndpoints(),
    profile: null as UserProfile | null,
    /** 已从 localStorage 恢复过（main.ts 里调用 restore，避免路由守卫早于恢复） */
    restored: false
  }),
  getters: {
    loggedIn: (state): boolean => !!state.userId,
    wsUrl: (state): string => state.endpoints.wsUrl,
    /** 头像/昵称优先取本地资料 */
    displayName: (state) => (userId: string): string => {
      if (userId === state.userId && state.profile?.nickname) return state.profile.nickname;
      return userId;
    }
  },
  actions: {
    async restore(): Promise<void> {
      const saved = loadPersisted();
      if (saved) {
        this.userId = saved.userId;
        this.deviceId = saved.deviceId || 'web';
        this.token = saved.token || '';
        this.endpoints = { ...defaultEndpoints(), ...saved.endpoints };
        setRestBase(this.endpoints.restBase);
        this.profile = await cachedProfile(saved.userId);
        if (!this.profile) {
          this.profile = { userId: saved.userId, nickname: saved.userId, updatedAt: Date.now() };
        }
      } else {
        setRestBase(this.endpoints.restBase);
      }
      this.restored = true;
    },

    async login(input: { userId: string; deviceId: string; token?: string; endpoints?: Partial<Endpoints> }): Promise<void> {
      this.userId = input.userId.trim();
      this.deviceId = (input.deviceId || 'web').trim();
      this.token = (input.token ?? '').trim();
      this.endpoints = { ...this.endpoints, ...(input.endpoints ?? {}) };
      setRestBase(this.endpoints.restBase);

      const saved: PersistedAuth = {
        userId: this.userId,
        deviceId: this.deviceId,
        token: this.token,
        endpoints: this.endpoints
      };
      localStorage.setItem(AUTH_KEY, JSON.stringify(saved));
      // 设备 ID 变化时不复用别人的资料
      this.profile = (await cachedProfile(this.userId)) ?? {
        userId: this.userId,
        nickname: this.userId,
        updatedAt: Date.now()
      };
    },

    async saveProfile(patch: Pick<UserProfile, 'nickname'> & Partial<Pick<UserProfile, 'avatar' | 'signature'>>): Promise<void> {
      if (!this.userId) return;
      const profile: UserProfile = {
        userId: this.userId,
        nickname: patch.nickname.trim() || this.userId,
        avatar: patch.avatar,
        signature: patch.signature,
        updatedAt: Date.now()
      };
      this.profile = profile;
      await cacheProfile(profile);
    },

    logout(): void {
      this.userId = '';
      this.token = '';
      this.profile = null;
      localStorage.removeItem(AUTH_KEY);
    }
  }
});
