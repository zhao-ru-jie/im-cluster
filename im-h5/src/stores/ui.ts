import { defineStore } from 'pinia';

export interface ToastItem {
  id: number;
  text: string;
  kind: 'info' | 'error' | 'success';
}

let seed = 0;

export const useUiStore = defineStore('ui', {
  state: () => ({
    toasts: [] as ToastItem[],
    /** 全局横幅（断线、被踢、迁移中），由 socket 状态驱动 */
    banner: '' as string,
    bannerKind: 'info' as 'info' | 'error'
  }),
  actions: {
    toast(text: string, kind: ToastItem['kind'] = 'info', duration = 2200): void {
      seed += 1;
      const id = seed;
      this.toasts.push({ id, text, kind });
      window.setTimeout(() => {
        this.toasts = this.toasts.filter((item) => item.id !== id);
      }, duration);
    },
    error(text: string): void {
      this.toast(text, 'error', 3000);
    },
    success(text: string): void {
      this.toast(text, 'success');
    },
    setBanner(text: string, kind: 'info' | 'error' = 'info'): void {
      this.banner = text;
      this.bannerKind = kind;
    },
    clearBanner(): void {
      this.banner = '';
    }
  }
});
