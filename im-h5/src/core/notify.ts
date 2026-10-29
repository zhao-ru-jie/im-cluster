/**
 * 新消息提醒。
 *
 * 服务端**没有** APNs/FCM 这类离线推送（`HttpVendorPush` 只是可选的外部中转，
 * 默认关闭且只 POST 给第三方），所以「离线推送」在 H5 里的等价物是：
 * 1. 断线/切后台期间的消息由服务端落离线队列，重连后 `sync` 水位 + `pullPending` 补回来；
 * 2. 页面在前台 → 应用内提示音 + 震动 + 未读红点；
 * 3. 页面在后台（PWA/桌面浏览器）→ Web Notification（需要用户授权）。
 *
 * 微信内置浏览器不支持 Notification，这里会静默降级。
 */

export function notifySupported(): boolean {
  return typeof window !== 'undefined' && 'Notification' in window;
}

export function notifyPermission(): NotificationPermission | 'unsupported' {
  return notifySupported() ? Notification.permission : 'unsupported';
}

export async function requestNotifyPermission(): Promise<boolean> {
  if (!notifySupported()) return false;
  if (Notification.permission === 'granted') return true;
  if (Notification.permission === 'denied') return false;
  try {
    const result = await Notification.requestPermission();
    return result === 'granted';
  } catch {
    return false;
  }
}

export function notifyMessage(title: string, body: string, onClick?: () => void): void {
  if (!notifySupported() || Notification.permission !== 'granted') return;
  // 页面可见时不弹系统通知：应用内的未读与提示音已经够了，重复提醒很吵
  if (document.visibilityState === 'visible') return;
  try {
    const notification = new Notification(title, { body, tag: title, renotify: false, silent: false } as NotificationOptions);
    notification.onclick = () => {
      window.focus();
      onClick?.();
      notification.close();
    };
    setTimeout(() => notification.close(), 6000);
  } catch {
    /* 部分浏览器在非 https 下会抛，忽略 */
  }
}

export function vibrate(pattern: number | number[] = 30): void {
  try {
    navigator.vibrate?.(pattern);
  } catch {
    /* ignore */
  }
}

let audioContext: AudioContext | null = null;

/** 首次用户手势后才能创建/恢复 AudioContext，见 main.ts 的 unlockAudio */
export function unlockAudio(): void {
  try {
    const Ctor = window.AudioContext ?? (window as unknown as { webkitAudioContext?: typeof AudioContext }).webkitAudioContext;
    if (!Ctor) return;
    if (!audioContext) audioContext = new Ctor();
    if (audioContext.state === 'suspended') void audioContext.resume();
  } catch {
    /* ignore */
  }
}

/** 极短的「叮」：不引入音频资源，两个正弦音叠一个指数衰减包络 */
export function beep(): void {
  const ctx = audioContext;
  if (!ctx || ctx.state !== 'running') return;
  try {
    const now = ctx.currentTime;
    const gain = ctx.createGain();
    gain.gain.setValueAtTime(0.0001, now);
    gain.gain.exponentialRampToValueAtTime(0.12, now + 0.01);
    gain.gain.exponentialRampToValueAtTime(0.0001, now + 0.18);
    gain.connect(ctx.destination);

    const osc = ctx.createOscillator();
    osc.type = 'sine';
    osc.frequency.setValueAtTime(880, now);
    osc.frequency.exponentialRampToValueAtTime(1320, now + 0.08);
    osc.connect(gain);
    osc.start(now);
    osc.stop(now + 0.2);
  } catch {
    /* ignore */
  }
}
