import type { ContentType, MediaAttrs } from '@/types/protocol';

/**
 * 媒体消息的客户端实现。
 *
 * ## 为什么是 data URL
 *
 * 服务端只有 IM 通道，**没有对象存储/上传接口**（`MessageEnvelope` 注释写得很清楚：
 * 文件本体应该由客户端直传 OSS，IM 只携带 URL）。所以这一版把媒体编码成 data URL 放进
 * `attrs.url` 里直接随消息发走：
 * - 好处：零依赖、单机可跑通全链路（发送 → 落库 → 推送 → 对端渲染）；
 * - 代价：受 WebSocket 单帧 **1 MiB** 限制，图片必须先压缩，语音/视频只能短片段。
 *
 * 真机部署时把 `imageToPayload` / `fileToPayload` 换成「上传 OSS → 拿 URL」即可，
 * 上层（store / 组件）不用改：它们只认 `attrs`。
 */

/** 1 MiB 的帧预算里，给 body/其它字段留出余量后允许的 attrs 体积 */
export const MAX_MEDIA_BYTES = 640 * 1024;

export interface MediaPayload {
  contentType: ContentType;
  attrs: MediaAttrs;
  /** 消息正文：媒体消息用摘要文本，列表页直接显示它 */
  body: string;
}

export class MediaTooLargeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'MediaTooLargeError';
  }
}

export function dataUrlBytes(dataUrl: string): number {
  const index = dataUrl.indexOf(',');
  const payload = index >= 0 ? dataUrl.slice(index + 1) : dataUrl;
  // base64 每 4 个字符 3 字节，去掉可能的 padding
  const padding = payload.endsWith('==') ? 2 : payload.endsWith('=') ? 1 : 0;
  return Math.floor((payload.length * 3) / 4) - padding;
}

export function readAsDataUrl(file: Blob): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result));
    reader.onerror = () => reject(new Error('读取文件失败'));
    reader.readAsDataURL(file);
  });
}

interface CompressResult {
  dataUrl: string;
  width: number;
  height: number;
  size: number;
}

/**
 * 图片压缩：长边不超过 `maxSide`，逐步降质直到落进 `budgetBytes`。
 *
 * 手机会直接给 3~8MB 的原图，不压缩的话连 base64 都放不进一帧里。
 */
export async function compressImage(file: File, maxSide = 1280, budgetBytes = MAX_MEDIA_BYTES): Promise<CompressResult> {
  const bitmap = await loadImage(file);
  const scale = Math.min(1, maxSide / Math.max(bitmap.width, bitmap.height));
  const width = Math.max(1, Math.round(bitmap.width * scale));
  const height = Math.max(1, Math.round(bitmap.height * scale));

  const canvas = document.createElement('canvas');
  canvas.width = width;
  canvas.height = height;
  const context = canvas.getContext('2d');
  if (!context) throw new Error('当前浏览器不支持图片处理');
  context.fillStyle = '#fff';
  context.fillRect(0, 0, width, height);
  context.drawImage(bitmap.source, 0, 0, width, height);
  bitmap.release();

  let quality = 0.72;
  let dataUrl = canvas.toDataURL('image/jpeg', quality);
  // 最多再降三轮：绝大多数手机照片两轮内就能进 640KB
  for (let i = 0; i < 3 && dataUrlBytes(dataUrl) > budgetBytes; i += 1) {
    quality = Math.max(0.4, quality - 0.12);
    dataUrl = canvas.toDataURL('image/jpeg', quality);
  }
  return { dataUrl, width, height, size: dataUrlBytes(dataUrl) };
}

async function loadImage(file: File): Promise<{ source: CanvasImageSource; width: number; height: number; release: () => void }> {
  if (typeof createImageBitmap === 'function') {
    try {
      const bitmap = await createImageBitmap(file);
      return {
        source: bitmap,
        width: bitmap.width,
        height: bitmap.height,
        release: () => bitmap.close()
      };
    } catch {
      /* iOS 老版本会在这里抛错，退回 Image 方案 */
    }
  }
  const url = URL.createObjectURL(file);
  const image = new Image();
  image.decoding = 'async';
  await new Promise<void>((resolve, reject) => {
    image.onload = () => resolve();
    image.onerror = () => reject(new Error('图片解析失败'));
    image.src = url;
  });
  return {
    source: image,
    width: image.naturalWidth,
    height: image.naturalHeight,
    release: () => URL.revokeObjectURL(url)
  };
}

export async function imageToPayload(file: File): Promise<MediaPayload> {
  const compressed = await compressImage(file);
  return {
    contentType: 'IMAGE',
    body: '[图片]',
    attrs: { url: compressed.dataUrl, width: compressed.width, height: compressed.height, size: compressed.size }
  };
}

export async function fileToPayload(file: File): Promise<MediaPayload> {
  if (file.size > MAX_MEDIA_BYTES) {
    throw new MediaTooLargeError(
      `文件 ${(file.size / 1024).toFixed(0)}KB 超过 ${(MAX_MEDIA_BYTES / 1024).toFixed(0)}KB 上限（未接对象存储时的限制）`
    );
  }
  const dataUrl = await readAsDataUrl(file);
  return {
    contentType: 'FILE',
    body: `[文件] ${file.name}`,
    attrs: { url: dataUrl, name: file.name, size: file.size }
  };
}

export async function videoToPayload(file: File): Promise<MediaPayload> {
  if (file.size > MAX_MEDIA_BYTES) {
    throw new MediaTooLargeError(
      `视频 ${(file.size / 1024 / 1024).toFixed(1)}MB 过大：未接对象存储时只能发 ${(MAX_MEDIA_BYTES / 1024 / 1024).toFixed(1)}MB 以内的短视频`
    );
  }
  const dataUrl = await readAsDataUrl(file);
  return {
    contentType: 'VIDEO',
    body: '[视频]',
    attrs: { url: dataUrl, name: file.name, size: file.size }
  };
}

/* ------------------------------ 录音 ------------------------------ */

export function isVoiceSupported(): boolean {
  return typeof window !== 'undefined'
    && typeof MediaRecorder !== 'undefined'
    && !!(navigator.mediaDevices && navigator.mediaDevices.getUserMedia);
}

function pickMimeType(): string | undefined {
  const candidates = ['audio/webm;codecs=opus', 'audio/webm', 'audio/mp4', 'audio/ogg;codecs=opus'];
  for (const type of candidates) {
    if (typeof MediaRecorder !== 'undefined' && MediaRecorder.isTypeSupported?.(type)) return type;
  }
  return undefined;
}

/**
 * 录音器：按住说话 / 松手发送。
 *
 * 微信内的 iOS 需要 **用户手势** 才能拿到麦克风，所以 `start()` 必须在 touchstart/click 里同步调用；
 * 也不要在 `start()` 里 await 任何东西，否则会丢掉手势上下文。
 */
export class VoiceRecorder {
  private recorder: MediaRecorder | null = null;
  private stream: MediaStream | null = null;
  private chunks: BlobPart[] = [];
  private startedAt = 0;
  private mimeType: string | undefined;

  async start(): Promise<void> {
    if (!isVoiceSupported()) throw new Error('当前浏览器不支持录音');
    this.stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    this.mimeType = pickMimeType();
    this.chunks = [];
    this.recorder = new MediaRecorder(this.stream, this.mimeType ? { mimeType: this.mimeType } : undefined);
    this.recorder.ondataavailable = (event) => {
      if (event.data && event.data.size > 0) this.chunks.push(event.data);
    };
    this.recorder.start();
    this.startedAt = Date.now();
  }

  /** 停止并返回可直接发送的 payload；时长过短视为误触 */
  async stop(minDurationMs = 800): Promise<MediaPayload> {
    const recorder = this.recorder;
    if (!recorder) throw new Error('录音未开始');
    const duration = Date.now() - this.startedAt;
    const blob = await new Promise<Blob>((resolve) => {
      recorder.onstop = () => resolve(new Blob(this.chunks, { type: this.mimeType ?? 'audio/webm' }));
      recorder.stop();
    });
    this.release();
    if (duration < minDurationMs) throw new Error('说话时间太短');
    if (blob.size > MAX_MEDIA_BYTES) throw new MediaTooLargeError('录音过长，请控制在 60 秒内');
    const dataUrl = await readAsDataUrl(blob);
    return {
      contentType: 'VOICE',
      body: `[语音] ${Math.round(duration / 1000)}"`,
      attrs: { url: dataUrl, duration, size: blob.size }
    };
  }

  cancel(): void {
    try {
      this.recorder?.stop();
    } catch {
      /* ignore */
    }
    this.release();
  }

  private release(): void {
    this.recorder = null;
    this.stream?.getTracks().forEach((track) => track.stop());
    this.stream = null;
  }
}
