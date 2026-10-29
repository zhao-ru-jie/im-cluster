/**
 * REST 层：只做「拼 URL + 超时 + 错误归一」，不含业务语义。
 *
 * 服务端的约定：
 * - 业务结果在响应体的 `code` 里（0 = 成功），HTTP 往往仍是 200；
 * - 只有「缺 query 参数」这类框架级错误才是 4xx/5xx；
 * - CORS 默认全放开（`im.http.cors.allowed-origins=*`），所以前端可以直接跨端口调用。
 */

const DEFAULT_TIMEOUT = 12_000;

let restBase = '';

/** 空串表示与页面同源；末尾斜杠会被去掉 */
export function setRestBase(base: string): void {
  restBase = (base || '').replace(/\/+$/, '');
}

export function getRestBase(): string {
  return restBase;
}

export class ApiError extends Error {
  /** HTTP 状态码，0 表示根本没连上（网络层失败） */
  readonly status: number;
  /** 响应体里的业务码，没有时为 undefined */
  readonly code?: number;
  /** 网络/超时这类可以重试的错误 */
  readonly retryable: boolean;

  constructor(message: string, status: number, code?: number) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.code = code;
    this.retryable = status === 0 || status === 408 || status === 502 || status === 503 || status === 504;
  }
}

interface RequestOptions {
  timeoutMs?: number;
  /** 业务码非 0 时是否抛错（默认 true，仅对带 code 的接口生效） */
  throwOnBizError?: boolean;
}

export async function api<T>(
  method: 'GET' | 'POST' | 'DELETE',
  path: string,
  body?: unknown,
  options: RequestOptions = {}
): Promise<T> {
  const url = restBase + path;
  const controller = typeof AbortController === 'undefined' ? null : new AbortController();
  const timer = controller ? setTimeout(() => controller.abort(), options.timeoutMs ?? DEFAULT_TIMEOUT) : 0;

  let res: Response;
  try {
    res = await fetch(url, {
      method,
      headers: body === undefined ? undefined : { 'Content-Type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: controller ? controller.signal : undefined,
      // 微信内置浏览器不会自己带 cookie，这里也刻意不用凭据：服务端把 URL 里的 userId 当身份
      credentials: 'omit'
    });
  } catch (e) {
    const message = (e as Error)?.name === 'AbortError' ? '请求超时，请检查网络' : '网络不可用，请检查服务端地址';
    throw new ApiError(message, 0);
  } finally {
    if (timer) clearTimeout(timer);
  }

  const text = await res.text();
  if (/^\s*<(?:!doctype|html)/i.test(text)) {
    // 打到了静态服务器 / 前端 dev server，而不是 im-server
    throw new ApiError('接口地址指向了静态页面，请检查「服务端地址」配置', res.status);
  }

  let data: unknown = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      throw new ApiError('响应不是合法 JSON', res.status);
    }
  }

  if (!res.ok) {
    const message = (data as { message?: string; error?: string } | null)?.message
      ?? (data as { error?: string } | null)?.error
      ?? `HTTP ${res.status}`;
    throw new ApiError(message, res.status);
  }

  if (options.throwOnBizError !== false) {
    const code = (data as { code?: number } | null)?.code;
    if (typeof code === 'number' && code !== 0) {
      const message = (data as { message?: string }).message ?? bizMessage(code);
      throw new ApiError(message, res.status, code);
    }
  }

  return data as T;
}

/** 服务端错误码含义（GroupService / MessageService 的约定） */
export function bizMessage(code: number): string {
  switch (code) {
    case 400:
      return '参数不合法';
    case 403:
      return '没有权限（或已超过撤回时限）';
    case 404:
      return '内容不存在或已被清理';
    case 409:
      return '当前状态下不允许该操作';
    case 413:
      return '超出人数上限';
    case 503:
      return '服务暂时不可用，请稍后重试';
    default:
      return `操作失败（code=${code}）`;
  }
}

/** 带 query 的路径拼装：自动 encode，跳过 undefined/null */
export function withQuery(path: string, query: Record<string, string | number | boolean | undefined | null>): string {
  const parts: string[] = [];
  Object.keys(query).forEach((key) => {
    const value = query[key];
    if (value === undefined || value === null || value === '') return;
    parts.push(`${encodeURIComponent(key)}=${encodeURIComponent(String(value))}`);
  });
  return parts.length ? `${path}?${parts.join('&')}` : path;
}
