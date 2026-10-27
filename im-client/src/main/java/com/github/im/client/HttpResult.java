package com.github.im.client;

/**
 * 服务端「业务结果」：IM 的 REST 在 HTTP 层恒返回 200，成败看 body 里的 {@code code}。
 *
 * <p>为什么单独一个类型而不是 {@code boolean}：错误码是有语义的
 * （{@code 403} 无权/超撤回时限、{@code 404} 消息已被裁剪、{@code 409} 群主不能退群、
 * {@code 413} 超员、{@code 503} 可重试），压成一个布尔值就只能给用户看「操作失败」。
 *
 * @param code    0 = 成功；其余见服务端各接口定义
 * @param message 失败原因（成功时通常为空串或 null）
 * @author zhaorujie
 * @since 1.0.0
 */
public record HttpResult(int code, String message) {

    public static final int OK = 0;

    public static HttpResult ok() {
        return new HttpResult(OK, null);
    }

    public boolean success() {
        return code == OK;
    }
}
