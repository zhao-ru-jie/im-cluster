package com.github.im.client;

/**
 * 短请求的传输层失败：连不上、超时、HTTP 状态非 2xx、响应不是 JSON。
 *
 * <p>与 {@link HttpResult} 的分工很清楚：
 * <ul>
 *   <li>{@link HttpResult} 是「服务端答复了，但它说这件事没做成」（业务错，看 {@code code}）；</li>
 *   <li>本异常是「根本没拿到答复」（网络/网关/部署错）。两者混在一起会让调用方
 *       对「网络抖动」做用户提示（应该重试），或者对「403」重试（永远失败）。</li>
 * </ul>
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class ImHttpException extends RuntimeException {

    private final int status;

    public ImHttpException(int status, String message, Throwable cause) {
        super(message, cause);
        this.status = status;
    }

    public ImHttpException(int status, String message) {
        this(status, message, null);
    }

    /** HTTP 状态码；{@code 0} 表示请求根本没发出去（连接失败/超时） */
    public int status() {
        return status;
    }
}
