package com.github.im.client;

/**
 * 置顶 / 免打扰的设置结果（{@code POST /conv/{id}/flag}）。
 *
 * <p>注意参数语义：{@code pinned}/{@code muted} 传 {@code null} 表示<b>这一项不改</b>，
 * 不是「设为 false」。两者都传 null 会被服务端判 400——
 * 因为「什么都不改」的请求没有任何意义，静默成功只会让人误以为写进去了。
 *
 * @param code    0 = 成功
 * @param message 失败原因
 * @param pinned  设置后的置顶状态
 * @param muted   设置后的免打扰状态
 * @author zhaorujie
 * @since 1.0.0
 */
public record FlagResult(int code, String message, boolean pinned, boolean muted) {

    public boolean success() {
        return code == HttpResult.OK;
    }
}
