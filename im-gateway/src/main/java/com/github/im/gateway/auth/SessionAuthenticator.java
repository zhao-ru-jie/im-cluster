package com.github.im.gateway.auth;

/**
 * 连接身份校验器。
 *
 * <p>抽成接口的原因：token 的形态取决于部署环境——自签 HMAC（本仓库的
 * {@link HmacTokenAuthenticator}）、接认证中心的 JWT、或者企业内部网关透传的身份。
 * 网关只依赖「userId + token → 结论」这一件事，换实现不动传输层。
 *
 * <p><b>实现约束</b>：
 * <ul>
 *   <li>必须校验 token <b>与 userId 的绑定关系</b>（否则拿到自己的 token 就能冒充任何人）；</li>
 *   <li>不得抛异常：任何异常都转成 {@link AuthVerdict#deny(String)}，否则一条畸形 token
 *       就能让连接处理线程打出堆栈；</li>
 *   <li>不打印 token 原文（日志里只允许出现前缀或哈希）。</li>
 * </ul>
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public interface SessionAuthenticator {

    /**
     * @param userId 客户端声明（或握手 query 携带）的 userId
     * @param token  客户端携带的 token，可能为 null
     * @return 校验结论，永不返回 null
     */
    AuthVerdict authenticate(String userId, String token);
}
