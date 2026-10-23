package com.github.im.gateway.session;

import io.netty.util.AttributeKey;

/**
 * Channel 上挂的会话相关属性（握手/认证阶段写入，后续 handler 读取）。
 *
 * <p>集中定义的原因：心跳、认证、传输适配三处都要读同一份身份，
 * 各写一个 {@code AttributeKey.valueOf(...)} 字符串容易拼错且拿不到值（AttributeKey 按名字相等）。
 */
public final class SessionAttributes {

    /** 握手/认证阶段解析出的 userId（认证通过后才有值） */
    public static final AttributeKey<String> USER_ID = AttributeKey.valueOf("im.userId");
    public static final AttributeKey<String> DEVICE_ID = AttributeKey.valueOf("im.deviceId");
    /** 认证成功后绑定的会话对象；心跳 handler 靠它下发 ping / 关闭连接 */
    public static final AttributeKey<NettyClientSession> SESSION = AttributeKey.valueOf("im.session");

    private SessionAttributes() {
    }
}
