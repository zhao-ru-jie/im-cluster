package com.example.im.route;

/**
 * 一条待路由的消息信封。
 * @param from 发送方用户 ID
 * @param to   接收方用户 ID
 * @param body 消息体（已序列化的 JSON 字符串）
 */
public record MessageEnvelope(String from, String to, String body) {
}
