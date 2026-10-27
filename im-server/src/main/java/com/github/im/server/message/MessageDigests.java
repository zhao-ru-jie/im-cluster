package com.github.im.server.message;

import com.github.im.contract.MessageEnvelope;

/**
 * 消息摘要与类型占位符（会话列表、离线通知共用）。
 *
 * <p>抽出来的原因是这两处必须显示同一句话：会话列表里是「[图片]」，
 * 而厂商推送的通知正文如果写成空字符串，用户看到的是一条没有任何信息的提醒。
 * 两处各写一份 switch，第一次加新类型时就会漂移。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
final class MessageDigests {

    private MessageDigests() {
    }

    /** 非文本消息的占位文案；文本消息返回空串（正文就是 body 本身） */
    static String placeholderOf(String contentType) {
        if (contentType == null || MessageEnvelope.CT_TEXT.equals(contentType)) {
            return "";
        }
        return switch (contentType) {
            case MessageEnvelope.CT_IMAGE -> "[图片]";
            case MessageEnvelope.CT_FILE -> "[文件]";
            case MessageEnvelope.CT_VOICE -> "[语音]";
            case MessageEnvelope.CT_VIDEO -> "[视频]";
            default -> "[消息]";
        };
    }

    /**
     * 会话列表 / 通知用的摘要：文本取 body 前缀，媒体取占位符。
     *
     * @param maxChars 最大字符数，超出截断（列表页与推送各有各的限制）
     */
    static String digestOf(MessageEnvelope envelope, int maxChars) {
        String text = envelope.isText() ? envelope.body() : null;
        if (text == null || text.isBlank()) {
            return placeholderOf(envelope.contentType());
        }
        String trimmed = text.strip();
        return trimmed.length() <= maxChars ? trimmed : trimmed.substring(0, maxChars);
    }
}
