package com.github.im.server.message;

import com.github.im.contract.MessageEnvelope;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 下行帧 {@code msg.push} 的序列化（协议所有者是 IM 服务）。
 *
 * <p>frame 一旦生成就是「完整可下发的报文」，网关通过 {@code PushTask.frame} 原样透传，
 * 不做反序列化。这样新增业务字段（已读回执、卡片消息）不需要改网关。
 *
 * <pre>
 * {"ver":1,"type":"msg.push","conversationId":"s:alice:bob","convType":"SINGLE","seq":12,
 *  "serverMsgId":"0192f0c4-8a3b-7c2d-9f01-4b7a2c8d5e60",
 *  "from":"alice","to":"bob","body":"hi","ts":1750000000000}
 * </pre>
 *
 * <p>实时下发与历史拉取必须产出<b>同一个结构</b>（都从 {@link StoredMessage} 这一份数据渲染），
 * 否则客户端要写两套解析。因此 {@link #pushFrame} 只是「受理时先算出帧」的便捷入口，
 * 真正的模板在 {@link #frameOf}。
 */
// 显式命名：im-gateway 里也有一个同名类（网关只产出控制帧），避免阶段 1 同进程装配冲突
@Component("serverClientFrames")
public class ClientFrames {

    public static final String TYPE_PUSH = "msg.push";
    private static final int VER = 1;

    private final ObjectMapper objectMapper;

    public ClientFrames(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /** 受理路径用：拿到 seq/serverMsgId 后立刻渲染出要推送的帧 */
    public String pushFrame(MessageEnvelope envelope, String serverMsgId, long seq, long createdAt) {
        return frameOf(new StoredMessage(
                envelope.conversationId(), seq, serverMsgId,
                envelope.from(), envelope.to(), envelope.convType(),
                envelope.clientMsgId(), envelope.body(), createdAt));
    }

    /** 读路径用：把存储里的记录还原成帧（历史消息、离线补拉都走这里） */
    public String frameOf(StoredMessage message) {
        return objectMapper.writeValueAsString(frameMap(message));
    }

    /**
     * 帧的字段模板（唯一来源）。
     *
     * <p>{@code ts} 用存储时的服务端受理时间，而不是渲染时间——否则同一条历史消息
     * 每次拉取的 ts 都会变，客户端排序/去重会出错。
     */
    public Map<String, Object> frameMap(StoredMessage message) {
        Map<String, Object> frame = new LinkedHashMap<>();
        frame.put("ver", VER);
        frame.put("type", TYPE_PUSH);
        frame.put("conversationId", message.conversationId());
        frame.put("convType", message.convType());
        frame.put("seq", message.seq());
        frame.put("serverMsgId", message.serverMsgId());
        frame.put("clientMsgId", message.clientMsgId());
        frame.put("from", message.from());
        frame.put("to", message.to());
        frame.put("body", message.body());
        frame.put("ts", message.createdAt());
        return frame;
    }
}
