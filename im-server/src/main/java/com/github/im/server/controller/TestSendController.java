package com.github.im.server.controller;

import com.github.im.contract.MessageEnvelope;
import com.github.im.proto.ingress.v1.SubmitMessageResponse;
import com.github.im.server.message.MessageService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 测试用 REST 入口（归属 IM 服务）：没有 WebSocket 客户端时验证受理与投递链路。
 *
 * <pre>POST /send  {"from":"alice","to":"bob","body":"hello"}</pre>
 *
 * <p>它直接复用 {@link MessageService#dispatch} 的完整流程（幂等 + seq + 落库 + 推送 + 离线兜底），
 * 因此「测试通路」不会绕过业务逻辑，这也是拆分后把该入口从连接层搬到业务层的原因。
 */
@RestController
public class TestSendController {

    private final MessageService messageService;

    public TestSendController(MessageService messageService) {
        this.messageService = messageService;
    }

    @PostMapping("/send")
    public Map<String, Object> send(@RequestBody MessageEnvelope envelope) {
        SubmitMessageResponse response = messageService.dispatch(envelope);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("code", response.getCode());
        result.put("serverMsgId", response.getServerMsgId());
        result.put("seq", response.getSeq());
        result.put("duplicate", response.getDuplicate());
        return result;
    }
}
