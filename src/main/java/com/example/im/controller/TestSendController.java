package com.example.im.controller;

import com.example.im.route.MessageEnvelope;
import com.example.im.route.MessageRouter;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * 测试用 REST 入口：方便在没有 WebSocket 客户端时验证跨节点转发。
 * 例如：POST /send  {"from":"alice","to":"bob","body":"hello"}
 */
@RestController
public class TestSendController {

    private final MessageRouter router;

    public TestSendController(MessageRouter router) {
        this.router = router;
    }

    @PostMapping("/send")
    public void send(@RequestBody MessageEnvelope env) {
        router.route(env);
    }
}
