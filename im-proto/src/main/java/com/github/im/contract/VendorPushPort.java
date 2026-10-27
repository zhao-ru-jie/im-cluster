package com.github.im.contract;

/**
 * 厂商推送端口（离线通知通道）。
 *
 * <p>实现必须遵守两条约定，否则会把一条通知的失败放大成一条消息的失败：
 * <ul>
 *   <li><b>永不抛异常</b>：厂商接口限流、token 过期、网络超时都是常态。
 *       通知丢了只影响「用户被提醒得晚一点」，而消息本身已经在库里与离线队列里；</li>
 *   <li><b>永不阻塞调用方</b>：调用点在投递路径上，同步等一次第三方 HTTP 会把
 *       下行推送的并发上限（{@code im.delivery.max-inflight}）直接吃掉。</li>
 * </ul>
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public interface VendorPushPort {

    /**
     * 推一条离线通知。实现应当异步执行并自行吞掉所有异常。
     *
     * @param request 目标用户与通知内容
     */
    void push(VendorPushRequest request);
}
