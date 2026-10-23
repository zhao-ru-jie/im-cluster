package com.github.im.gateway.transport;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 上行业务的执行器：虚拟线程池 + 每连接一个 {@link SerialExecutor}。
 *
 * <p><b>它解决的问题</b>：{@code channelRead0 → SessionLifecycle.onFrame → ingress.submit()}
 * 这条链上有一次同步 gRPC（最长 300ms）和若干 Redis 往返。之前它们直接跑在 Netty event loop 上，
 * 于是单实例的上行并发度被锁死在 {@code 2×CPU} 个线程，任何一个下游变慢都会把 event loop 占满
 * （head-of-line blocking，表现为「整个实例所有连接一起卡」）。
 *
 * <p><b>为什么是虚拟线程而不是固定线程池</b>：这里几乎全是「阻塞等 IO」的活，
 * 一条连接一个线程的直觉模型最清晰；虚拟线程在阻塞点自动让出载体线程，
 * 十万连接也不需要十万个平台线程。串行性由 {@link SerialExecutor} 保证，
 * 所以同一连接永远只有一个虚拟线程在干活。
 *
 * <p><b>生命周期</b>{@code NettyGatewayServer} 依赖本 Bean，Spring 会先销毁它、
 * 再销毁本 Bean，因此 {@code channelInactive} 触发的清理任务仍能正常执行。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class BusinessDispatcher {

    private static final Logger log = LoggerFactory.getLogger(BusinessDispatcher.class);

    private final ExecutorService virtualThreads = Executors.newVirtualThreadPerTaskExecutor();

    /**
     * 单连接积压的<b>高水位</b>：超过它就暂停从该连接读取（真背压），降到一半自动恢复。
     *
     * <p>不是「超限就断连」：客户端常见的「本地发件箱一次 flush」会瞬间灌进上千帧，
     * 断连只会让它重连后重发同一批，越试越糟。暂停读取把压力推回客户端，内存有界且不丢帧。
     */
    private final int maxPendingFrames;

    public BusinessDispatcher(@Value("${im.gateway.biz.max-pending-frames:1024}") int maxPendingFrames) {
        // 至少 1：配成 0 会让「积压 1 帧就暂停读」，等于把连接掐死
        this.maxPendingFrames = Math.max(maxPendingFrames, 1);
        log.info("[biz] 上行业务执行器就绪：虚拟线程池，单连接积压高水位 {} 帧（超过即暂停读取）", this.maxPendingFrames);
    }

    /** 为一条连接创建串行执行器（每连接一个，随 Channel 一起被 GC） */
    public SerialExecutor serial(String name) {
        return new SerialExecutor(name, virtualThreads, maxPendingFrames);
    }

    @PreDestroy
    public void shutdown() {
        // 不等待：此刻所有连接已被 Netty 关闭，残留任务的结果也没人消费
        virtualThreads.shutdown();
        log.info("[biz] 上行业务执行器已停止");
    }
}
