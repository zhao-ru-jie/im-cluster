package com.github.im.gateway.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 把「同一连接上的一串任务」串行化，但执行线程是虚拟线程而不是 Netty event loop。
 *
 * <p><b>为什么需要它</b>：上行 {@code msg.send} 要同步等 IM 服务（gRPC ≤300ms），如果直接在
 * {@code channelRead0} 里做，event loop 线程被占住，挂在这个线程上的所有连接一起停摆
 * （吞吐被锁死在 {@code 2×CPU} 个并发，且抖动会放大成雪崩）。
 *
 * <p><b>为什么不直接丢线程池</b>：同一个连接的两帧不能并发执行，否则
 * 「先 connect 后 msg.send」的顺序、以及同一会话内的处理顺序都会乱。
 * 一个 {@code SerialExecutor} 对应一条连接：任务 FIFO 严格串行，但绝不占用 event loop。
 *
 * <p><b>背压（关键设计）</b>：积压超过高水位 {@code maxPending} 时，<b>不丢帧、不断连</b>，
 * 而是回调 {@code onPause} 让调用方暂停从 socket 读取（{@code setAutoRead(false)}）——
 * 数据停在客户端与内核缓冲区里，TCP 窗口自然收缩，客户端自己就被限速了。
 * 积压降到低水位（高水位一半）时回调 {@code onResume} 恢复读取。
 *
 * <p>为什么不是「超限就断开」：客户端常用的模式是「本地发件箱一次 flush」，离线攒了 2000 条
 * 上线瞬间全灌进来，断连只会让它重连后重发同样 2000 条 —— 越重试越糟的活锁。
 * 只有「暂停读也救不回来」的极端情况才断开，那个兜底交给心跳（暂停读期间收不到上行，
 * {@code IdleStateHandler} 45~90s 后会判定超时并断开）。
 *
 * <p><b>与 Netty 的关系</b>：{@code writeAndFlush} 与 {@code config().setAutoRead()} 从任意线程
 * 调用都是线程安全的（Netty 内部会切回该 Channel 的 event loop 执行），
 * 所以业务在虚拟线程里回写 ack、恢复读取都无需加锁。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public final class SerialExecutor {

    private static final Logger log = LoggerFactory.getLogger(SerialExecutor.class);

    /** 用于日志定位是哪条连接/哪种传输；不参与逻辑 */
    private final String name;
    private final Executor delegate;
    /** 高水位：积压超过它就暂停读取（不是丢帧） */
    private final int maxPending;
    /** 低水位：降到这个值以下才恢复读取。取高水位一半，避免在阈值附近来回抖动 */
    private final int resumeBelow;

    private final Queue<Runnable> queue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger pending = new AtomicInteger();
    /** 是否已有一个 drain 循环在跑：保证同一时刻最多一个虚拟线程在执行本实例的任务 */
    private final AtomicBoolean draining = new AtomicBoolean();
    /** 是否处于「已通知调用方暂停读取」的状态，保证 onPause / onResume 各只触发一次 */
    private final AtomicBoolean paused = new AtomicBoolean();

    private volatile Runnable onPause = () -> { };
    private volatile Runnable onResume = () -> { };

    SerialExecutor(String name, Executor delegate, int maxPending) {
        this.name = name;
        this.delegate = delegate;
        this.maxPending = maxPending;
        this.resumeBelow = Math.max(1, maxPending / 2);
    }

    /**
     * 注册背压钩子（由 handler 注入）。
     *
     * <p>{@code onPause} 在「积压超过高水位」时被调用，通常实现为 {@code setAutoRead(false)}；
     * {@code onResume} 在「积压降到低水位」时被调用，通常实现为 {@code setAutoRead(true)}。
     * 两者可能在不同线程执行（前者多在 event loop，后者在虚拟线程），实现方需保证线程安全。
     */
    public void onBackpressure(Runnable onPause, Runnable onResume) {
        this.onPause = onPause == null ? () -> { } : onPause;
        this.onResume = onResume == null ? () -> { } : onResume;
    }

    /**
     * 提交业务帧：<b>总是入队，永不丢帧</b>。
     *
     * <p>积压超过高水位时触发一次 {@code onPause}，由调用方暂停读取把压力推回客户端；
     * 后续帧仍然入队（当前这一批已经读进内存了，丢掉反而会破坏消息语义）。
     */
    public void offer(Runnable task) {
        int now = pending.incrementAndGet();
        if (!enqueue(task)) {
            return;
        }
        if (now > maxPending && paused.compareAndSet(false, true)) {
            log.warn("[biz] {} 积压 {} 帧超过高水位 {}，暂停读取（消化到 {} 帧以下自动恢复）",
                    name, now, maxPending, resumeBelow);
            fire(onPause, "pause");
        }
    }

    /**
     * 无背压的控制类任务（登记会话、清理会话）。
     *
     * <p>不受高水位约束：清理任务如果被推迟，本次连接就会在 Redis 路由表里留下脏数据
     * （TTL 内消息会被推到已死的连接上）。
     */
    public void submit(Runnable task) {
        pending.incrementAndGet();
        enqueue(task);
    }

    private boolean enqueue(Runnable task) {
        queue.add(task);
        if (!draining.compareAndSet(false, true)) {
            // 已有 drain 循环在跑，它会看到这个任务
            return true;
        }
        try {
            delegate.execute(this::drain);
        } catch (RejectedExecutionException e) {
            // 只在进程关闭时发生：复位状态，任务直接丢弃（连接本来也要断了）
            draining.set(false);
            queue.clear();
            pending.set(0);
            log.debug("[biz] {} 执行器已关闭，丢弃积压任务", name);
            return false;
        }
        return true;
    }

    /** 单个虚拟线程里把队列抽干；每个任务异常都不能打断循环，否则该连接后续帧永远卡住 */
    private void drain() {
        for (; ; ) {
            Runnable task = queue.poll();
            if (task == null) {
                draining.set(false);
                resumeIfDrained();
                // 复位与并发入队之间有竞态：入队方看到 draining=true 就不会再提交，
                // 所以这里必须复查一次队列，否则可能出现「任务已入队但没人执行」
                if (queue.isEmpty() || !draining.compareAndSet(false, true)) {
                    return;
                }
                continue;
            }
            pending.decrementAndGet();
            try {
                task.run();
            } catch (Throwable t) {
                log.warn("[biz] {} 任务执行异常，继续处理后续帧", name, t);
            }
            resumeIfDrained();
        }
    }

    /** 消化到低水位后恢复读取；阈值取一半，避免「刚好到上限就暂停、执行一条又恢复」的抖动 */
    private void resumeIfDrained() {
        if (paused.get() && pending.get() <= resumeBelow && paused.compareAndSet(true, false)) {
            log.debug("[biz] {} 积压降到 {} 帧，恢复读取", name, pending.get());
            fire(onResume, "resume");
        }
    }

    /** 钩子异常不能影响读写线程，也不能打断 drain 循环 */
    private void fire(Runnable hook, String action) {
        try {
            hook.run();
        } catch (RuntimeException e) {
            log.warn("[biz] {} {} 钩子执行异常: {}", name, action, e.getMessage());
        }
    }

    /** 当前积压数，供观测/测试 */
    public int pending() {
        return pending.get();
    }

    /** 是否处于暂停读取状态，供观测/测试 */
    public boolean isPaused() {
        return paused.get();
    }
}
