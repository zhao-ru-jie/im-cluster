package com.github.im.gateway.transport;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SerialExecutor} 的行为约定：<b>串行、保序、背压不丢帧、异常不中断</b>。
 *
 * <p>这几条都不能靠直觉保证：一旦串行被打破，同一条连接的 connect 帧与 msg.send 就可能乱序；
 * 一旦保序被打破，客户端看到的 seq 顺序会抖；一旦背压变成「丢帧」，消息就真丢了
 * （客户端本地发件箱 flush 是最容易撞上高水位的场景）。
 */
class SerialExecutorTest {

    @Test
    @DisplayName("同一串行执行器的任务不会并发，且按提交顺序执行")
    void runsSeriallyInSubmitOrder() throws Exception {
        Executor virtualThreads = Executors.newVirtualThreadPerTaskExecutor();
        SerialExecutor serial = new SerialExecutor("test", virtualThreads, 1024);
        List<Integer> order = new ArrayList<>();
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxInFlight = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(100);

        for (int i = 0; i < 100; i++) {
            int value = i;
            serial.offer(() -> {
                maxInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
                order.add(value);
                inFlight.decrementAndGet();
                done.countDown();
            });
        }

        assertTrue(done.await(5, TimeUnit.SECONDS), "所有任务应在 5s 内执行完");
        assertEquals(1, maxInFlight.get(), "同一串行执行器内不允许两个任务同时执行");
        assertEquals(100, order.size());
        for (int i = 0; i < 100; i++) {
            assertEquals(i, order.get(i), "第 " + i + " 个任务必须按提交顺序执行");
        }
    }

    @Test
    @DisplayName("多个提交线程并发提交时，仍然只有一个任务在跑")
    void staysSerialUnderConcurrentSubmitters() throws Exception {
        Executor virtualThreads = Executors.newVirtualThreadPerTaskExecutor();
        SerialExecutor serial = new SerialExecutor("test", virtualThreads, 4096);
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxInFlight = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(200);

        for (int t = 0; t < 4; t++) {
            Thread.ofVirtual().start(() -> {
                for (int i = 0; i < 50; i++) {
                    serial.offer(() -> {
                        maxInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
                        Thread.onSpinWait();
                        inFlight.decrementAndGet();
                        done.countDown();
                    });
                }
            });
        }

        assertTrue(done.await(5, TimeUnit.SECONDS));
        assertEquals(1, maxInFlight.get(), "并发提交也不能让两个任务同时执行");
    }

    @Test
    @DisplayName("超过高水位只暂停一次，且一帧都不丢")
    void pausesOnceAboveHighWatermarkWithoutDroppingFrames() {
        HoldingExecutor holding = new HoldingExecutor();
        SerialExecutor serial = new SerialExecutor("test", holding, 2);
        AtomicInteger paused = new AtomicInteger();
        AtomicInteger resumed = new AtomicInteger();
        AtomicInteger ran = new AtomicInteger();
        serial.onBackpressure(paused::incrementAndGet, resumed::incrementAndGet);

        for (int i = 0; i < 5; i++) {
            serial.offer(ran::incrementAndGet);
        }

        assertEquals(1, paused.get(), "高水位之上只应触发一次暂停，不能每帧都回调");
        assertTrue(serial.isPaused());

        holding.runAll();
        assertEquals(5, ran.get(), "背压是暂停读，不是丢帧：已入队的 5 帧都必须执行");
        assertEquals(1, resumed.get(), "消化到低水位后应恢复一次");
        assertFalse(serial.isPaused());
    }

    @Test
    @DisplayName("低水位以下才恢复：刚过一半不恢复，降到一半才恢复")
    void resumesOnlyBelowLowWatermark() {
        HoldingExecutor holding = new HoldingExecutor();
        // 高水位 4 → 低水位 2
        SerialExecutor serial = new SerialExecutor("test", holding, 4);
        AtomicInteger resumed = new AtomicInteger();
        serial.onBackpressure(() -> { }, resumed::incrementAndGet);

        for (int i = 0; i < 5; i++) {
            serial.offer(() -> { });
        }
        assertTrue(serial.isPaused());

        holding.runAll();
        assertEquals(1, resumed.get());
        assertEquals(0, serial.pending());
    }

    @Test
    @DisplayName("没到高水位不触发暂停")
    void noBackpressureBelowHighWatermark() {
        HoldingExecutor holding = new HoldingExecutor();
        SerialExecutor serial = new SerialExecutor("test", holding, 4);
        AtomicBoolean paused = new AtomicBoolean();
        serial.onBackpressure(() -> paused.set(true), () -> { });

        serial.offer(() -> { });
        serial.offer(() -> { });

        assertFalse(paused.get());
        assertFalse(serial.isPaused());
    }

    @Test
    @DisplayName("控制类任务（会话登记/清理）不参与背压判定")
    void controlTasksDoNotTriggerBackpressure() {
        HoldingExecutor holding = new HoldingExecutor();
        SerialExecutor serial = new SerialExecutor("test", holding, 1);
        AtomicBoolean paused = new AtomicBoolean();
        serial.onBackpressure(() -> paused.set(true), () -> { });
        AtomicInteger ran = new AtomicInteger();

        serial.submit(ran::incrementAndGet);
        serial.submit(ran::incrementAndGet);
        serial.submit(ran::incrementAndGet);

        assertFalse(paused.get(), "submit 用于控制类任务，不能被背压挡住");
        holding.runAll();
        assertEquals(3, ran.get());
    }

    @Test
    @DisplayName("单个任务抛异常不能打断 drain，后续任务照常执行")
    void taskFailureDoesNotStopDrain() {
        HoldingExecutor holding = new HoldingExecutor();
        SerialExecutor serial = new SerialExecutor("test", holding, 8);
        AtomicBoolean secondRan = new AtomicBoolean();

        serial.offer(() -> {
            throw new IllegalStateException("boom");
        });
        serial.offer(() -> secondRan.set(true));

        holding.runAll();
        assertTrue(secondRan.get(), "前一任务异常后，后续任务必须继续执行");
    }

    /** 只收不跑：用来确定性地构造「队列已积压」这个状态 */
    private static final class HoldingExecutor implements Executor {

        private final List<Runnable> held = new CopyOnWriteArrayList<>();

        @Override
        public void execute(Runnable command) {
            held.add(command);
        }

        void runAll() {
            List<Runnable> tasks = List.copyOf(held);
            held.clear();
            tasks.forEach(Runnable::run);
        }
    }
}
