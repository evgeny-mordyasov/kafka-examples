package rgo.nativekafka.common.concurrent;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class SafeExecutorsTest {

    @Test
    void newSingleThreadScheduledExecutor_taskThrowsThrowable_continuesScheduling() throws InterruptedException {
        CountDownLatch calls = new CountDownLatch(2);
        AtomicInteger callCount = new AtomicInteger();

        try (ScheduledExecutorService executor = SafeExecutors.newSingleThreadScheduledExecutor(runnable -> new Thread(runnable, "safe-executor-test"))) {
            executor.scheduleWithFixedDelay(() -> {
                calls.countDown();
                if (callCount.incrementAndGet() == 1) {
                    throw new Error("unexpected");
                }
            }, 0, 10, TimeUnit.MILLISECONDS);

            assertThat(calls.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(callCount).hasValueGreaterThanOrEqualTo(2);
        }
    }

    @Test
    void newSingleThreadScheduledExecutor_usesThreadFactory() throws InterruptedException {
        CountDownLatch call = new CountDownLatch(1);
        AtomicReference<String> threadName = new AtomicReference<>();

        try (ScheduledExecutorService executor = SafeExecutors.newSingleThreadScheduledExecutor(runnable -> new Thread(runnable, "custom-thread-name"))) {
            executor.execute(() -> {
                threadName.set(Thread.currentThread().getName());
                call.countDown();
            });

            assertThat(call.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(threadName).hasValue("custom-thread-name");
        }
    }
}
