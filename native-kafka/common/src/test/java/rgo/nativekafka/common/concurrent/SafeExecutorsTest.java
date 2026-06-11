package rgo.nativekafka.common.concurrent;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SafeExecutorsTest {

    @Test
    @SuppressWarnings({"ConstantConditions", "resource"})
    void newSingleThreadScheduledExecutor_withNullThreadFactory_ShouldThrowIllegalArgumentException() {
        assertThatThrownBy(() -> SafeExecutors.newSingleThreadScheduledExecutor(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[threadFactory] must not be null.");
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    void execute_withNullCommand_ShouldThrowIllegalArgumentException() {
        try (ScheduledExecutorService executor = SafeExecutors.newSingleThreadScheduledExecutor()) {
            assertThatThrownBy(() -> executor.execute(null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[command] must not be null.");
        }
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    void submit_withNullCallable_ShouldThrowIllegalArgumentException() {
        try (ScheduledExecutorService executor = SafeExecutors.newSingleThreadScheduledExecutor()) {
            assertThatThrownBy(() -> executor.submit((java.util.concurrent.Callable<?>) null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[callable] must not be null.");
        }
    }

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

    @Test
    void submit_callableThrowsRuntimeException_ShouldReturnNull() throws ExecutionException, InterruptedException {
        try (ScheduledExecutorService executor = SafeExecutors.newSingleThreadScheduledExecutor()) {
            Future<String> future = executor.submit(() -> {
                throw new RuntimeException("boom");
            });

            assertThat(future.get()).isNull();
        }
    }

    @Test
    void submit_callableThrowsInterruptedException_ShouldReturnNull() throws ExecutionException, InterruptedException {
        try (ScheduledExecutorService executor = SafeExecutors.newSingleThreadScheduledExecutor()) {
            Future<String> future = executor.submit(() -> {
                throw new InterruptedException("boom");
            });

            assertThat(future.get()).isNull();
        }
    }
}
