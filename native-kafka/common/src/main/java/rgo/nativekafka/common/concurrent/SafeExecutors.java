package rgo.nativekafka.common.concurrent;

import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rgo.nativekafka.common.Asserts;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public final class SafeExecutors {

    private static final Logger LOGGER = LoggerFactory.getLogger(SafeExecutors.class);

    private SafeExecutors() {
    }

    public static ScheduledExecutorService newSingleThreadScheduledExecutor(ThreadFactory threadFactory) {
        return new SafeScheduledThreadPoolExecutor(Asserts.nonNull(threadFactory, "threadFactory"));
    }

    public static ScheduledExecutorService newSingleThreadScheduledExecutor() {
        return newSingleThreadScheduledExecutor(Executors.defaultThreadFactory());
    }

    private static class SafeScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {

        private SafeScheduledThreadPoolExecutor(ThreadFactory threadFactory) {
            super(1, threadFactory);
        }

        @Override
        public void execute(@Nonnull Runnable command) {
            super.execute(safe(command));
        }

        @Nonnull
        @Override
        public Future<?> submit(@Nonnull Runnable task) {
            return super.submit(safe(task));
        }

        @Nonnull
        @Override
        public <T> Future<T> submit(@Nonnull Runnable task, T result) {
            return super.submit(safe(task), result);
        }

        @Nonnull
        @Override
        public <T> Future<T> submit(@Nonnull Callable<T> task) {
            return super.submit(safe(task));
        }

        @Nonnull
        @Override
        public ScheduledFuture<?> schedule(@Nonnull Runnable command, long delay, @Nonnull TimeUnit unit) {
            return super.schedule(safe(command), delay, unit);
        }

        @Nonnull
        @Override
        public <V> ScheduledFuture<V> schedule(@Nonnull Callable<V> callable, long delay, @Nonnull TimeUnit unit) {
            return super.schedule(safe(callable), delay, unit);
        }

        @Nonnull
        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(@Nonnull Runnable command, long initialDelay, long period, @Nonnull TimeUnit unit) {
            return super.scheduleAtFixedRate(safe(command), initialDelay, period, unit);
        }

        @Nonnull
        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(@Nonnull Runnable command, long initialDelay, long delay, @Nonnull TimeUnit unit) {
            return super.scheduleWithFixedDelay(safe(command), initialDelay, delay, unit);
        }

        private static Runnable safe(Runnable command) {
            Asserts.nonNull(command, "command");
            return () -> {
                try {
                    command.run();
                } catch (Throwable e) {
                    LOGGER.error("Scheduled task failed unexpectedly.", e);
                }
            };
        }

        private static <V> Callable<V> safe(Callable<V> callable) {
            Asserts.nonNull(callable, "callable");
            return () -> {
                try {
                    return callable.call();
                } catch (Throwable e) {
                    LOGGER.error("Scheduled task failed unexpectedly.", e);
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    return null;
                }
            };
        }
    }
}
