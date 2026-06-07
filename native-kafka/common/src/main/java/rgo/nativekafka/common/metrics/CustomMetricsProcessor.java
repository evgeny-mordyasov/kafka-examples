package rgo.nativekafka.common.metrics;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.util.Map;
import java.util.function.ToDoubleFunction;

public interface CustomMetricsProcessor {
    void processCustomCounterMetrics(@Nonnull String method,
                                     @Nullable String status,
                                     @Nullable String direction,
                                     @Nullable String type,
                                     long increment);

    <T> void processCustomGaugeMetrics(@Nonnull String method,
                                       @Nullable String status,
                                       @Nullable String direction,
                                       @Nullable String type,
                                       @Nonnull T flag,
                                       @Nonnull ToDoubleFunction<T> function);

    <T> void processCustomGaugeMetrics(@Nonnull Map<CommonLabelName, String> label2Value,
                                       @Nonnull T flag,
                                       @Nonnull ToDoubleFunction<T> function);

    void processCustomHistogramMetrics(@Nonnull String method,
                                       @Nullable String status,
                                       @Nullable String direction,
                                       @Nullable String type,
                                       long latencyNanos);

    void processCustomSummaryMetrics(@Nonnull String method,
                                     @Nullable String status,
                                     @Nullable String direction,
                                     @Nullable String type,
                                     long latencyNanos);
}
