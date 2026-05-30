package rgo.nativekafka.consumer.service.metrics;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.util.Map;
import java.util.function.ToDoubleFunction;

public class PrometheusCustomMetricsProcessorImpl implements CustomMetricsProcessor {

    private static final double NANOS_PER_SECONDS = 1_000_000_000.0D;

    private final PrometheusMetricsProvider metricsProvider;

    public PrometheusCustomMetricsProcessorImpl(
            PrometheusMetricsProvider metricsProvider
    ) {
        this.metricsProvider = metricsProvider;
    }

    @Override
    public void processCustomCounterMetrics(
            @Nonnull String method,
            @Nullable String status,
            @Nullable String direction,
            @Nullable String type,
            long increment
    ) {
        metricsProvider
                .getCustomCounter()
                .labelValues(
                        method,
                        emptyIfNull(status),
                        emptyIfNull(direction),
                        emptyIfNull(type))
                .inc(increment);
    }

    @Override
    public <T> void processCustomGaugeMetrics(
            @Nonnull  String method,
            @Nullable String status,
            @Nullable String direction,
            @Nullable String type,
            @Nonnull  T flag,
            @Nonnull ToDoubleFunction<T> function
    ) {
        metricsProvider
                .getCustomGauge()
                .labelValues(
                        method,                   // CommonLabelName.METHOD
                        emptyIfNull(status),      // CommonLabelName.STATUS
                        emptyIfNull(direction),   // CommonLabelName.DIRECTION
                        emptyIfNull(type),        // CommonLabelName.TYPE
                        "")                       // CommonLabelName.ID
                .set(function.applyAsDouble(flag));
    }

    @Override
    public <T> void processCustomGaugeMetrics(
            @Nonnull Map<CommonLabelName, String> label2Value,
            @Nonnull T flag,
            @Nonnull ToDoubleFunction<T> function
    ) {
        metricsProvider
                .getCustomGauge()
                .labelValues(
                        emptyIfNull(label2Value.get(CommonLabelName.METHOD)),
                        emptyIfNull(label2Value.get(CommonLabelName.STATUS)),
                        emptyIfNull(label2Value.get(CommonLabelName.DIRECTION)),
                        emptyIfNull(label2Value.get(CommonLabelName.TYPE)),
                        emptyIfNull(label2Value.get(CommonLabelName.ID)))
                .set(function.applyAsDouble(flag));
    }

    @Override
    public void processCustomHistogramMetrics(
            @Nonnull String method,
            @Nullable String status,
            @Nullable String direction,
            @Nullable String type,
            long latencyNanos
    ) {
        double latencySeconds = latencyNanos / NANOS_PER_SECONDS;
        metricsProvider
                .getCustomHistogram()
                .labelValues(
                        method,
                        emptyIfNull(status),
                        emptyIfNull(direction),
                        emptyIfNull(type))
                .observe(latencySeconds);
    }

    @Override
    public void processCustomSummaryMetrics(
            @Nonnull String method,
            @Nullable String status,
            @Nullable String direction,
            @Nullable String type,
            long latencyNanos
    ) {
        double latencySeconds = latencyNanos / NANOS_PER_SECONDS;
        metricsProvider
                .getCustomSummary()
                .labelValues(
                        method,
                        emptyIfNull(status),
                        emptyIfNull(direction),
                        emptyIfNull(type))
                .observe(latencySeconds);
    }

    @Nonnull
    private String emptyIfNull(@Nullable String value) {
        return value == null ? "" : value;
    }
}