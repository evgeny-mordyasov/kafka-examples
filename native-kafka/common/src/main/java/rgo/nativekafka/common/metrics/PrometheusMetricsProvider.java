package rgo.nativekafka.common.metrics;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.core.metrics.Summary;
import jakarta.annotation.Nonnull;

public interface PrometheusMetricsProvider {

    @Nonnull
    Counter getRpcCounter();

    @Nonnull
    Summary getRpcLatency();

    @Nonnull
    Counter getCustomCounter();

    @Nonnull
    Gauge getCustomGauge();

    @Nonnull
    Histogram getCustomHistogram();

    @Nonnull
    Summary getCustomSummary();

}
