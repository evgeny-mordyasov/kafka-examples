package rgo.nativekafka.consumer.service.metrics;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.core.metrics.Summary;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import jakarta.annotation.Nonnull;

import static rgo.nativekafka.consumer.service.metrics.PrometheusMetricsSupplier.CUSTOM_COUNTER_BUILDER;
import static rgo.nativekafka.consumer.service.metrics.PrometheusMetricsSupplier.CUSTOM_GAUGE_BUILDER;
import static rgo.nativekafka.consumer.service.metrics.PrometheusMetricsSupplier.CUSTOM_HISTOGRAM_BUILDER;
import static rgo.nativekafka.consumer.service.metrics.PrometheusMetricsSupplier.CUSTOM_SUMMARY_BUILDER;
import static rgo.nativekafka.consumer.service.metrics.PrometheusMetricsSupplier.RPC_COUNTER_BUILDER;
import static rgo.nativekafka.consumer.service.metrics.PrometheusMetricsSupplier.RPC_LATENCY_BUILDER;

public class PrometheusMetricsProviderImpl implements PrometheusMetricsProvider {

    private final Counter rpcCounter;
    private final Summary rpcLatency;
    private final Counter processCustomCounter;
    private final Gauge processCustomGauge;
    private final Histogram processCustomHistogram;
    private final Summary processCustomSummary;

    public PrometheusMetricsProviderImpl(
            @Nonnull PrometheusRegistry prometheusRegistry
    ) {
        this.rpcCounter = RPC_COUNTER_BUILDER.get().register(prometheusRegistry);
        this.rpcLatency = RPC_LATENCY_BUILDER.get().register(prometheusRegistry);

        this.processCustomCounter = CUSTOM_COUNTER_BUILDER.get().register(prometheusRegistry);
        this.processCustomGauge = CUSTOM_GAUGE_BUILDER.get().register(prometheusRegistry);
        this.processCustomHistogram = CUSTOM_HISTOGRAM_BUILDER.get().register(prometheusRegistry);
        this.processCustomSummary = CUSTOM_SUMMARY_BUILDER.get().register(prometheusRegistry);
    }

    @Nonnull
    @Override
    public Counter getRpcCounter() {
        return rpcCounter;
    }

    @Nonnull
    @Override
    public Summary getRpcLatency() {
        return rpcLatency;
    }

    @Nonnull
    @Override
    public Counter getCustomCounter() {
        return processCustomCounter;
    }

    @Nonnull
    @Override
    public Gauge getCustomGauge() {
        return processCustomGauge;
    }

    @Nonnull
    @Override
    public Histogram getCustomHistogram() {
        return processCustomHistogram;
    }

    @Nonnull
    @Override
    public Summary getCustomSummary() {
        return processCustomSummary;
    }
}