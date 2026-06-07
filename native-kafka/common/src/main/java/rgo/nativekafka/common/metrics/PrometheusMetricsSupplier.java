package rgo.nativekafka.common.metrics;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.core.metrics.Summary;

import java.util.function.Supplier;

public final class PrometheusMetricsSupplier {
    private PrometheusMetricsSupplier() {
    }

    private static final double[] DEFAULT_LATENCY_BUCKETS =
            new double[] {.001, .0025, .005, .0075, .01, .025, .05, 0.075, .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10};

    public static final Supplier<Counter.Builder> RPC_COUNTER_BUILDER = () -> Counter.builder()
            .name("rpc_count_stats")
            .labelNames(CommonLabelName.STATUS.lowerCaseName(),
                    CommonLabelName.METHOD.lowerCaseName(),
                    CommonLabelName.DIRECTION.lowerCaseName(),
                    CommonLabelName.TYPE.lowerCaseName())
            .help("Counts service RPC requests");

    public static final Supplier<Summary.Builder> RPC_LATENCY_BUILDER = () -> Summary.builder()
            .name("rpc_latency_stats")
            .labelNames(CommonLabelName.STATUS.lowerCaseName(),
                    CommonLabelName.METHOD.lowerCaseName(),
                    CommonLabelName.DIRECTION.lowerCaseName(),
                    CommonLabelName.TYPE.lowerCaseName())
            .quantile(0.95, 0.01)
            .quantile(0.999, 0.01)
            .maxAgeSeconds(10)
            .numberOfAgeBuckets(50)
            .help("Tracks service RPC latency");

    public static final Supplier<Counter.Builder> CUSTOM_COUNTER_BUILDER = () -> Counter.builder()
            .name("custom_count_stats")
            .labelNames(CommonLabelName.METHOD.lowerCaseName(),
                    CommonLabelName.STATUS.lowerCaseName(),
                    CommonLabelName.DIRECTION.lowerCaseName(),
                    CommonLabelName.TYPE.lowerCaseName())
            .help("Generic application counter metric");

    public static final Supplier<Gauge.Builder> CUSTOM_GAUGE_BUILDER = () -> Gauge.builder()
            .name("custom_gauge_stats")
            .labelNames(CommonLabelName.METHOD.lowerCaseName(),
                    CommonLabelName.STATUS.lowerCaseName(),
                    CommonLabelName.DIRECTION.lowerCaseName(),
                    CommonLabelName.TYPE.lowerCaseName(),
                    CommonLabelName.ID.lowerCaseName())
            .help("Generic application gauge metric");

    public static final Supplier<Histogram.Builder> CUSTOM_HISTOGRAM_BUILDER = () -> Histogram.builder()
            .name("custom_histogram_stats")
            .labelNames(CommonLabelName.METHOD.lowerCaseName(),
                    CommonLabelName.STATUS.lowerCaseName(),
                    CommonLabelName.DIRECTION.lowerCaseName(),
                    CommonLabelName.TYPE.lowerCaseName())
            .classicUpperBounds(DEFAULT_LATENCY_BUCKETS)
            .help("Generic application histogram metric");

    public static final Supplier<Summary.Builder> CUSTOM_SUMMARY_BUILDER = () -> Summary.builder()
            .name("custom_summary_stats")
            .labelNames(CommonLabelName.METHOD.lowerCaseName(),
                    CommonLabelName.STATUS.lowerCaseName(),
                    CommonLabelName.DIRECTION.lowerCaseName(),
                    CommonLabelName.TYPE.lowerCaseName())
            .quantile(0.95, 0.01)
            .quantile(0.999, 0.01)
            .maxAgeSeconds(10)
            .numberOfAgeBuckets(50)
            .help("Generic application summary metric");

}
