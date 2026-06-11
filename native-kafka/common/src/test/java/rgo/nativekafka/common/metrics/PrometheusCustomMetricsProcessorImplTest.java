package rgo.nativekafka.common.metrics;

import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.SummarySnapshot;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PrometheusCustomMetricsProcessorImplTest {

    @Test
    void processCustomCounterMetrics_withNullLabels_ShouldStoreEmptyLabels() {
        PrometheusRegistry registry = new PrometheusRegistry();
        PrometheusCustomMetricsProcessorImpl processor = processor(registry);

        processor.processCustomCounterMetrics("method-a", null, null, null, 2);

        CounterSnapshot.CounterDataPointSnapshot dataPoint = ((CounterSnapshot) snapshot(registry, "custom_count_stats"))
                .getDataPoints()
                .getFirst();
        assertThat(dataPoint.getValue()).isEqualTo(2.0);
        assertThat(dataPoint.getLabels().get("method")).isEqualTo("method-a");
        assertThat(dataPoint.getLabels().get("status")).isEmpty();
        assertThat(dataPoint.getLabels().get("direction")).isEmpty();
        assertThat(dataPoint.getLabels().get("type")).isEmpty();
    }

    @Test
    void processCustomGaugeMetrics_withMapLabels_ShouldSetGaugeValue() {
        PrometheusRegistry registry = new PrometheusRegistry();
        PrometheusCustomMetricsProcessorImpl processor = processor(registry);

        processor.processCustomGaugeMetrics(
                Map.of(
                        CommonLabelName.METHOD, "lag",
                        CommonLabelName.STATUS, "OK",
                        CommonLabelName.DIRECTION, "IN",
                        CommonLabelName.TYPE, "topic",
                        CommonLabelName.ID, "partition-0"
                ),
                3,
                Integer::doubleValue);

        GaugeSnapshot.GaugeDataPointSnapshot dataPoint = ((GaugeSnapshot) snapshot(registry, "custom_gauge_stats"))
                .getDataPoints()
                .getFirst();
        assertThat(dataPoint.getValue()).isEqualTo(3.0);
        assertThat(dataPoint.getLabels().get("method")).isEqualTo("lag");
        assertThat(dataPoint.getLabels().get("status")).isEqualTo("OK");
        assertThat(dataPoint.getLabels().get("direction")).isEqualTo("IN");
        assertThat(dataPoint.getLabels().get("type")).isEqualTo("topic");
        assertThat(dataPoint.getLabels().get("id")).isEqualTo("partition-0");
    }

    @Test
    void processCustomHistogramMetrics_withNanos_ShouldObserveSeconds() {
        PrometheusRegistry registry = new PrometheusRegistry();
        PrometheusCustomMetricsProcessorImpl processor = processor(registry);

        processor.processCustomHistogramMetrics("latency", "OK", "IN", "topic", 1_500_000_000L);

        HistogramSnapshot.HistogramDataPointSnapshot dataPoint = ((HistogramSnapshot) snapshot(registry, "custom_histogram_stats"))
                .getDataPoints()
                .getFirst();
        assertThat(dataPoint.getCount()).isOne();
        assertThat(dataPoint.getSum()).isEqualTo(1.5);
    }

    @Test
    void processCustomSummaryMetrics_withNanos_ShouldObserveSeconds() {
        PrometheusRegistry registry = new PrometheusRegistry();
        PrometheusCustomMetricsProcessorImpl processor = processor(registry);

        processor.processCustomSummaryMetrics("latency", "OK", "IN", "topic", 1_500_000_000L);

        SummarySnapshot.SummaryDataPointSnapshot dataPoint = ((SummarySnapshot) snapshot(registry, "custom_summary_stats"))
                .getDataPoints()
                .getFirst();
        assertThat(dataPoint.getCount()).isOne();
        assertThat(dataPoint.getSum()).isEqualTo(1.5);
    }

    private static PrometheusCustomMetricsProcessorImpl processor(PrometheusRegistry registry) {
        return new PrometheusCustomMetricsProcessorImpl(new PrometheusMetricsProviderImpl(registry));
    }

    private static MetricSnapshot snapshot(PrometheusRegistry registry, String name) {
        return registry.scrape()
                .stream()
                .filter(snapshot -> snapshot.getMetadata().getName().equals(name))
                .findFirst()
                .orElseThrow();
    }
}
