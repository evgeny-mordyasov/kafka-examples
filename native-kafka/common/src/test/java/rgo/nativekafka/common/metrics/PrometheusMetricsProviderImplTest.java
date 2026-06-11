package rgo.nativekafka.common.metrics;

import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PrometheusMetricsProviderImplTest {

    @Test
    void createInstance_withRegistry_ShouldCreateAllMetrics() {
        PrometheusMetricsProviderImpl provider = new PrometheusMetricsProviderImpl(new PrometheusRegistry());

        assertThat(provider.getRpcCounter()).isNotNull();
        assertThat(provider.getRpcLatency()).isNotNull();
        assertThat(provider.getCustomCounter()).isNotNull();
        assertThat(provider.getCustomGauge()).isNotNull();
        assertThat(provider.getCustomHistogram()).isNotNull();
        assertThat(provider.getCustomSummary()).isNotNull();
    }
}
