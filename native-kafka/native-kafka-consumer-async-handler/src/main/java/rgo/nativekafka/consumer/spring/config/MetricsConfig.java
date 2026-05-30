package rgo.nativekafka.consumer.spring.config;

import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import rgo.nativekafka.consumer.service.metrics.CustomMetricsProcessor;
import rgo.nativekafka.consumer.service.metrics.MetricsService;
import rgo.nativekafka.consumer.service.metrics.PrometheusCustomMetricsProcessorImpl;
import rgo.nativekafka.consumer.service.metrics.PrometheusMetricsProvider;
import rgo.nativekafka.consumer.service.metrics.PrometheusMetricsProviderImpl;

@Configuration
public class MetricsConfig {

    @Bean
    public PrometheusMetricsProvider prometheusMetricsProvider(PrometheusRegistry prometheusRegistry) {
        return new PrometheusMetricsProviderImpl(prometheusRegistry);
    }

    @Bean
    public CustomMetricsProcessor customMetricsProcessor(PrometheusMetricsProvider prometheusMetricsProvider) {
        return new PrometheusCustomMetricsProcessorImpl(prometheusMetricsProvider);
    }

    @Bean
    public MetricsService metricsService(CustomMetricsProcessor customMetricsProcessor) {
        return new MetricsService(customMetricsProcessor);
    }
}
