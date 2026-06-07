package rgo.nativekafka.consumer.spring.config;

import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import rgo.nativekafka.common.ConsumerFactory;
import rgo.nativekafka.common.metrics.CustomMetricsProcessor;
import rgo.nativekafka.common.metrics.MetricsService;
import rgo.nativekafka.common.metrics.PrometheusCustomMetricsProcessorImpl;
import rgo.nativekafka.common.metrics.PrometheusMetricsProvider;
import rgo.nativekafka.common.metrics.PrometheusMetricsProviderImpl;
import rgo.nativekafka.consumer.kafka.consumer.SyncNativeConsumer;
import rgo.nativekafka.consumer.service.handler.DataHandler;
import rgo.nativekafka.consumer.service.handler.LoggingDataHandler;
import rgo.nativekafka.consumer.spring.properties.KafkaConsumerProperties;

import java.util.Map;

@Configuration
public class ApplicationConfig {

    @Bean
    @ConfigurationProperties("kafka-consumer")
    public KafkaConsumerProperties kafkaConsumerProperties() {
        return new KafkaConsumerProperties();
    }

    @Bean
    public ConsumerFactory consumerFactory() {
        return ConsumerFactory.getInstance(Map.of());
    }

    @Bean
    public DataHandler loggingDataHandler() {
        return new LoggingDataHandler();
    }

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

    @Bean
    public SyncNativeConsumer consumerAtLeastOnce(MetricsService metricsService) {
        SyncNativeConsumer consumer = new SyncNativeConsumer(
                consumerFactory(),
                loggingDataHandler(),
                metricsService,
                kafkaConsumerProperties());
        consumer.checkConnect();
        consumer.run();
        return consumer;
    }
}
