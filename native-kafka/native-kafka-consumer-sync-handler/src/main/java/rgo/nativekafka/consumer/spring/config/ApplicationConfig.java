package rgo.nativekafka.consumer.spring.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import rgo.nativekafka.common.kafka.ConsumerFactory;
import rgo.nativekafka.common.metrics.MetricsService;
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
