package rgo.nativekafka.consumer.spring.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import rgo.nativekafka.consumer.kafka.ConsumerFactory;
import rgo.nativekafka.consumer.kafka.consumer.NativeConsumer;
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
    public NativeConsumer consumerAtLeastOnce() {
        NativeConsumer consumer = new NativeConsumer(consumerFactory(), kafkaConsumerProperties(), loggingDataHandler());
        consumer.checkConnect();
        consumer.start();
        return consumer;
    }
}
