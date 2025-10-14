package rgo.nativekafka.consumer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import rgo.nativekafka.consumer.properties.KafkaConsumerProperties;
import rgo.nativekafka.consumer.service.ConsumerAtLeastOnce;
import rgo.nativekafka.consumer.service.handler.DataHandler;
import rgo.nativekafka.consumer.service.handler.LoggingDataHandler;
import rgo.nativekafka.consumer.service.ConsumerAtMostOnce;

import java.util.List;

@Configuration
public class ApplicationConfig {

    @Bean
    @ConfigurationProperties("kafka-consumer")
    public KafkaConsumerProperties kafkaConsumerProperties() {
        return new KafkaConsumerProperties();
    }

    @Bean
    public DataHandler loggingDataHandler() {
        return new LoggingDataHandler();
    }

    @Profile("at-most-once")
    @Bean(destroyMethod = "complete")
    public ConsumerAtMostOnce consumerAtMostOnce(List<DataHandler> handlers) {
        ConsumerAtMostOnce consumer = new ConsumerAtMostOnce(kafkaConsumerProperties(), handlers);
        consumer.start();
        return consumer;
    }

    @Profile("at-least-once")
    @Bean(destroyMethod = "complete")
    public ConsumerAtLeastOnce consumerAtLeastOnce(List<DataHandler> handlers) {
        ConsumerAtLeastOnce consumer = new ConsumerAtLeastOnce(kafkaConsumerProperties(), handlers);
        consumer.start();
        return consumer;
    }
}
