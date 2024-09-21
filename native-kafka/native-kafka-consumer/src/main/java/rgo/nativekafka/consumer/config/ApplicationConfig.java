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
    public DataHandler loggingDataHandler() {
        return new LoggingDataHandler();
    }

    @Configuration
    @Profile("at-most-once")
    static class AtMostOnceConfig {

        @Bean
        @ConfigurationProperties("kafka-consumer-at-most-once")
        public KafkaConsumerProperties kafkaConsumerProperties() {
            return new KafkaConsumerProperties();
        }

        @Bean(destroyMethod = "complete")
        public ConsumerAtMostOnce consumer(List<DataHandler> handlers) {
            ConsumerAtMostOnce consumer = new ConsumerAtMostOnce(kafkaConsumerProperties(), handlers);
            consumer.start();
            return consumer;
        }
    }

    @Configuration
    @Profile("at-least-once")
    static class AtLeastOnce {

        @Bean
        @ConfigurationProperties("kafka-consumer-at-least-once")
        public KafkaConsumerProperties kafkaConsumerProperties() {
            return new KafkaConsumerProperties();
        }

        @Bean(destroyMethod = "complete")
        public ConsumerAtLeastOnce consumer(List<DataHandler> handlers) {
            ConsumerAtLeastOnce consumer = new ConsumerAtLeastOnce(kafkaConsumerProperties(), handlers);
            consumer.start();
            return consumer;
        }
    }
}
