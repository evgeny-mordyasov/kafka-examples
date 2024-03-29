package rgo.producer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import rgo.producer.properties.KafkaProducerProperties;
import rgo.producer.service.Producer;

@Configuration
public class ApplicationConfig {

    @Configuration
    @Profile("at-most-once")
    static class AtMostOnceConfig {

        @Bean
        @ConfigurationProperties("kafka-producer-at-most-once")
        public KafkaProducerProperties kafkaProducerProperties() {
            return new KafkaProducerProperties();
        }

        @Bean(destroyMethod = "complete")
        public Producer producer() {
            Producer producer = new Producer(kafkaProducerProperties());
            producer.start();
            return producer;
        }
    }

    @Configuration
    @Profile("at-least-once")
    static class AtLeastOnceConfig {

        @Bean
        @ConfigurationProperties("kafka-producer-at-least-once")
        public KafkaProducerProperties kafkaProducerProperties() {
            return new KafkaProducerProperties();
        }

        @Bean(destroyMethod = "complete")
        public Producer producer() {
            Producer producer = new Producer(kafkaProducerProperties());
            producer.start();
            return producer;
        }
    }
}
