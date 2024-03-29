package rgo.producer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import rgo.producer.properties.KafkaProducerProperties;
import rgo.producer.service.Producer;

@Configuration
public class ApplicationConfig {

    @Bean
    @ConfigurationProperties("kafka-producer")
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
