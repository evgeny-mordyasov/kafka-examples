package rgo.kafka.producer.config;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import rgo.kafka.producer.properties.KafkaProducerProperties;
import rgo.kafka.producer.service.Producer;

@Configuration
public class ApplicationConfig {

    @Bean
    @ConfigurationProperties("kafka-producer")
    public KafkaProducerProperties kafkaProducerProperties() {
        return new KafkaProducerProperties();
    }

    @EventListener
    public void start(ApplicationReadyEvent event) {
        new Producer(kafkaProducerProperties())
                .start();
    }
}
