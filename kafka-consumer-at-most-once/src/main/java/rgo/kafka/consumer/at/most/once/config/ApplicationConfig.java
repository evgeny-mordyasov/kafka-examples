package rgo.kafka.consumer.at.most.once.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import rgo.kafka.consumer.at.most.once.properties.KafkaConsumerProperties;
import rgo.kafka.consumer.at.most.once.service.Consumer;
import rgo.kafka.consumer.at.most.once.service.handler.DataHandler;
import rgo.kafka.consumer.at.most.once.service.handler.LoggingDataHandler;

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

    @Bean
    public Consumer start(List<DataHandler> handlers) {
        Consumer consumer = new Consumer(kafkaConsumerProperties(), handlers);
        consumer.start();
        return consumer;
    }
}
