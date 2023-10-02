package rgo.consumer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import rgo.consumer.properties.KafkaConsumerProperties;
import rgo.consumer.service.handler.DataHandler;
import rgo.consumer.service.handler.LoggingDataHandler;
import rgo.consumer.service.Consumer;

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
       return new Consumer(kafkaConsumerProperties(), handlers);
    }
}
