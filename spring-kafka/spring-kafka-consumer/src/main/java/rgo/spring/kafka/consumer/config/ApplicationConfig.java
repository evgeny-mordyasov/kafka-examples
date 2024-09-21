package rgo.spring.kafka.consumer.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;

@Configuration
public class ApplicationConfig {

    private static final String TOPIC = "string-values";

    @Configuration
    @Profile("default-config")
    static class DefaultConfig {

        private static final Logger LOGGER = LoggerFactory.getLogger(DefaultConfig.class);

        @KafkaListener(topics = TOPIC)
        public void listen(String data, ConsumerRecordMetadata meta) {
            LOGGER.info("Received message: data={}, metadata={}", data, meta);
        }
    }

    @Configuration
    @Profile("manually-config")
    static class ManuallyConfig {

        private static final Logger LOGGER = LoggerFactory.getLogger(ManuallyConfig.class);

        @Bean
        ConcurrentMessageListenerContainer<String, String> ConcurrentMessageListenerContainer(
                ConcurrentKafkaListenerContainerFactory<String, String> factory
        ) {
            ConcurrentMessageListenerContainer<String, String> container = factory.createContainer(TOPIC);
            container.getContainerProperties().setMessageListener((MessageListener<String, String>) data ->
                    LOGGER.info("Received message: data={}, metadata={}-{}@{}",
                            data.value(), data.topic(), data.partition(), data.offset()));

            container.start();
            return container;
        }
    }

    @Configuration
    @Profile("manually-config2")
    static class ManuallyConfig2 {

        private static final Logger LOGGER = LoggerFactory.getLogger(ManuallyConfig2.class);

        @Bean
        ConcurrentMessageListenerContainer<String, String> kafkaListenerContainerFactory(
                KafkaProperties properties
        ) {
            ContainerProperties containerProps = new ContainerProperties(TOPIC);
            containerProps.setMessageListener((MessageListener<String, String>) data ->
                    LOGGER.info("Received message: data={}, metadata={}-{}@{}",
                            data.value(), data.topic(), data.partition(), data.offset()));

            ConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(properties.buildConsumerProperties());
            return new ConcurrentMessageListenerContainer<>(consumerFactory, containerProps);
        }
    }
}
