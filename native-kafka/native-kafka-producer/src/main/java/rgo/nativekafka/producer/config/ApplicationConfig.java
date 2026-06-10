package rgo.nativekafka.producer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import rgo.nativekafka.common.kafka.ProducerFactory;
import rgo.nativekafka.producer.properties.KafkaProducerProperties;
import rgo.nativekafka.producer.service.NativeProducer;

import java.util.Map;

@Configuration
class ApplicationConfig {

    @Bean
    @ConfigurationProperties("kafka-producer")
    public KafkaProducerProperties kafkaProducerProperties() {
        return new KafkaProducerProperties();
    }

    @Bean
    public ProducerFactory producerFactory() {
        return ProducerFactory.getInstance(Map.of());
    }

    @Bean(destroyMethod = "close")
    public NativeProducer producer() {
        NativeProducer nativeProducer = new NativeProducer(producerFactory(), kafkaProducerProperties());
        nativeProducer.run();
        return nativeProducer;
    }
}
