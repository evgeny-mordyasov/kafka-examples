package rgo.nativekafka.producer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import rgo.nativekafka.producer.properties.KafkaProducerProperties;
import rgo.nativekafka.producer.service.NativeProducer;

@Configuration
class ApplicationConfig {

    @Bean
    @ConfigurationProperties("kafka-producer")
    public KafkaProducerProperties kafkaProducerProperties() {
        return new KafkaProducerProperties();
    }

    @Bean(destroyMethod = "complete")
    public NativeProducer producer() {
        NativeProducer nativeProducer = new NativeProducer(kafkaProducerProperties());
        nativeProducer.start();
        return nativeProducer;
    }
}
