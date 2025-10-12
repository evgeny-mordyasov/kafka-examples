package rgo.simple;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static rgo.simple.CommonProperties.BOOTSTRAP_SERVERS;
import static rgo.simple.CommonProperties.TOPIC;

public class SimpleProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleProducer.class);
    private static final long DELAY_MS = 1000L;

    public static void main(String[] args) {
        pushingLoop();
    }

    private static void pushingLoop() {
        try (Producer<Long, String> producer = new KafkaProducer<>(properties())) {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    ProducerRecord<Long, String> message = new ProducerRecord<>(TOPIC, randomKey(), randomValue());
                    producer.send(message, callback());
                    TimeUnit.MILLISECONDS.sleep(DELAY_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.warn("Thread interrupted during sleep.");
                }
            }
        }
    }

    private static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(CLIENT_ID_CONFIG, "kafka-producer");
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ACKS_CONFIG, "all");
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongSerializer");
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(BATCH_SIZE_CONFIG, "16384");
        properties.setProperty(LINGER_MS_CONFIG, "0");
        properties.setProperty(ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(RETRIES_CONFIG, "3");
        return properties;
    }

    private static Long randomKey() {
        return ThreadLocalRandom.current().nextLong();
    }

    private static String randomValue() {
        return UUID.randomUUID().toString();
    }

    private static Callback callback() {
        return (metadata, exception) -> {
            if (exception != null) {
                LOGGER.error("Failed to send message.", exception);
            } else {
                LOGGER.info("The message was sent. metadata = {}", metadata);
            }
        };
    }
}
