package rgo.simple;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;
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
import static rgo.simple.CommonUtils.BOOTSTRAP_SERVERS;
import static rgo.simple.CommonUtils.TOPIC;

public class Producer {

    private static final long DELAY_MS = 500L;

    public static void main(String[] args) throws InterruptedException {
        pushingLoop();
    }

    private static void pushingLoop() throws InterruptedException {
        KafkaProducer<Long, String> kafkaProducer = new KafkaProducer<>(properties());

        while (true) {
            ProducerRecord<Long, String> producerRecord = new ProducerRecord<>(TOPIC, randomString());
            kafkaProducer.send(producerRecord, callback());
            TimeUnit.MILLISECONDS.sleep(DELAY_MS);
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

    private static String randomString() {
        return UUID.randomUUID().toString();
    }

    private static Callback callback() {
        return (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Failed to send message: ");
                exception.printStackTrace();
            } else {
                System.out.printf("The message was sent. offset=%s, partition=%s%n", metadata.offset(), metadata.partition());
            }
        };
    }
}
