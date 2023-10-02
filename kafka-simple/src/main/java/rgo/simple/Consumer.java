package rgo.simple;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;
import java.util.stream.StreamSupport;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static rgo.simple.CommonUtils.BOOTSTRAP_SERVERS;
import static rgo.simple.CommonUtils.TOPIC;

public class Consumer {

    private static final Duration TIMEOUT_POLL_MS = Duration.of(300L, ChronoUnit.MILLIS);

    public static void main(String[] args) {
        pollingLoop();
    }

    private static void pollingLoop() {
        KafkaConsumer<Long, String> kafkaConsumer = new KafkaConsumer<>(properties());
        kafkaConsumer.subscribe(List.of(TOPIC));

        while (true) {
            ConsumerRecords<Long, String> records = kafkaConsumer.poll(TIMEOUT_POLL_MS);
            List<ConsumerRecord<Long, String>> data = toList(records);
            data.forEach(record ->
                    System.out.printf("Handle message. offset=%s, partition=%s, value=%s%n", record.offset(), record.partition(), record.value()));
        }
    }

    private static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(GROUP_ID_CONFIG, "consumer-at-most-once");
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.setProperty(AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(MAX_POLL_RECORDS_CONFIG, "10");
        properties.setProperty(MAX_POLL_INTERVAL_MS_CONFIG, "5000");
        properties.setProperty(SESSION_TIMEOUT_MS_CONFIG, "10000");
        properties.setProperty(HEARTBEAT_INTERVAL_MS_CONFIG, "500");
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongDeserializer");
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        return properties;
    }

    private static List<ConsumerRecord<Long, String>> toList(ConsumerRecords<Long, String> records) {
        return StreamSupport
                .stream(records.spliterator(), false)
                .toList();
    }
}
