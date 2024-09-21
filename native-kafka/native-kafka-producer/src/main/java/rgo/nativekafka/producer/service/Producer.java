package rgo.nativekafka.producer.service;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rgo.nativekafka.producer.properties.KafkaProducerProperties;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Producer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);

    private final KafkaProducer<Long, String> kafkaProducer;
    private final ScheduledExecutorService pushingExecutor;
    private final KafkaProducerProperties config;
    private final AtomicBoolean isRunning = new AtomicBoolean();

    public Producer(KafkaProducerProperties properties) {
        kafkaProducer = new KafkaProducer<>(properties.getProperties());
        pushingExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> new Thread(runnable, "producer-kafka-pushing"));
        config = properties;
    }

    public void start() {
        if (canStartPushing()) {
            startPushing();
        }
    }

    private void startPushing() {
        pushingExecutor.scheduleWithFixedDelay(this::pushing, 0L, config.getDelayMs(), TimeUnit.MILLISECONDS);
        LOGGER.info("Started pushing.");
    }

    private boolean canStartPushing() {
        return isRunning.compareAndSet(false, true);
    }

    private void pushing() {
        ProducerRecord<Long, String> producerRecord = new ProducerRecord<>(config.getTopic(), randomString());
        kafkaProducer.send(producerRecord, callback());
    }

    private static String randomString() {
        return UUID.randomUUID().toString();
    }

    private static Callback callback() {
        return (metadata, exception) -> {
            if (exception != null) {
                LOGGER.error("Failed to send message: ", exception);
            } else {
                LOGGER.info("The message was sent. offset={}, partition={}", metadata.offset(), metadata.partition());
            }
        };
    }

    public void complete() {
        if (canCompletePushing()) {
            completePushing();
            close();
        }
    }

    private boolean canCompletePushing() {
        return isRunning.compareAndSet(true, false);
    }

    private void completePushing() {
        pushingExecutor.shutdown();
        LOGGER.info("Completed pushing.");
    }

    private void close() {
        kafkaProducer.close();
    }
}