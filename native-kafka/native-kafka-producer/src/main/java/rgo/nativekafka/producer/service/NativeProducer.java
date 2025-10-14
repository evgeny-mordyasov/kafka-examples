package rgo.nativekafka.producer.service;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rgo.nativekafka.producer.properties.KafkaProducerProperties;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class NativeProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(NativeProducer.class);

    private final Producer<Long, String> producer;
    private final ScheduledExecutorService pushingExecutor;
    private final KafkaProducerProperties config;
    private final AtomicBoolean isRunning = new AtomicBoolean();

    public NativeProducer(KafkaProducerProperties properties) {
        producer = new KafkaProducer<>(properties.getProperties());
        pushingExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> new Thread(runnable, "producer-kafka-pushing"));
        config = properties;
    }

    public void start() {
        if (canStartPushing()) {
            startPushing();
        }
    }

    private boolean canStartPushing() {
        return isRunning.compareAndSet(false, true);
    }

    private void startPushing() {
        pushingExecutor.scheduleWithFixedDelay(this::pushing, 0L, config.getDelayMs(), TimeUnit.MILLISECONDS);
        LOGGER.info("Started pushing.");
    }

    private void pushing() {
        ProducerRecord<Long, String> message = new ProducerRecord<>(config.getTopic(), randomKey(), randomValue());
        producer.send(message, callback());
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
        producer.close();
    }
}