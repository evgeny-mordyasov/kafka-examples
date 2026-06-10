package rgo.nativekafka.producer.service;

import com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rgo.nativekafka.common.Asserts;
import rgo.nativekafka.common.kafka.ProducerFactory;
import rgo.nativekafka.producer.properties.KafkaProducerProperties;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class NativeProducer implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(NativeProducer.class);

    private final Producer<Long, String> producer;
    private final ScheduledExecutorService pushingExecutor;
    private final KafkaProducerProperties properties;
    private final AtomicReference<State> state = new AtomicReference<>(State.CREATED);

    public NativeProducer(ProducerFactory producerFactory, KafkaProducerProperties properties) {
        this.properties = Asserts.nonNull(properties, "properties");
        Asserts.nonEmpty(properties.getTopic(), "topic");
        Asserts.positive(properties.getDelayMs(), "delayMs");
        Asserts.nonEmpty(properties.getProperties(), "properties");
        this.producer = Asserts.nonNull(producerFactory, "producerFactory").create(properties.getProperties());
        this.pushingExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> new Thread(runnable, "producer-kafka-pushing"));
    }

    public synchronized void run() {
        if (state.compareAndSet(State.CREATED, State.STARTED)) {
            pushingExecutor.scheduleWithFixedDelay(
                    this::loopPushing,
                    0,
                    properties.getDelayMs(),
                    TimeUnit.MILLISECONDS);
            LOGGER.info("Started pushing.");
        }
    }

    private void loopPushing() {
        if (state.get() != State.STARTED) {
            return;
        }
        pushOnce();
    }

    @VisibleForTesting
    void pushOnce() {
        ProducerRecord<Long, String> message = new ProducerRecord<>(properties.getTopic(), randomKey(), randomValue());
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

    @Override
    public synchronized void close() {
        State previousState = state.getAndSet(State.CLOSED);
        if (previousState == State.CLOSED) {
            return;
        }
        pushingExecutor.execute(producer::close);
        pushingExecutor.shutdown();
        LOGGER.info("Completed pushing.");
    }

    private enum State {
        CREATED, STARTED, CLOSED
    }
}
