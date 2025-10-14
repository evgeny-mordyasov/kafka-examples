package rgo.nativekafka.consumer.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rgo.nativekafka.consumer.properties.KafkaConsumerProperties;
import rgo.nativekafka.consumer.service.handler.DataHandler;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.StreamSupport;

@SuppressWarnings("Duplicates")
public class ConsumerAtMostOnce {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerAtMostOnce.class);

    private final KafkaConsumer<Long, String> kafkaConsumer;
    private final ExecutorService handlerExecutor;
    private final ExecutorService pollingExecutor;
    private final KafkaConsumerProperties config;
    private final List<DataHandler> handlers;
    private final AtomicBoolean isRunning = new AtomicBoolean();

    public ConsumerAtMostOnce(KafkaConsumerProperties properties, List<DataHandler> handlers) {
        kafkaConsumer = new KafkaConsumer<>(properties.getProperties());
        handlerExecutor = Executors.newFixedThreadPool(properties.getThreadPoolSize());
        pollingExecutor = Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, "consumer-kafka-polling"));
        config = properties;
        this.handlers = handlers;
    }

    public void start() {
        if (canStartPolling()) {
            subscribeToTopic();
            startPolling();
        }
    }

    private boolean canStartPolling() {
        return isRunning.compareAndSet(false, true);
    }

    private void subscribeToTopic() {
        kafkaConsumer.subscribe(List.of(config.getTopic()));
        LOGGER.info("Subscribe to the kafka topic: {}", config.getTopic());
    }

    private void startPolling() {
        pollingExecutor.execute(this::polling);
        LOGGER.info("Started polling.");
    }

    private void polling() {
        try {
            fetchRecords();
        } finally {
            close();
        }
    }

    private void fetchRecords() {
        Duration timeout = Duration.of(config.getTimeoutPollMs(), ChronoUnit.MILLIS);

        while (isRunning.get()) {
            try {
                ConsumerRecords<Long, String> records = kafkaConsumer.poll(timeout);
                if (records.isEmpty()) {
                    LOGGER.debug("No more records to consume.");
                    continue;
                }

                List<ConsumerRecord<Long, String>> data = toList(records);
                handlerExecutor.execute(() -> handlers.forEach(handler -> handler.handle(data)));
            } catch (Exception e) {
                LOGGER.error("An unexpected exception, but the polling continued.", e);
            }
        }
    }

    private static List<ConsumerRecord<Long, String>> toList(ConsumerRecords<Long, String> records) {
        return StreamSupport
                .stream(records.spliterator(), false)
                .toList();
    }

    private void close() {
        kafkaConsumer.unsubscribe();
        kafkaConsumer.close();
    }

    public void complete() {
        if (canCompletePolling()) {
            completePolling();
        }
    }

    private boolean canCompletePolling() {
        return isRunning.compareAndSet(true, false);
    }

    private void completePolling() {
        pollingExecutor.shutdown();
        handlerExecutor.shutdown();
        LOGGER.info("Completed polling.");
    }
}
