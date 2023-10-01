package rgo.kafka.consumer.at.most.once.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rgo.kafka.consumer.at.most.once.properties.KafkaConsumerProperties;
import rgo.kafka.consumer.at.most.once.service.handler.DataHandler;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.StreamSupport;

public class Consumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

    private final KafkaConsumer<Long, String> kafkaConsumer;
    private final ExecutorService executor;
    private final ExecutorService pollingExecutor;
    private final KafkaConsumerProperties config;
    private final List<DataHandler> handlers;
    private final AtomicBoolean isRunning = new AtomicBoolean();

    public Consumer(KafkaConsumerProperties properties, List<DataHandler> handlers) {
        kafkaConsumer = new KafkaConsumer<>(properties.getProperties());
        executor = Executors.newFixedThreadPool(10);
        pollingExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> new Thread(runnable, "producer-kafka-polling"));
        config = properties;
        this.handlers = handlers;
    }

    @PostConstruct
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
        Duration timeout = Duration.of(config.getWaitPollMs(), ChronoUnit.MILLIS);

        while (isRunning.get()) {
            ConsumerRecords<Long, String> records = kafkaConsumer.poll(timeout);
            LOGGER.info("Received messages. partitions={}, count={}", records.partitions(), records.count());

            List<ConsumerRecord<Long, String>> data = toList(records);
            executor.execute(() -> handlers.forEach(handler -> handler.handle(data)));
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

    @PreDestroy
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
        executor.shutdown();
        LOGGER.info("Completed polling.");
    }
}
