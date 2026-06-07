package rgo.nativekafka.consumer.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rgo.nativekafka.common.Asserts;
import rgo.nativekafka.common.ConsumerFactory;
import rgo.nativekafka.common.api.RequestMessage;
import rgo.nativekafka.common.kafka.KafkaUtils;
import rgo.nativekafka.common.metrics.MetricsService;
import rgo.nativekafka.consumer.service.handler.DataHandler;
import rgo.nativekafka.consumer.spring.properties.KafkaConsumerProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static rgo.nativekafka.common.kafka.KafkaUtils.lagSec;
import static rgo.nativekafka.common.kafka.StringifyUtils.briefPartitions;
import static rgo.nativekafka.common.kafka.StringifyUtils.briefRecord;

public class SyncNativeConsumer implements AutoCloseable, ConsumerRebalanceListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(SyncNativeConsumer.class);

    private final Consumer<Long, String> consumer;
    private final DataHandler handler;
    private final MetricsService metricsService;
    private final KafkaConsumerProperties properties;
    private final AtomicReference<State> state = new AtomicReference<>(State.CREATED);
    private final ScheduledExecutorService pollingExecutor;
    private final List<TopicPartition> assignedPartitions = new ArrayList<>();

    public SyncNativeConsumer(
            ConsumerFactory consumerFactory,
            DataHandler handler,
            MetricsService metricsService,
            KafkaConsumerProperties properties
    ) {
        this.properties = Asserts.nonNull(properties, "properties");
        Asserts.nonEmpty(properties.getTopic(), "topic");
        Asserts.positive(properties.getCheckTimeoutMillis(), "checkTimeoutMillis");
        Asserts.positive(properties.getPollTimeoutMillis(), "pollTimeoutMillis");
        Asserts.positive(properties.getCloseTimeoutMillis(), "closeTimeoutMillis");
        Asserts.nonEmpty(properties.getProperties(), "properties");
        this.handler = Asserts.nonNull(handler, "handler");
        this.metricsService = Asserts.nonNull(metricsService, "metricsService");
        this.consumer = Asserts.nonNull(consumerFactory, "consumerFactory").create(properties.getProperties());
        String shortClientId = KafkaUtils.shortClientId(properties.getProperties());
        this.pollingExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> new Thread(runnable, shortClientId));
    }

    public void checkConnect() {
        KafkaUtils.checkTopic(
                consumer,
                properties.getTopic(),
                Duration.ofMillis(properties.getCheckTimeoutMillis()));
    }

    public synchronized void run() {
        if (state.compareAndSet(State.CREATED, State.STARTED)) {
            pollingExecutor.scheduleWithFixedDelay(
                    this::loopPolling,
                    0,
                    properties.getPollTimeoutMillis(),
                    TimeUnit.MILLISECONDS);
            LOGGER.info("Started polling.");
        }
    }

    private void loopPolling() {
        if (!isStarted()) {
            LOGGER.debug("Consumer is not started.");
            return;
        }

        try {
            consumer.subscribe(List.of(properties.getTopic()), this);
            LOGGER.info("Subscribed to kafka topic: {}", properties.getTopic());
            Duration timeout = Duration.ofMillis(properties.getPollTimeoutMillis());

            while (isStarted()) {
                var records = consumer.poll(timeout);
                measure(records);
                if (!records.isEmpty()) {
                    logRecords(records);
                    handle(records);
                }
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            LOGGER.warn("Kafka consumer has woken up.");
        } catch (Exception e) {
            LOGGER.error("Poll or message batch processing failed.", e);
            metricsService.incFailedConsumerPollMainTopic();
        } finally {
            consumer.unsubscribe();
            metricsService.reportInFlightVectorsCount(0);
        }
    }

    private boolean isStarted() {
        return state.get() == State.STARTED && !Thread.currentThread().isInterrupted();
    }

    private void measure(ConsumerRecords<Long, String> records) {
        metricsService.reportIncomingMessages(records.count());
        metricsService.reportConsumerLagSeconds(properties.getTopic(), lagSec(records));
    }

    private void logRecords(ConsumerRecords<Long, String> records) {
        StringBuilder sb = new StringBuilder().append("Fetched (s=").append(records.count()).append("):");
        records.forEach(record -> sb.append("\n\t").append(briefRecord(record)));
        LOGGER.info(sb.toString());
    }

    private void handle(ConsumerRecords<Long, String> records) throws Exception {
        List<RequestMessage<String>> messages = new ArrayList<>(records.count());
        records.forEach(record -> messages.add(KafkaUtils.rqMessage(record)));
        handler.handle(messages);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        Set<TopicPartition> prevAssigned = new HashSet<>(assignedPartitions);
        assignedPartitions.addAll(partitions);
        LOGGER.info("On partitions assigned. prev={}, curr={}",
                briefPartitions(prevAssigned),
                briefPartitions(assignedPartitions));
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        Set<TopicPartition> prevAssigned = new HashSet<>(assignedPartitions);
        assignedPartitions.removeAll(partitions);
        LOGGER.info("On partitions revoked. prev={}, curr={}",
                briefPartitions(prevAssigned),
                briefPartitions(assignedPartitions));
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        Set<TopicPartition> prevAssigned = new HashSet<>(assignedPartitions);
        assignedPartitions.removeAll(partitions);
        LOGGER.info("On partitions lost. prev={}, curr={}",
                briefPartitions(prevAssigned),
                briefPartitions(assignedPartitions));
    }

    @Override
    public synchronized void close() {
        State previousState = state.getAndSet(State.CLOSED);
        if (previousState == State.CLOSED) {
            return;
        }
        if (previousState == State.STARTED) {
            consumer.wakeup();
        }
        pollingExecutor.execute(consumer::close);
        pollingExecutor.shutdown();
    }

    private enum State {
        CREATED, STARTED, CLOSED
    }
}
