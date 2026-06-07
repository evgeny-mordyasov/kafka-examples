package rgo.nativekafka.consumer.kafka.consumer;

import jakarta.annotation.Nonnull;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rgo.nativekafka.common.ConsumerFactory;
import rgo.nativekafka.consumer.api.RequestMessage;
import rgo.nativekafka.consumer.exception.BatchProcessingException;
import rgo.nativekafka.consumer.kafka.utils.KafkaUtils;
import rgo.nativekafka.consumer.service.DataHandler;
import rgo.nativekafka.consumer.service.metrics.MetricsService;
import rgo.nativekafka.consumer.spring.properties.KafkaConsumerProperties;
import rgo.nativekafka.common.Asserts;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static rgo.nativekafka.consumer.kafka.utils.KafkaUtils.lagSec;
import static rgo.nativekafka.consumer.kafka.utils.StringifyUtils.briefPartitions;
import static rgo.nativekafka.consumer.kafka.utils.StringifyUtils.briefRecord;

public class NativeConsumer implements AutoCloseable, ConsumerRebalanceListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(NativeConsumer.class);

    private final Consumer<Long, String> consumer;
    private final DataHandler handler;
    private final MetricsService metricsService;
    private final KafkaConsumerProperties properties;
    private final AtomicReference<State> state = new AtomicReference<>(State.CREATED);
    private final ScheduledExecutorService pollingExecutor;
    private final InFlightBatchBuffer buffer = new InFlightBatchBuffer();
    private final List<TopicPartition> assignedPartitions = new ArrayList<>();

    public NativeConsumer(
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
        Asserts.positive(properties.getBufferThreshold(), "bufferThreshold");
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
                checkBufferSize();
                var records = consumer.poll(timeout);
                measure(records);
                if (!records.isEmpty()) {
                    logRecords(records);
                    handle(records);
                }
                commitOffsets();
            }
        } catch (WakeupException e) {
            LOGGER.warn("Kafka consumer has woken up.");
        } catch (Exception e) {
            LOGGER.error("Poll failed.", e);
            metricsService.incFailedConsumerPollMainTopic();
        } finally {
            consumer.unsubscribe();
            buffer.clear();
            metricsService.reportInFlightVectorsCount(0);
        }
    }

    private boolean isStarted() {
        return state.get() == State.STARTED && !Thread.currentThread().isInterrupted();
    }

    private void checkBufferSize() {
        if (isOverflow()) {
            if (!assignedPartitions.isEmpty()) {
                LOGGER.debug("Pause partitions: {}", assignedPartitions);
                consumer.pause(assignedPartitions);
                metricsService.reportConsumerPaused();
            }
        } else {
            var paused = consumer.paused();
            if (!paused.isEmpty()) {
                LOGGER.debug("Resume partitions: {}", paused);
                consumer.resume(paused);
                metricsService.reportConsumerResumed();
            }
        }
    }

    private boolean isOverflow() {
        return buffer.getCountOfVectors() > properties.getBufferThreshold();
    }

    private void logRecords(ConsumerRecords<Long, String> records) {
        StringBuilder sb = new StringBuilder().append("Fetched (s=").append(records.count()).append("):");
        records.forEach(record -> sb.append("\n\t").append(briefRecord(record)));
        LOGGER.info(sb.toString());
    }

    private void measure(ConsumerRecords<Long, String> records) {
        metricsService.reportIncomingMessages(records.count());
        if (!isOverflow()) {
            metricsService.reportConsumerLagSeconds(properties.getTopic(), lagSec(records));
        }
    }

    private void handle(ConsumerRecords<Long, String> records) {
        List<RequestMessage<String>> messages = new ArrayList<>(records.count());
        records.forEach(record -> messages.add(KafkaUtils.rqMessage(record)));
        Future<Void> future = handler.handle(messages);
        buffer.add(new InFlightBatch(future, records));
    }

    private void commitOffsets() {
        int countOfVectors = buffer.getCountOfVectors();
        metricsService.reportInFlightVectorsCount(countOfVectors);

        if (countOfVectors == 0) {
            try {
                consumer.commitSync();
            } catch (Exception e) {
                LOGGER.warn("Failed to commit all offsets.", e);
            }
            return;
        }

        List<InFlightBatch> completedBatches = new ArrayList<>();
        Map<TopicPartition, Long> completedMinOffsets = new HashMap<>();
        Map<TopicPartition, Long> allMinOffsets = new HashMap<>();

        for (var batch : buffer) {
            batch.getOffsets().forEach((k, v) -> allMinOffsets.merge(k, v, Math::min));

            Future<Void> future = batch.getFuture();
            if (future.isDone()) {
                var futureState = future.state();
                switch (futureState) {
                    case SUCCESS -> {
                        completedBatches.add(batch);
                        batch.getOffsets().forEach((k, v) -> completedMinOffsets.merge(k, v, Math::min));
                    }
                    case FAILED -> throw new BatchProcessingException("Failed to process message batch", future.exceptionNow());
                    default -> throw new BatchProcessingException("Message batch processing was not completed successfully. State: " + futureState);
                }
            }
        }

        Map<TopicPartition, OffsetAndMetadata> commitOffsets = new HashMap<>();
        for (var entry : completedMinOffsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            Long completedOffset = entry.getValue();
            Long minOffsetInBuffer = allMinOffsets.get(tp);

            if (completedOffset.equals(minOffsetInBuffer)) {
                commitOffsets.put(tp, new OffsetAndMetadata(completedOffset + 1));
            }
        }

        buffer.remove(completedBatches);

        if (!commitOffsets.isEmpty()) {
            try {
                consumer.commitSync(commitOffsets);
                LOGGER.info("Offsets committed: {}", commitOffsets);
            } catch (Exception e) {
                LOGGER.warn("Commit offsets failed. Offsets: {}", commitOffsets, e);
            }
        }
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

    private static class InFlightBatchBuffer implements Iterable<InFlightBatch> {

        private final Queue<InFlightBatch> buffer = new LinkedList<>();

        public int getCountOfVectors() {
            return buffer.stream().mapToInt(InFlightBatch::getSize).sum();
        }

        public void add(InFlightBatch batch) {
            buffer.add(batch);
        }

        public void remove(List<InFlightBatch> batches) {
            buffer.removeAll(batches);
        }

        public void clear() {
            buffer.clear();
        }

        @Override
        @Nonnull
        public Iterator<InFlightBatch> iterator() {
            return buffer.iterator();
        }
    }

    private static class InFlightBatch {

        private final Future<Void> future;
        private final int size;
        private final Map<TopicPartition, Long> offsets;

        public InFlightBatch(Future<Void> future, ConsumerRecords<Long, String> records) {
            this.future = future;
            this.size = records.count();
            this.offsets = maxOffsets(records);
        }

        private Map<TopicPartition, Long> maxOffsets(ConsumerRecords<Long, String> records) {
            Map<TopicPartition, Long> maxOffsets = new HashMap<>();
            for (TopicPartition tp : records.partitions()) {
                List<ConsumerRecord<Long, String>> partitionRecords = records.records(tp);

                long max = Long.MIN_VALUE;
                for (ConsumerRecord<Long, String> r : partitionRecords) {
                    if (r.offset() > max) {
                        max = r.offset();
                    }
                }

                if (max != Long.MIN_VALUE) {
                    maxOffsets.put(tp, max);
                }
            }
            return maxOffsets;
        }

        public Future<Void> getFuture() {
            return future;
        }

        public int getSize() {
            return size;
        }

        public Map<TopicPartition, Long> getOffsets() {
            return offsets;
        }
    }
}
