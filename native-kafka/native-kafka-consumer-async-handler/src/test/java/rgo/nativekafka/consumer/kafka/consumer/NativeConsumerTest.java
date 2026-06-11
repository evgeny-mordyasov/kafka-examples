package rgo.nativekafka.consumer.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import rgo.nativekafka.common.api.RequestMessage;
import rgo.nativekafka.common.kafka.ConsumerFactory;
import rgo.nativekafka.common.kafka.TestConsumer;
import rgo.nativekafka.common.metrics.MetricsService;
import rgo.nativekafka.consumer.exception.BatchProcessingException;
import rgo.nativekafka.consumer.service.handler.DataHandler;
import rgo.nativekafka.consumer.spring.properties.KafkaConsumerProperties;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings({"resource", "ConstantConditions"})
class NativeConsumerTest {

    private static final String TOPIC = "request_topic";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

    @Test
    void createInstance_withNullConsumerFactory_ShouldThrowIllegalArgumentException() {
        ConsumerFactory consumerFactory = null;
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        KafkaConsumerProperties properties = properties(10);

        assertThatThrownBy(() -> new NativeConsumer(consumerFactory, handler, metricsService, properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[consumerFactory] must not be null.");
    }

    @Test
    void createInstance_withNullHandler_ShouldThrowIllegalArgumentException() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        ConsumerFactory consumerFactory = consumerFactory(kafkaConsumer);
        DataHandler handler = null;
        MetricsService metricsService = mock(MetricsService.class);
        KafkaConsumerProperties properties = properties(10);

        assertThatThrownBy(() -> new NativeConsumer(consumerFactory, handler, metricsService, properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[handler] must not be null.");
    }

    @Test
    void createInstance_withNullMetricsService_ShouldThrowIllegalArgumentException() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        ConsumerFactory consumerFactory = consumerFactory(kafkaConsumer);
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = null;
        KafkaConsumerProperties properties = properties(10);

        assertThatThrownBy(() -> new NativeConsumer(consumerFactory, handler, metricsService, properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[metricsService] must not be null.");
    }

    @Test
    void createInstance_withNullProperties_ShouldThrowIllegalArgumentException() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        ConsumerFactory consumerFactory = consumerFactory(kafkaConsumer);
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        KafkaConsumerProperties properties = null;

        assertThatThrownBy(() -> new NativeConsumer(consumerFactory, handler, metricsService, properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[properties] must not be null.");
    }

    @Test
    void createInstance_withNullTopic_ShouldThrowIllegalArgumentException() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        KafkaConsumerProperties properties = properties(10);
        properties.setTopic(null);

        assertThatThrownBy(() -> new NativeConsumer(consumerFactory(kafkaConsumer), mock(DataHandler.class), mock(MetricsService.class), properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[topic] must not be null or empty.");
    }

    @Test
    void createInstance_withEmptyTopic_ShouldThrowIllegalArgumentException() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        KafkaConsumerProperties properties = properties(10);
        properties.setTopic("");

        assertThatThrownBy(() -> new NativeConsumer(consumerFactory(kafkaConsumer), mock(DataHandler.class), mock(MetricsService.class), properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[topic] must not be null or empty.");
    }

    @Test
    void createInstance_withNonPositiveCheckTimeoutMillis_ShouldThrowIllegalArgumentException() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        KafkaConsumerProperties properties = properties(10);
        properties.setCheckTimeoutMillis(0);

        assertThatThrownBy(() -> new NativeConsumer(consumerFactory(kafkaConsumer), mock(DataHandler.class), mock(MetricsService.class), properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[checkTimeoutMillis] must be a positive number");
    }

    @Test
    void createInstance_withNonPositivePollTimeoutMillis_ShouldThrowIllegalArgumentException() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        KafkaConsumerProperties properties = properties(10);
        properties.setPollTimeoutMillis(0);

        assertThatThrownBy(() -> new NativeConsumer(consumerFactory(kafkaConsumer), mock(DataHandler.class), mock(MetricsService.class), properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[pollTimeoutMillis] must be a positive number");
    }

    @Test
    void createInstance_withNonPositiveCloseTimeoutMillis_ShouldThrowIllegalArgumentException() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        KafkaConsumerProperties properties = properties(10);
        properties.setCloseTimeoutMillis(0);

        assertThatThrownBy(() -> new NativeConsumer(consumerFactory(kafkaConsumer), mock(DataHandler.class), mock(MetricsService.class), properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[closeTimeoutMillis] must be a positive number");
    }

    @Test
    void createInstance_withNonPositiveBufferThreshold_ShouldThrowIllegalArgumentException() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        KafkaConsumerProperties properties = properties(0);

        assertThatThrownBy(() -> new NativeConsumer(consumerFactory(kafkaConsumer), mock(DataHandler.class), mock(MetricsService.class), properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[bufferThreshold] must be a positive number");
    }

    @Test
    void createInstance_withNullPropertiesMap_ShouldThrowIllegalArgumentException() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        KafkaConsumerProperties properties = properties(10);
        properties.setProperties(null);

        assertThatThrownBy(() -> new NativeConsumer(consumerFactory(kafkaConsumer), mock(DataHandler.class), mock(MetricsService.class), properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[properties] must not be null or empty.");
    }

    @Test
    void createInstance_withEmptyPropertiesMap_ShouldThrowIllegalArgumentException() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        KafkaConsumerProperties properties = properties(10);
        properties.setProperties(Map.of());

        assertThatThrownBy(() -> new NativeConsumer(consumerFactory(kafkaConsumer), mock(DataHandler.class), mock(MetricsService.class), properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[properties] must not be null or empty.");
    }

    @Test
    void createInstance_withoutClientId_ShouldThrowIllegalArgumentException() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        KafkaConsumerProperties properties = properties(10);
        properties.setProperties(Map.of("bootstrap.servers", "localhost:9092"));

        assertThatThrownBy(() -> new NativeConsumer(consumerFactory(kafkaConsumer), mock(DataHandler.class), mock(MetricsService.class), properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[client.id] must not be null.");
    }

    @Test
    void createInstance_withValidDependencies_ShouldCreateConsumer() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        KafkaConsumerProperties properties = properties(10);
        doReturn(kafkaConsumer).when(consumerFactory).create(properties.getProperties());

        NativeConsumer consumer = new NativeConsumer(consumerFactory, handler, metricsService, properties);

        assertThat(consumer).isNotNull();
        verify(consumerFactory).create(properties.getProperties());
    }

    @Test
    void checkConnect_withExistingTopic_ShouldNotCallDependencies() {
        TestConsumer testConsumer = new TestConsumer(TOPIC);
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(testConsumer.getConsumer(), handler, metricsService, 10);

        consumer.checkConnect();
    }

    @Test
    void checkConnect_withMissingTopic_ShouldThrowIllegalStateException() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(kafkaConsumer, handler, metricsService, 10);

        assertThatThrownBy(consumer::checkConnect)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Failed to connect to topic " + TOPIC);
    }

    @Test
    void pollOnce_withEmptyRecords_ShouldCommitAllOffsets() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(kafkaConsumer, handler, metricsService, 10);

        consumer.pollOnce(Duration.ZERO);

        verify(metricsService).reportIncomingMessages(0);
        verify(metricsService).reportConsumerLagSeconds(TOPIC, 0);
        verify(metricsService).reportInFlightVectorsCount(0);
        verify(handler, never()).handle(anyList());
        Map<TopicPartition, OffsetAndMetadata> committed = kafkaConsumer.committed(Set.of(PARTITION));
        assertThat(committed).containsKey(PARTITION);
        assertThat(committed.get(PARTITION).offset()).isZero();
    }

    @Test
    void pollOnce_oneRecord_handleFailed() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(kafkaConsumer, handler, metricsService, 10);
        addRecord(kafkaConsumer, 0, 0L, 7L, "payload");
        CompletableFuture<Void> failedFuture = CompletableFuture.failedFuture(new RuntimeException("Simulated error"));
        when(handler.handle(anyList())).thenReturn(failedFuture);

        assertThatThrownBy(() -> consumer.pollOnce(Duration.ZERO))
                .isInstanceOf(BatchProcessingException.class)
                .hasMessage("Failed to process message batch")
                .hasCauseInstanceOf(RuntimeException.class);

        verify(handler).handle(anyList());
        verify(metricsService).reportIncomingMessages(1);
        verify(metricsService).reportConsumerLagSeconds(eq(TOPIC), anyLong());
        verify(metricsService).reportInFlightVectorsCount(1);
    }

    @Test
    @SuppressWarnings("unchecked")
    void pollOnce_withValidRecord_ShouldHandleMessagesAndAddInFlightBatch() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(kafkaConsumer, handler, metricsService, 10);
        addRecord(kafkaConsumer, 0, 0L, 42L, "payload");
        when(handler.handle(anyList())).thenReturn(new CompletableFuture<>());

        consumer.pollOnce(Duration.ZERO);

        ArgumentCaptor<List<RequestMessage<String>>> captor = ArgumentCaptor.forClass(List.class);
        verify(handler).handle(captor.capture());
        assertThat(captor.getValue())
                .singleElement()
                .satisfies(message -> {
                    assertThat(message.key()).isEqualTo(42L);
                    assertThat(message.payload()).isEqualTo("payload");
                    assertThat(message.metadata().topic()).isEqualTo(TOPIC);
                    assertThat(message.metadata().partition()).isZero();
                    assertThat(message.metadata().offset()).isZero();
                });
        verify(metricsService).reportIncomingMessages(1);
        verify(metricsService).reportConsumerLagSeconds(eq(TOPIC), anyLong());
        verify(metricsService).reportInFlightVectorsCount(1);
    }

    @Test
    @SuppressWarnings("unchecked")
    void pollOnce_withManyRecords_ShouldHandleAllMessagesAndCommitOffsets() {
        TestConsumer testConsumer = new TestConsumer(TOPIC);
        MockConsumer<Long, String> kafkaConsumer = testConsumer.getConsumer();
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(kafkaConsumer, handler, metricsService, 10);
        testConsumer.assign();
        testConsumer.addRecord(0, 0L, 10L, "first");
        testConsumer.addRecord(1, 0L, 20L, "second");
        testConsumer.addRecord(2, 0L, 30L, "third");
        when(handler.handle(anyList())).thenReturn(CompletableFuture.completedFuture(null));

        consumer.pollOnce(Duration.ZERO);

        ArgumentCaptor<List<RequestMessage<String>>> captor = ArgumentCaptor.forClass(List.class);
        verify(handler).handle(captor.capture());
        assertThat(captor.getValue())
                .extracting(RequestMessage::payload)
                .containsExactlyInAnyOrder("first", "second", "third");
        verify(metricsService).reportIncomingMessages(3);
        verify(metricsService).reportConsumerLagSeconds(eq(TOPIC), anyLong());
        verify(metricsService).reportInFlightVectorsCount(3);
        Map<TopicPartition, OffsetAndMetadata> committed = kafkaConsumer.committed(testConsumer.getPartitions());
        assertThat(committed)
                .containsEntry(new TopicPartition(TOPIC, 0), new OffsetAndMetadata(1L))
                .containsEntry(new TopicPartition(TOPIC, 1), new OffsetAndMetadata(1L))
                .containsEntry(new TopicPartition(TOPIC, 2), new OffsetAndMetadata(1L));
    }

    @Test
    void pollOnce_withCompletedSuccessfulFuture_ShouldCommitNextOffset() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(kafkaConsumer, handler, metricsService, 10);
        addRecord(kafkaConsumer, 0, 0L, 42L, "payload");
        when(handler.handle(anyList())).thenReturn(CompletableFuture.completedFuture(null));

        consumer.pollOnce(Duration.ZERO);
        consumer.pollOnce(Duration.ZERO);

        verify(handler).handle(anyList());
        verify(metricsService).reportIncomingMessages(1);
        verify(metricsService).reportIncomingMessages(0);
        Map<TopicPartition, OffsetAndMetadata> committed = kafkaConsumer.committed(Set.of(PARTITION));
        assertThat(committed).containsKey(PARTITION);
        assertThat(committed.get(PARTITION).offset()).isEqualTo(1L);
    }

    @Test
    void pollOnce_withCancelledFuture_ShouldThrowBatchProcessingException() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(kafkaConsumer, handler, metricsService, 10);
        CompletableFuture<Void> cancelledFuture = new CompletableFuture<>();
        cancelledFuture.cancel(false);
        addRecord(kafkaConsumer, 0, 0L, 42L, "payload");
        when(handler.handle(anyList())).thenReturn(cancelledFuture);

        assertThatThrownBy(() -> consumer.pollOnce(Duration.ZERO))
                .isInstanceOf(BatchProcessingException.class)
                .hasMessage("Message batch processing was not completed successfully. State: CANCELLED");
    }

    @Test
    void pollOnce_withLaterBatchCompletedBeforeEarlierBatch_ShouldNotCommitLaterOffset() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(kafkaConsumer, handler, metricsService, 10);
        CompletableFuture<Void> firstFuture = new CompletableFuture<>();
        CompletableFuture<Void> secondFuture = CompletableFuture.completedFuture(null);
        when(handler.handle(anyList())).thenReturn(firstFuture, secondFuture);

        addRecord(kafkaConsumer, 0, 0L, 1L, "first");
        consumer.pollOnce(Duration.ZERO);
        addRecord(kafkaConsumer, 0, 1L, 2L, "second");
        consumer.pollOnce(Duration.ZERO);

        assertThat(kafkaConsumer.committed(Set.of(PARTITION))).isEmpty();
    }

    @Test
    void pollOnce_withBufferOverflow_ShouldPauseAssignedPartitionsAndReportPaused() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(kafkaConsumer, handler, metricsService, 1);
        consumer.onPartitionsAssigned(List.of(PARTITION));
        addRecord(kafkaConsumer, 0, 0L, 1L, "first");
        addRecord(kafkaConsumer, 0, 1L, 2L, "second");
        when(handler.handle(anyList())).thenReturn(new CompletableFuture<>());

        consumer.pollOnce(Duration.ZERO);
        consumer.pollOnce(Duration.ZERO);

        assertThat(kafkaConsumer.paused()).containsExactly(PARTITION);
        verify(handler).handle(anyList());
        verify(metricsService).reportConsumerPaused();
        verify(metricsService, atLeastOnce()).reportInFlightVectorsCount(2);
    }

    @Test
    void pollOnce_withBufferBelowThreshold_ShouldResumePausedPartitionsAndReportResumed() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(kafkaConsumer, handler, metricsService, 10);
        kafkaConsumer.pause(List.of(PARTITION));

        consumer.pollOnce(Duration.ZERO);

        assertThat(kafkaConsumer.paused()).isEmpty();
        verify(handler, never()).handle(anyList());
        verify(metricsService).reportConsumerResumed();
        verify(metricsService).reportIncomingMessages(0);
        verify(metricsService).reportInFlightVectorsCount(0);
    }

    @Test
    void run_pollFailed_ShouldIncrementFailedPollMetricAndCleanupBufferMetric() throws InterruptedException {
        MockConsumer<Long, String> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        kafkaConsumer.updatePartitions(TOPIC, List.of(new PartitionInfo(TOPIC, 0, null, null, null)));
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(kafkaConsumer, handler, metricsService, 10);
        kafkaConsumer.setPollException(new KafkaException("boom"));

        consumer.run();

        verify(metricsService, timeout(1_000)).incFailedConsumerPoll();
        verify(metricsService, timeout(1_000).atLeastOnce()).reportInFlightVectorsCount(0);
        verify(handler, never()).handle(anyList());
        consumer.close();
        awaitClosed(kafkaConsumer);
    }

    @Test
    void run_withCreatedConsumer_ShouldPollUntilClose() throws InterruptedException {
        TestConsumer testConsumer = new TestConsumer(TOPIC);
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(testConsumer.getConsumer(), handler, metricsService, 10);
        testConsumer.scheduleRebalance();

        consumer.run();

        verify(metricsService, timeout(1_000).atLeastOnce()).reportIncomingMessages(0);
        verify(metricsService, timeout(1_000).atLeastOnce()).reportInFlightVectorsCount(0);
        verify(handler, never()).handle(anyList());
        consumer.close();
        awaitClosed(testConsumer.getConsumer());
    }

    @Test
    void close_withCreatedConsumer_ShouldCloseKafkaConsumer() throws InterruptedException {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(kafkaConsumer, handler, metricsService, 10);

        consumer.close();

        awaitClosed(kafkaConsumer);
        verifyNoInteractions(handler, metricsService);
    }

    @Test
    void onPartitionsAssigned_withRebalanceCallbacks_ShouldUpdateWithoutException() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(kafkaConsumer, handler, metricsService, 10);

        consumer.onPartitionsAssigned(List.of(PARTITION));
        consumer.onPartitionsRevoked(List.of(PARTITION));
        consumer.onPartitionsLost(List.of(PARTITION));

        verifyNoInteractions(handler, metricsService);
        assertThat(kafkaConsumer.closed()).isFalse();
    }

    @Test
    void close_calledTwice_ShouldCloseKafkaConsumerOnce() {
        Consumer<Long, String> kafkaConsumer = mock(Consumer.class);
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = new NativeConsumer(consumerFactory(kafkaConsumer), handler, metricsService, properties(10));

        consumer.close();
        consumer.close();

        verify(kafkaConsumer, timeout(1_000).times(1)).close(any(CloseOptions.class));
        verifyNoInteractions(handler, metricsService);
    }

    @Test
    void run_afterClose_ShouldNotPollKafkaConsumer() {
        Consumer<Long, String> kafkaConsumer = mock(Consumer.class);
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = new NativeConsumer(consumerFactory(kafkaConsumer), handler, metricsService, properties(10));

        consumer.close();
        consumer.run();

        verify(kafkaConsumer, timeout(1_000).times(1)).close(any(CloseOptions.class));
        verify(kafkaConsumer, times(0)).poll(any(Duration.class));
        verifyNoInteractions(handler, metricsService);
    }

    private static NativeConsumer consumer(
            MockConsumer<Long, String> kafkaConsumer,
            DataHandler handler,
            MetricsService metricsService,
            int bufferThreshold
    ) {
        return new NativeConsumer(consumerFactory(kafkaConsumer), handler, metricsService, properties(bufferThreshold));
    }

    private static MockConsumer<Long, String> kafkaConsumer() {
        MockConsumer<Long, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.assign(List.of(PARTITION));
        consumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
        return consumer;
    }

    private static void addRecord(MockConsumer<Long, String> consumer, int partition, long offset, Long key, String value) {
        consumer.addRecord(new ConsumerRecord<>(TOPIC, partition, offset, key, value));
    }

    private static KafkaConsumerProperties properties(int bufferThreshold) {
        KafkaConsumerProperties properties = new KafkaConsumerProperties();
        properties.setTopic(TOPIC);
        properties.setCheckTimeoutMillis(100);
        properties.setPollTimeoutMillis(10);
        properties.setCloseTimeoutMillis(100);
        properties.setBufferThreshold(bufferThreshold);
        properties.setProperties(Map.of(ConsumerConfig.CLIENT_ID_CONFIG, "async-test@localhost"));
        return properties;
    }

    private static ConsumerFactory consumerFactory(Consumer<Long, String> kafkaConsumer) {
        return new ConsumerFactory() {
            @Override
            @SuppressWarnings("unchecked")
            public <K, V> Consumer<K, V> create(Map<String, Object> properties) {
                return (Consumer<K, V>) kafkaConsumer;
            }
        };
    }

    private static void awaitClosed(MockConsumer<Long, String> consumer) throws InterruptedException {
        for (int i = 0; i < 50 && !consumer.closed(); i++) {
            Thread.sleep(10);
        }
        assertThat(consumer.closed()).isTrue();
    }
}
