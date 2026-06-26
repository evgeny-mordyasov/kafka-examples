package rgo.nativekafka.consumer.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
import rgo.nativekafka.consumer.service.handler.DataHandler;
import rgo.nativekafka.consumer.spring.properties.KafkaConsumerProperties;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@SuppressWarnings({"resource", "ConstantConditions"})
class NativeConsumerTest {

    private static final String TOPIC = "request_topic";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

    @Test
    void createInstance_withNullConsumerFactory_ShouldThrowIllegalArgumentException() {
        ConsumerFactory consumerFactory = null;
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        KafkaConsumerProperties properties = properties();

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
        KafkaConsumerProperties properties = properties();

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
        KafkaConsumerProperties properties = properties();

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
        KafkaConsumerProperties properties = properties();
        properties.setTopic(null);

        assertThatThrownBy(() -> new NativeConsumer(consumerFactory(kafkaConsumer), mock(DataHandler.class), mock(MetricsService.class), properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[topic] must not be null or empty.");
    }

    @Test
    void createInstance_withEmptyTopic_ShouldThrowIllegalArgumentException() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        KafkaConsumerProperties properties = properties();
        properties.setTopic("");

        assertThatThrownBy(() -> new NativeConsumer(consumerFactory(kafkaConsumer), mock(DataHandler.class), mock(MetricsService.class), properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[topic] must not be null or empty.");
    }

    @Test
    void createInstance_withNonPositiveCheckTimeoutMillis_ShouldThrowIllegalArgumentException() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        KafkaConsumerProperties properties = properties();
        properties.setCheckTimeoutMillis(0);

        assertThatThrownBy(() -> new NativeConsumer(consumerFactory(kafkaConsumer), mock(DataHandler.class), mock(MetricsService.class), properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[checkTimeoutMillis] must be a positive number");
    }

    @Test
    void createInstance_withNonPositivePollTimeoutMillis_ShouldThrowIllegalArgumentException() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        KafkaConsumerProperties properties = properties();
        properties.setPollTimeoutMillis(0);

        assertThatThrownBy(() -> new NativeConsumer(consumerFactory(kafkaConsumer), mock(DataHandler.class), mock(MetricsService.class), properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[pollTimeoutMillis] must be a positive number");
    }

    @Test
    void createInstance_withNonPositiveCloseTimeoutMillis_ShouldThrowIllegalArgumentException() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        KafkaConsumerProperties properties = properties();
        properties.setCloseTimeoutMillis(0);

        assertThatThrownBy(() -> new NativeConsumer(consumerFactory(kafkaConsumer), mock(DataHandler.class), mock(MetricsService.class), properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[closeTimeoutMillis] must be a positive number");
    }

    @Test
    void createInstance_withNullPropertiesMap_ShouldThrowIllegalArgumentException() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        KafkaConsumerProperties properties = properties();
        properties.setProperties(null);

        assertThatThrownBy(() -> new NativeConsumer(consumerFactory(kafkaConsumer), mock(DataHandler.class), mock(MetricsService.class), properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[properties] must not be null or empty.");
    }

    @Test
    void createInstance_withEmptyPropertiesMap_ShouldThrowIllegalArgumentException() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        KafkaConsumerProperties properties = properties();
        properties.setProperties(Map.of());

        assertThatThrownBy(() -> new NativeConsumer(consumerFactory(kafkaConsumer), mock(DataHandler.class), mock(MetricsService.class), properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[properties] must not be null or empty.");
    }

    @Test
    void createInstance_withoutClientId_ShouldThrowIllegalArgumentException() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        KafkaConsumerProperties properties = properties();
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
        KafkaConsumerProperties properties = properties();
        doReturn(kafkaConsumer).when(consumerFactory).create(properties.getProperties());

        NativeConsumer consumer = new NativeConsumer(consumerFactory, handler, metricsService, properties);

        assertThat(consumer).isNotNull();
    }

    @Test
    void checkConnect_withExistingTopic_ShouldNotCallDependencies() {
        TestConsumer testConsumer = new TestConsumer(TOPIC);
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(testConsumer.getConsumer(), handler, metricsService);

        consumer.checkConnect();
    }

    @Test
    void checkConnect_withMissingTopic_ShouldThrowIllegalStateException() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(kafkaConsumer, handler, metricsService);

        assertThatThrownBy(consumer::checkConnect)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Failed to connect to topic " + TOPIC);
    }

    @Test
    void pollOnce_withEmptyRecords_ShouldReportZeroIncomingMessages() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(kafkaConsumer, handler, metricsService);

        consumer.pollOnce(Duration.ZERO);

        verify(metricsService).reportIncomingMessages(0);
        verify(metricsService).reportConsumerLagSeconds(TOPIC, 0);
        verify(handler, never()).handle(any());
        assertThat(kafkaConsumer.committed(Set.of(PARTITION))).isEmpty();
    }

    @Test
    void pollOnce_oneRecord_handleFailed() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(kafkaConsumer, handler, metricsService);
        addRecord(kafkaConsumer, 0, 0L, 7L, "payload");
        doThrow(new RuntimeException("Simulated error")).when(handler).handle(any());

        consumer.pollOnce(Duration.ZERO);

        verify(metricsService).reportIncomingMessages(1);
        verify(metricsService).reportConsumerLagSeconds(eq(TOPIC), anyLong());
        assertThat(kafkaConsumer.committed(Set.of(PARTITION))).isEmpty();
        assertThat(kafkaConsumer.position(PARTITION)).isZero();
    }

    @Test
    void pollOnce_manyRecordsSecondHandleFailed_ShouldCommitBeforeFailedOffsetAndSeekFailedRecord() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(kafkaConsumer, handler, metricsService);
        addRecord(kafkaConsumer, 0, 0L, 10L, "first");
        addRecord(kafkaConsumer, 0, 1L, 20L, "second");
        org.mockito.Mockito.doNothing()
                .doThrow(new RuntimeException("Simulated error"))
                .when(handler).handle(any());

        consumer.pollOnce(Duration.ZERO);

        Map<TopicPartition, OffsetAndMetadata> committed = kafkaConsumer.committed(Set.of(PARTITION));
        assertThat(committed).containsKey(PARTITION);
        assertThat(committed.get(PARTITION).offset()).isEqualTo(1L);
        assertThat(kafkaConsumer.position(PARTITION)).isEqualTo(1L);
    }

    @Test
    void pollOnce_manyPartitionsHandleFailed_ShouldProcessOtherPartitions() {
        TopicPartition failedPartition = new TopicPartition(TOPIC, 0);
        TopicPartition processedPartition = new TopicPartition(TOPIC, 1);
        Consumer<Long, String> kafkaConsumer = mock(Consumer.class);
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = new NativeConsumer(consumerFactory(kafkaConsumer), handler, metricsService, properties());
        Map<TopicPartition, List<ConsumerRecord<Long, String>>> batch = new LinkedHashMap<>();
        batch.put(failedPartition, List.of(new ConsumerRecord<>(TOPIC, 0, 0L, 10L, "failed")));
        batch.put(processedPartition, List.of(new ConsumerRecord<>(TOPIC, 1, 0L, 20L, "processed")));
        doReturn(new ConsumerRecords<>(batch)).when(kafkaConsumer).poll(Duration.ZERO);
        doAnswer(invocation -> {
            RequestMessage<String> message = invocation.getArgument(0);
            if ("failed".equals(message.payload())) {
                throw new RuntimeException("Simulated error");
            }
            return null;
        }).when(handler).handle(any());

        consumer.pollOnce(Duration.ZERO);

        verify(handler, times(2)).handle(any());
        verify(kafkaConsumer).seek(failedPartition, 0L);
        verify(kafkaConsumer).commitSync(Map.of(processedPartition, new OffsetAndMetadata(1L)));
    }

    @Test
    @SuppressWarnings("unchecked")
    void pollOnce_withValidRecord_ShouldHandleMessagesAndCommitOffsets() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(kafkaConsumer, handler, metricsService);
        addRecord(kafkaConsumer, 0, 0L, 42L, "payload");

        consumer.pollOnce(Duration.ZERO);

        ArgumentCaptor<RequestMessage<String>> captor = ArgumentCaptor.forClass(RequestMessage.class);
        verify(handler).handle(captor.capture());
        assertThat(captor.getValue())
                .satisfies(message -> {
                    assertThat(message.key()).isEqualTo(42L);
                    assertThat(message.payload()).isEqualTo("payload");
                    assertThat(message.metadata().topic()).isEqualTo(TOPIC);
                    assertThat(message.metadata().partition()).isZero();
                    assertThat(message.metadata().offset()).isZero();
                });
        verify(metricsService).reportIncomingMessages(1);
        verify(metricsService).reportConsumerLagSeconds(eq(TOPIC), anyLong());
        Map<TopicPartition, OffsetAndMetadata> committed = kafkaConsumer.committed(Set.of(PARTITION));
        assertThat(committed).containsKey(PARTITION);
        assertThat(committed.get(PARTITION).offset()).isEqualTo(1L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void pollOnce_withManyRecords_ShouldHandleAllMessagesAndCommitOffsets() {
        TestConsumer testConsumer = new TestConsumer(TOPIC);
        MockConsumer<Long, String> kafkaConsumer = testConsumer.getConsumer();
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(kafkaConsumer, handler, metricsService);
        testConsumer.assign();
        testConsumer.addRecord(0, 0L, 10L, "first");
        testConsumer.addRecord(1, 0L, 20L, "second");
        testConsumer.addRecord(2, 0L, 30L, "third");

        consumer.pollOnce(Duration.ZERO);

        ArgumentCaptor<RequestMessage<String>> captor = ArgumentCaptor.forClass(RequestMessage.class);
        verify(handler, times(3)).handle(captor.capture());
        assertThat(captor.getAllValues())
                .extracting(RequestMessage::payload)
                .containsExactlyInAnyOrder("first", "second", "third");
        verify(metricsService).reportIncomingMessages(3);
        verify(metricsService).reportConsumerLagSeconds(eq(TOPIC), anyLong());
        Map<TopicPartition, OffsetAndMetadata> committed = kafkaConsumer.committed(testConsumer.getPartitions());
        assertThat(committed)
                .containsEntry(new TopicPartition(TOPIC, 0), new OffsetAndMetadata(1L))
                .containsEntry(new TopicPartition(TOPIC, 1), new OffsetAndMetadata(1L))
                .containsEntry(new TopicPartition(TOPIC, 2), new OffsetAndMetadata(1L));
    }

    @Test
    void run_pollFailed_ShouldIncrementFailedPollMetric() throws InterruptedException {
        MockConsumer<Long, String> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        kafkaConsumer.updatePartitions(TOPIC, List.of(new PartitionInfo(TOPIC, 0, null, null, null)));
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(kafkaConsumer, handler, metricsService);
        kafkaConsumer.setPollException(new KafkaException("boom"));

        consumer.run();

        verify(metricsService, timeout(1_000)).incFailedConsumerPoll();
        verify(handler, never()).handle(any());
        consumer.close();
        awaitClosed(kafkaConsumer);
    }

    @Test
    void run_withCreatedConsumer_ShouldPollUntilClose() throws InterruptedException {
        TestConsumer testConsumer = new TestConsumer(TOPIC);
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(testConsumer.getConsumer(), handler, metricsService);
        testConsumer.scheduleRebalance();

        consumer.run();

        verify(metricsService, timeout(1_000).atLeastOnce()).reportIncomingMessages(0);
        verify(handler, never()).handle(any());
        consumer.close();
        awaitClosed(testConsumer.getConsumer());
    }

    @Test
    void onPartitionsAssigned_withRebalanceCallbacks_ShouldUpdateWithoutException() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(kafkaConsumer, handler, metricsService);

        consumer.onPartitionsAssigned(List.of(PARTITION));
        consumer.onPartitionsRevoked(List.of(PARTITION));
        consumer.onPartitionsLost(List.of(PARTITION));

        verifyNoInteractions(handler, metricsService);
        assertThat(kafkaConsumer.closed()).isFalse();
    }

    @Test
    void close_withCreatedConsumer_ShouldCloseKafkaConsumer() throws InterruptedException {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(kafkaConsumer, handler, metricsService);

        consumer.close();

        awaitClosed(kafkaConsumer);
        verifyNoInteractions(handler, metricsService);
    }

    @Test
    void close_calledTwice_ShouldCloseKafkaConsumerOnce() {
        Consumer<Long, String> kafkaConsumer = mock(Consumer.class);
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = new NativeConsumer(consumerFactory(kafkaConsumer), handler, metricsService, properties());

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
        NativeConsumer consumer = new NativeConsumer(consumerFactory(kafkaConsumer), handler, metricsService, properties());

        consumer.close();
        consumer.run();

        verify(kafkaConsumer, timeout(1_000).times(1)).close(any(CloseOptions.class));
        verify(kafkaConsumer, times(0)).poll(any(Duration.class));
        verifyNoInteractions(handler, metricsService);
    }

    private static NativeConsumer consumer(
            MockConsumer<Long, String> kafkaConsumer,
            DataHandler handler,
            MetricsService metricsService
    ) {
        return new NativeConsumer(consumerFactory(kafkaConsumer), handler, metricsService, properties());
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

    private static KafkaConsumerProperties properties() {
        KafkaConsumerProperties properties = new KafkaConsumerProperties();
        properties.setTopic(TOPIC);
        properties.setCheckTimeoutMillis(100);
        properties.setPollTimeoutMillis(10);
        properties.setCloseTimeoutMillis(100);
        properties.setProperties(Map.of(ConsumerConfig.CLIENT_ID_CONFIG, "replay-test@localhost"));
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
