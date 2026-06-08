package rgo.nativekafka.consumer.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
import rgo.nativekafka.consumer.service.handler.DataHandler;
import rgo.nativekafka.consumer.spring.properties.KafkaConsumerProperties;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
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

        verifyNoInteractions(handler, metricsService);
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

        verifyNoInteractions(metricsService);
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

        verifyNoInteractions(handler);
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

        verifyNoInteractions(handler, metricsService);
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
        verify(consumerFactory).create(properties.getProperties());
        verifyNoInteractions(handler, metricsService);
    }

    @Test
    void checkConnect_withExistingTopic_ShouldNotCallDependencies() {
        TestConsumer testConsumer = new TestConsumer(TOPIC);
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(testConsumer.getConsumer(), handler, metricsService);

        consumer.checkConnect();

        verifyNoInteractions(handler, metricsService);
    }

    @Test
    void checkConnect_withMissingTopic_ShouldThrowIllegalStateException() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(kafkaConsumer, handler, metricsService);

        assertThatThrownBy(consumer::checkConnect)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Failed to connect to topic request_topic");

        verifyNoInteractions(handler, metricsService);
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
        verify(handler, never()).handle(anyList());
        assertThat(kafkaConsumer.committed(Set.of(PARTITION))).isEmpty();
    }

    @Test
    void pollOnce_oneRecord_handleFailed() {
        MockConsumer<Long, String> kafkaConsumer = kafkaConsumer();
        DataHandler handler = mock(DataHandler.class);
        MetricsService metricsService = mock(MetricsService.class);
        NativeConsumer consumer = consumer(kafkaConsumer, handler, metricsService);
        addRecord(kafkaConsumer, 0, 0L, 7L, "payload");
        doThrow(new RuntimeException("Simulated error")).when(handler).handle(anyList());

        assertThatThrownBy(() -> consumer.pollOnce(Duration.ZERO))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Simulated error");

        verify(metricsService).reportIncomingMessages(1);
        verify(metricsService).reportConsumerLagSeconds(org.mockito.Mockito.eq(TOPIC), anyLong());
        assertThat(kafkaConsumer.committed(Set.of(PARTITION))).isEmpty();
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
        verify(metricsService).reportConsumerLagSeconds(org.mockito.Mockito.eq(TOPIC), anyLong());
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

        ArgumentCaptor<List<RequestMessage<String>>> captor = ArgumentCaptor.forClass(List.class);
        verify(handler).handle(captor.capture());
        assertThat(captor.getValue())
                .extracting(RequestMessage::payload)
                .containsExactlyInAnyOrder("first", "second", "third");
        verify(metricsService).reportIncomingMessages(3);
        verify(metricsService).reportConsumerLagSeconds(org.mockito.Mockito.eq(TOPIC), anyLong());
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
        verify(handler, never()).handle(anyList());
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
        verify(handler, never()).handle(anyList());
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
        properties.setProperties(Map.of(ConsumerConfig.CLIENT_ID_CONFIG, "sync-test@localhost"));
        return properties;
    }

    private static ConsumerFactory consumerFactory(MockConsumer<Long, String> kafkaConsumer) {
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
