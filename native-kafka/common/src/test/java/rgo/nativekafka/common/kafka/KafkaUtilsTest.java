package rgo.nativekafka.common.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import rgo.nativekafka.common.api.RequestMessage;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KafkaUtilsTest {

    private static final String TOPIC = "request_topic";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

    @Test
    void checkTopic_withExistingTopic_ShouldNotThrowException() {
        Consumer<?, ?> consumer = mock(Consumer.class);
        Duration timeout = Duration.ofMillis(100);
        when(consumer.partitionsFor(TOPIC, timeout))
                .thenReturn(List.of(new PartitionInfo(TOPIC, 0, null, null, null)));

        KafkaUtils.checkTopic(consumer, TOPIC, timeout);
    }

    @Test
    void checkTopic_withMissingTopic_ShouldThrowIllegalStateException() {
        Consumer<?, ?> consumer = mock(Consumer.class);
        Duration timeout = Duration.ofMillis(100);
        when(consumer.partitionsFor(TOPIC, timeout)).thenReturn(List.of());

        assertThatThrownBy(() -> KafkaUtils.checkTopic(consumer, TOPIC, timeout))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Failed to connect to topic " + TOPIC)
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void checkTopic_withConsumerException_ShouldThrowIllegalStateException() {
        Consumer<?, ?> consumer = mock(Consumer.class);
        Duration timeout = Duration.ofMillis(100);
        when(consumer.partitionsFor(TOPIC, timeout)).thenThrow(new RuntimeException("boom"));

        assertThatThrownBy(() -> KafkaUtils.checkTopic(consumer, TOPIC, timeout))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Failed to connect to topic " + TOPIC)
                .hasCauseInstanceOf(RuntimeException.class);
    }

    @Test
    void shortClientId_withClientIdContainingHost_ShouldReturnPrefix() {
        String result = KafkaUtils.shortClientId(Map.of(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-1@localhost"));

        assertThat(result).isEqualTo("consumer-1");
    }

    @Test
    void shortClientId_withClientIdWithoutHost_ShouldReturnWholeValue() {
        String result = KafkaUtils.shortClientId(Map.of(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-1"));

        assertThat(result).isEqualTo("consumer-1");
    }

    @Test
    void shortClientId_withoutClientId_ShouldThrowIllegalArgumentException() {
        assertThatThrownBy(() -> KafkaUtils.shortClientId(Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[client.id] must not be null.");
    }

    @Test
    void lagSec_withEmptyRecords_ShouldReturnZero() {
        long result = KafkaUtils.lagSec(new ConsumerRecords<>(Map.of()));

        assertThat(result).isZero();
    }

    @Test
    void lagSec_withPastRecord_ShouldReturnPositiveValue() {
        ConsumerRecords<Long, String> records = records(record(System.currentTimeMillis() - 2_000));

        long result = KafkaUtils.lagSec(records);

        assertThat(result).isGreaterThanOrEqualTo(1);
    }

    @Test
    void lagSec_withFutureRecord_ShouldReturnZero() {
        ConsumerRecords<Long, String> records = records(record(System.currentTimeMillis() + 60_000));

        long result = KafkaUtils.lagSec(records);

        assertThat(result).isZero();
    }

    @Test
    void rqMessage_withConsumerRecord_ShouldMapPayloadAndMetadata() {
        ConsumerRecord<Long, String> record = record(1_700_000_000_000L);

        RequestMessage<String> result = KafkaUtils.rqMessage(record);

        assertThat(result.key()).isEqualTo(42L);
        assertThat(result.payload()).isEqualTo("payload");
        assertThat(result.metadata().topic()).isEqualTo(TOPIC);
        assertThat(result.metadata().partition()).isZero();
        assertThat(result.metadata().offset()).isEqualTo(7L);
    }

    private static ConsumerRecords<Long, String> records(ConsumerRecord<Long, String> record) {
        return new ConsumerRecords<>(Map.of(PARTITION, List.of(record)));
    }

    private static ConsumerRecord<Long, String> record(long timestamp) {
        return new ConsumerRecord<>(
                TOPIC,
                0,
                7,
                timestamp,
                TimestampType.CREATE_TIME,
                8,
                7,
                42L,
                "payload",
                new RecordHeaders(),
                Optional.empty()
        );
    }
}
