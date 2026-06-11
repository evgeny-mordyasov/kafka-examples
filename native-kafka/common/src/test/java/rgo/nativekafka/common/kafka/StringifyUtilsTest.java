package rgo.nativekafka.common.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import rgo.nativekafka.common.api.Metadata;
import rgo.nativekafka.common.api.RequestMessage;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class StringifyUtilsTest {

    private static final String TOPIC = "request_topic";
    private static final long TIMESTAMP = 1_700_000_000_000L;

    @Test
    void briefRecord_withValidRecord_ShouldContainRecordFieldsAndTrace() {
        String result = StringifyUtils.briefRecord(record("payload"));

        assertThat(result)
                .contains("key=42")
                .contains("p=1")
                .contains("ofs=7")
                .contains("msg=payload")
                .contains("trace=request_topic_1_7");
    }

    @Test
    void briefRecordWithoutValue_withNullValue_ShouldContainNullLength() {
        String result = StringifyUtils.briefRecordWithoutValue(record(null));

        assertThat(result)
                .contains("msg={length=null}")
                .contains("trace=request_topic_1_7");
    }

    @Test
    void briefRecordWithoutValue_withValidValue_ShouldContainValueLength() {
        String result = StringifyUtils.briefRecordWithoutValue(record("payload"));

        assertThat(result).contains("msg={length=7}");
    }

    @Test
    void briefMessage_withValidMessage_ShouldContainMessageFieldsAndTrace() {
        RequestMessage<String> message = message("payload");

        String result = StringifyUtils.briefMessage(message);

        assertThat(result)
                .contains("key=42")
                .contains("p=1")
                .contains("ofs=7")
                .contains("time=")
                .contains("(unknown)")
                .contains("msg=payload")
                .contains("trace=request_topic_1_7");
    }

    @Test
    void briefMessageWithoutPayload_withNullPayload_ShouldHidePayload() {
        RequestMessage<String> message = message(null);

        String result = StringifyUtils.briefMessageWithoutPayload(message);

        assertThat(result)
                .contains("msg={unknown}")
                .contains("trace=request_topic_1_7");
    }

    @Test
    void briefPartitions_withManyPartitions_ShouldReturnPartitionNumbers() {
        String result = StringifyUtils.briefPartitions(List.of(
                new TopicPartition(TOPIC, 2),
                new TopicPartition(TOPIC, 0),
                new TopicPartition(TOPIC, 1)
        ));

        assertThat(result).isEqualTo("[2,0,1]");
    }

    private static ConsumerRecord<Long, String> record(String value) {
        return new ConsumerRecord<>(
                TOPIC,
                1,
                7,
                TIMESTAMP,
                TimestampType.CREATE_TIME,
                8,
                value == null ? -1 : value.length(),
                42L,
                value,
                new RecordHeaders(),
                Optional.empty()
        );
    }

    private static RequestMessage<String> message(String payload) {
        Metadata metadata = new Metadata(
                TOPIC,
                1,
                7,
                LocalDateTime.ofInstant(Instant.ofEpochMilli(TIMESTAMP), ZoneId.systemDefault())
        );
        return new RequestMessage<>(42L, payload, metadata);
    }
}
