package rgo.nativekafka.consumer.kafka.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import rgo.nativekafka.consumer.api.RequestMessage;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.stream.Collectors;

public final class StringifyUtils {

    private StringifyUtils() {
    }

    public static String briefRecord(ConsumerRecord<String, String> record) {
        var timeType = record.timestampType() == null ? "time" : record.timestampType().name;
        var recTime = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss.SSS").withZone(ZoneId.systemDefault()).format(Instant.ofEpochMilli(record.timestamp()));
        return "{key=" + record.key() +
                ",p=" + record.partition() +
                ",ofs=" + record.offset() +
                ",time=" + recTime + '(' + timeType + ')' +
                ",msg=" + record.value() +
                ",trace=" + record.topic() + "_" + record.partition() + "_" + record.offset() +
                '}';
    }

    public static String briefRecordWithoutValue(ConsumerRecord<String, String> record) {
        var timeType = record.timestampType() == null ? "time" : record.timestampType().name;
        var recTime = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss.SSS").withZone(ZoneId.systemDefault()).format(Instant.ofEpochMilli(record.timestamp()));
        return "{key=" + record.key() +
                ",p=" + record.partition() +
                ",ofs=" + record.offset() +
                ",time=" + recTime + '(' + timeType + ')' +
                ",msg={length=" + (record.value() != null ? record.value().length() : null) + "}" +
                ",trace=" + record.topic() + "_" + record.partition() + "_" + record.offset() +
                '}';
    }

    public static String briefMessage(RequestMessage<String> msg) {
        var recTime = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss.SSS").withZone(ZoneId.systemDefault()).format(msg.metadata().dateTime());
        return "{key=" + msg.key() +
                ",p=" + msg.metadata().partition() +
                ",ofs=" + msg.metadata().offset() +
                ",time=" + recTime + "(unknown)" +
                ",msg=" + msg.payload() +
                ",trace=" + msg.metadata().topic() + "_" + msg.metadata().partition() + "_" + msg.metadata().offset() +
                '}';
    }

    public static String briefMessageWithoutPayload(RequestMessage<?> msg) {
        var recTime = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss.SSS").withZone(ZoneId.systemDefault()).format(msg.metadata().dateTime());
        return "{key=" + msg.key() +
                ",p=" + msg.metadata().partition() +
                ",ofs=" + msg.metadata().offset() +
                ",time=" + recTime + "(unknown)" +
                ",msg={unknown}" +
                ",trace=" + msg.metadata().topic() + "_" + msg.metadata().partition() + "_" + msg.metadata().offset() +
                '}';
    }

    public static String briefPartitions(Collection<TopicPartition> partitions) {
        return partitions.stream()
                .map(each -> String.valueOf(each.partition()))
                .collect(Collectors.joining(",", "[", "]"));
    }
}