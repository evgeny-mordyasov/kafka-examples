package rgo.nativekafka.common.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TestConsumer {

    private final MockConsumer<Long, String> consumer;
    private final String topic;
    private final Set<TopicPartition> partitions;
    private final Map<TopicPartition, OffsetAndMetadata> endOffsets;

    public TestConsumer(String topic) {
        this.topic = topic;
        this.consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        TopicPartition p1 = new TopicPartition(topic, 0);
        TopicPartition p2 = new TopicPartition(topic, 1);
        TopicPartition p3 = new TopicPartition(topic, 2);
        partitions = Set.of(p1, p2, p3);

        Map<TopicPartition, Long> beginningOffsets = partitions.stream()
                .collect(Collectors.toMap(Function.identity(), ignored -> 0L));
        Map<TopicPartition, Long> endOffsetsLong = Map.of(p1, 3L, p2, 2L, p3, 1L);
        endOffsets = endOffsetsLong.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue())));

        consumer.updateBeginningOffsets(beginningOffsets);
        consumer.updateEndOffsets(endOffsetsLong);
        consumer.updatePartitions(
                topic,
                List.of(
                        new PartitionInfo(topic, 0, null, null, null),
                        new PartitionInfo(topic, 1, null, null, null),
                        new PartitionInfo(topic, 2, null, null, null)
                ));
    }

    public MockConsumer<Long, String> getConsumer() {
        return consumer;
    }

    public Set<TopicPartition> getPartitions() {
        return partitions;
    }

    public Map<TopicPartition, OffsetAndMetadata> getEndOffsets() {
        return endOffsets;
    }

    public void assign() {
        consumer.assign(partitions);
    }

    public void scheduleRebalance() {
        consumer.schedulePollTask(() -> consumer.rebalance(partitions));
    }

    public void addRecord(int partition, long offset, Long key, String value) {
        consumer.addRecord(new ConsumerRecord<>(
                topic,
                partition,
                offset,
                key,
                value
        ));
    }
}
