package rgo.nativekafka.consumer.kafka.utils;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;

import java.time.Duration;
import java.util.List;

public final class KafkaUtils {

    private KafkaUtils() {
    }

    public static void checkConnect(Consumer<?, ?> consumer, String topic, Duration timeout) {
        try {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic, timeout);
            if (partitionInfos.isEmpty()) {
                throw new IllegalStateException("Topic " + topic + " does not exist");
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to connect to topic " + topic, e);
        }
    }

    public static String shortClientId(String clientId) {
        int index = clientId.indexOf('@');
        return index == -1 ? clientId : clientId.substring(0, index);
    }
}
