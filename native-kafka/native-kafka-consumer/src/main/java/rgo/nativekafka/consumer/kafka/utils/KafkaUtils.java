package rgo.nativekafka.consumer.kafka.utils;

import jakarta.annotation.Nonnull;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.PartitionInfo;
import rgo.nativekafka.common.Asserts;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public final class KafkaUtils {

    private KafkaUtils() {
    }

    public static void checkTopic(Consumer<?, ?> consumer, String topic, Duration timeout) {
        try {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic, timeout);
            if (partitionInfos.isEmpty()) {
                throw new IllegalStateException("Topic " + topic + " does not exist");
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to connect to topic " + topic, e);
        }
    }

    @Nonnull
    public static String shortClientId(@Nonnull Map<String, Object> props) {
        var clientId = Asserts.nonNull(props.get(ConsumerConfig.CLIENT_ID_CONFIG), "client.id").toString();
        return shortClientId(clientId);
    }

    @Nonnull
    private static String shortClientId(@Nonnull String clientId) {
        int endIndex = clientId.indexOf('@');
        return clientId.substring(0, endIndex == -1 ? clientId.length() : endIndex);
    }
}
