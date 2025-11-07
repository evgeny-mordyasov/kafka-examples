package rgo.nativekafka.consumer.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.HashMap;
import java.util.Map;

public interface ConsumerFactory {

    <K, V> Consumer<K, V> create(Map<String, Object> properties);

    static ConsumerFactory getInstance(Map<String, Object> commonProperties) {
        return new ConsumerFactory() {
            @Override
            public <K, V> Consumer<K, V> create(Map<String, Object> specifiedProperties) {
                Map<String, Object> map = new HashMap<>(commonProperties);
                map.putAll(specifiedProperties);
                return new KafkaConsumer<>(map);
            }
        };
    }
}
