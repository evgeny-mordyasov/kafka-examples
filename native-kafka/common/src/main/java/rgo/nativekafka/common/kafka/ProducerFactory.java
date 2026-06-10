package rgo.nativekafka.common.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.HashMap;
import java.util.Map;

public interface ProducerFactory {

    <K, V> Producer<K, V> create(Map<String, Object> properties);

    static ProducerFactory getInstance(Map<String, Object> commonProperties) {
        return new ProducerFactory() {
            @Override
            public <K, V> Producer<K, V> create(Map<String, Object> specifiedProperties) {
                Map<String, Object> map = new HashMap<>(commonProperties);
                map.putAll(specifiedProperties);
                return new KafkaProducer<>(map);
            }
        };
    }
}
