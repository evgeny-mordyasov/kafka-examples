package rgo.nativekafka.producer.service;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import rgo.nativekafka.common.kafka.ProducerFactory;
import rgo.nativekafka.producer.properties.KafkaProducerProperties;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SuppressWarnings({"resource", "unchecked", "ConstantConditions"})
class NativeProducerTest {

    private static final String TOPIC = "request_topic";

    @Test
    void createInstance_withNullProducerFactory_ShouldThrowIllegalArgumentException() {
        ProducerFactory producerFactory = null;
        KafkaProducerProperties properties = properties();

        assertThatThrownBy(() -> new NativeProducer(producerFactory, properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[producerFactory] must not be null.");
    }

    @Test
    void createInstance_withNullProperties_ShouldThrowIllegalArgumentException() {
        ProducerFactory producerFactory = producerFactory(producer());
        KafkaProducerProperties properties = null;

        assertThatThrownBy(() -> new NativeProducer(producerFactory, properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[properties] must not be null.");
    }

    @Test
    void createInstance_withEmptyTopic_ShouldThrowIllegalArgumentException() {
        ProducerFactory producerFactory = producerFactory(producer());
        KafkaProducerProperties properties = properties();
        properties.setTopic("");

        assertThatThrownBy(() -> new NativeProducer(producerFactory, properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[topic] must not be null or empty.");
    }

    @Test
    void createInstance_withNonPositiveDelayMs_ShouldThrowIllegalArgumentException() {
        ProducerFactory producerFactory = producerFactory(producer());
        KafkaProducerProperties properties = properties();
        properties.setDelayMs(0);

        assertThatThrownBy(() -> new NativeProducer(producerFactory, properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[delayMs] must be a positive number");
    }

    @Test
    void createInstance_withEmptyProperties_ShouldThrowIllegalArgumentException() {
        ProducerFactory producerFactory = producerFactory(producer());
        KafkaProducerProperties properties = properties();
        properties.setProperties(Map.of());

        assertThatThrownBy(() -> new NativeProducer(producerFactory, properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("[properties] must not be null or empty.");
    }

    @Test
    void createInstance_withValidDependencies_ShouldCreateProducer() {
        Producer<Long, String> kafkaProducer = producer();
        ProducerFactory producerFactory = producerFactory(kafkaProducer);
        KafkaProducerProperties properties = properties();

        NativeProducer producer = new NativeProducer(producerFactory, properties);

        assertThat(producer).isNotNull();
    }

    @Test
    void pushOnce_withValidProperties_ShouldSendRecordToConfiguredTopic() {
        Producer<Long, String> kafkaProducer = producer();
        NativeProducer producer = producer(kafkaProducer);

        producer.pushOnce();

        ArgumentCaptor<ProducerRecord<Long, String>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaProducer).send(recordCaptor.capture(), any(Callback.class));
        assertThat(recordCaptor.getValue().topic()).isEqualTo(TOPIC);
        assertThat(recordCaptor.getValue().key()).isNotNull();
        assertThat(recordCaptor.getValue().value()).isNotEmpty();
    }

    @Test
    void run_withCreatedProducer_ShouldScheduleSending() {
        Producer<Long, String> kafkaProducer = producer();
        NativeProducer producer = producer(kafkaProducer);

        producer.run();

        verify(kafkaProducer, timeout(1_000)).send(any(), any(Callback.class));
        producer.close();
    }

    @Test
    void run_calledTwice_ShouldStartOnlyOnce() throws InterruptedException {
        Producer<Long, String> kafkaProducer = producer();
        KafkaProducerProperties properties = properties();
        properties.setDelayMs(5_000);
        NativeProducer producer = new NativeProducer(producerFactory(kafkaProducer), properties);

        producer.run();
        producer.run();
        Thread.sleep(100);

        verify(kafkaProducer, times(1)).send(any(), any(Callback.class));
        producer.close();
    }

    @Test
    void close_withCreatedProducer_ShouldCloseKafkaProducer() {
        Producer<Long, String> kafkaProducer = producer();
        NativeProducer producer = producer(kafkaProducer);

        producer.close();

        verify(kafkaProducer, timeout(1_000)).close();
    }

    @Test
    void close_withStartedProducer_ShouldCloseKafkaProducer() {
        Producer<Long, String> kafkaProducer = producer();
        NativeProducer producer = producer(kafkaProducer);
        producer.run();

        producer.close();

        verify(kafkaProducer, timeout(1_000)).close();
    }

    @Test
    void close_calledTwice_ShouldCloseKafkaProducerOnce() {
        Producer<Long, String> kafkaProducer = producer();
        NativeProducer producer = producer(kafkaProducer);

        producer.close();
        producer.close();

        verify(kafkaProducer, timeout(1_000).times(1)).close();
    }

    private static NativeProducer producer(Producer<Long, String> kafkaProducer) {
        return new NativeProducer(producerFactory(kafkaProducer), properties());
    }

    private static Producer<Long, String> producer() {
        return mock(Producer.class);
    }

    private static ProducerFactory producerFactory(Producer<Long, String> kafkaProducer) {
        return new ProducerFactory() {
            @Override
            @SuppressWarnings("unchecked")
            public <K, V> Producer<K, V> create(Map<String, Object> properties) {
                return (Producer<K, V>) kafkaProducer;
            }
        };
    }

    private static KafkaProducerProperties properties() {
        KafkaProducerProperties properties = new KafkaProducerProperties();
        properties.setTopic(TOPIC);
        properties.setDelayMs(10);
        properties.setProperties(Map.of(
                ProducerConfig.CLIENT_ID_CONFIG, "producer-test@localhost",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        ));
        return properties;
    }
}
