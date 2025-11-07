package rgo.nativekafka.consumer.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rgo.nativekafka.consumer.kafka.ConsumerFactory;
import rgo.nativekafka.consumer.kafka.utils.KafkaUtils;
import rgo.nativekafka.consumer.spring.properties.KafkaConsumerProperties;
import rgo.nativekafka.consumer.service.handler.DataHandler;
import rgo.nativekafka.consumer.utils.Asserts;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static rgo.nativekafka.consumer.kafka.utils.KafkaUtils.shortClientId;

public class NativeConsumer implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(NativeConsumer.class);

    private final Consumer<Long, String> consumer;
    private final ExecutorService pollingExecutor;
    private final KafkaConsumerProperties properties;
    private final DataHandler handler;
    private final AtomicReference<State> state = new AtomicReference<>(State.CREATED);

    public NativeConsumer(
            ConsumerFactory consumerFactory,
            KafkaConsumerProperties properties,
            DataHandler handler
    ) {
        this.properties = Asserts.nonNull(properties, "properties");
        Asserts.nonEmpty(properties.getTopic(), "topic");
        Asserts.positive(properties.getCheckTimeoutMillis(), "checkTimeoutMillis");
        Asserts.positive(properties.getPollTimeoutMillis(), "pollTimeoutMillis");
        Asserts.positive(properties.getCloseTimeoutMillis(), "closeTimeoutMillis");
        Asserts.nonEmpty(properties.getProperties(), "properties");
        consumer = Asserts.nonNull(consumerFactory, "consumerFactory").create(properties.getProperties());
        String clientId = Asserts.nonNull(properties.getProperties().get(ConsumerConfig.CLIENT_ID_CONFIG), "clientId").toString();
        pollingExecutor = Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, shortClientId(clientId)));
        this.handler = handler;
    }

    public void checkConnect() {
        KafkaUtils.checkConnect(
                consumer,
                properties.getTopic(),
                Duration.ofMillis(properties.getCheckTimeoutMillis())
        );
    }

    public void start() {
        if (state.compareAndSet(State.CREATED, State.STARTED)) {
            consumer.subscribe(List.of(properties.getTopic()));
            LOGGER.info("Subscribe to the kafka topic: {}", properties.getTopic());
            pollingExecutor.execute(this::loopPolling);
            LOGGER.info("Started polling.");
        } else {
            LOGGER.info("Consumer is already started.");
        }
    }

    private void loopPolling() {
        try {
            Duration timeout = Duration.of(properties.getPollTimeoutMillis(), ChronoUnit.MILLIS);

            while (isRunning()) {
                try {
                    ConsumerRecords<Long, String> records = consumer.poll(timeout);
                    if (records.isEmpty()) {
                        LOGGER.debug("No more records to consume.");
                        continue;
                    }

                    handler.handle(records);
                    consumer.commitSync();
                } catch (WakeupException e) {
                    LOGGER.warn("Wakeup occurred.");
                } catch (Exception e) {
                    LOGGER.error("An unexpected exception, but the polling continued.", e);
                }
            }
        } finally {
            consumer.unsubscribe();
            consumer.close(Duration.ofMillis(properties.getCloseTimeoutMillis()));
            LOGGER.info("Closed consumer.");
        }
    }

    private boolean isRunning() {
        return state.get() == State.STARTED && !Thread.currentThread().isInterrupted();
    }

    @Override
    public void close() {
        if (state.compareAndSet(State.STARTED, State.CLOSED)) {
            consumer.wakeup();
        }
    }

    private enum State {
        CREATED, STARTED, CLOSED
    }
}
