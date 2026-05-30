package rgo.nativekafka.consumer.api;

public record RequestMessage<T>(
        String key,
        T payload,
        Metadata metadata
) {
}
