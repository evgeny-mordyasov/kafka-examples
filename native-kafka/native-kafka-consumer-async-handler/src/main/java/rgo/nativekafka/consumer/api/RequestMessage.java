package rgo.nativekafka.consumer.api;

public record RequestMessage<T>(
        Long key,
        T payload,
        Metadata metadata
) {
}
