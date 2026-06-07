package rgo.nativekafka.common.api;

public record RequestMessage<T>(
        Long key,
        T payload,
        Metadata metadata
) {
}
