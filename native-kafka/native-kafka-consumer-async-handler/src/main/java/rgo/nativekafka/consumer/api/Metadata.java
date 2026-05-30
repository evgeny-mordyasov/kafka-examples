package rgo.nativekafka.consumer.api;

import java.time.LocalDateTime;

public record Metadata(
        String topic,
        int partition,
        long offset,
        LocalDateTime dateTime
) {
}
