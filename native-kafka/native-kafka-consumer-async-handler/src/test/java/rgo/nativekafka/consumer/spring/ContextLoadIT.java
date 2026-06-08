package rgo.nativekafka.consumer.spring;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

@SpringBootTest(
        classes = Main.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@EmbeddedKafka(
        partitions = 3,
        topics = "string-values",
        bootstrapServersProperty = "kafka-consumer.properties[bootstrap.servers]"
)
class ContextLoadIT {

    @Test
    void contextLoads() {
    }
}
