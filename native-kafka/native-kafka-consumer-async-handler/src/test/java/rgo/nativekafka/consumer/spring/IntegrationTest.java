package rgo.nativekafka.consumer.spring;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.web.servlet.client.RestTestClient;

@SpringBootTest(
        classes = Main.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@EmbeddedKafka(
        partitions = 3,
        topics = "string-values",
        bootstrapServersProperty = "kafka-consumer.properties[bootstrap.servers]"
)
abstract class IntegrationTest {

    @LocalServerPort int port;
    protected RestTestClient client;

    @BeforeEach
    void setup() {
        client = RestTestClient
                .bindToServer()
                .baseUrl("http://localhost:" + port)
                .build();
    }
}
