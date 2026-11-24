package com.sunkafka;

import static com.sunkafka.support.OrderEvents.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.sunkafka.consumer.OrderConsumer;
import com.sunkafka.producer.OrderProducer;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Same round trip against a REAL Kafka broker and a REAL Schema Registry.
 * Skipped automatically when Docker is unavailable (CI fails if that happens).
 */
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class KafkaContainerTest {

    static final Network NETWORK = Network.newNetwork();

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
            .withNetwork(NETWORK)
            .withNetworkAliases("kafka");

    @Container
    static final GenericContainer<?> SCHEMA_REGISTRY =
            new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.6.1"))
                    .withNetwork(NETWORK)
                    .withExposedPorts(8081)
                    .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                    .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
                    .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
                    .dependsOn(KAFKA);

    static String registryUrl() {
        return "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081);
    }

    @DynamicPropertySource
    static void kafkaProps(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.properties.schema.registry.url", KafkaContainerTest::registryUrl);
    }

    @Autowired OrderProducer producer;
    @Autowired OrderConsumer consumer;

    @Test
    void avroRoundTripAgainstRealBrokerAndRegistry() throws Exception {
        var event = valid(uniqueId());

        producer.send(event).join();

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(consumer.getReceived()).contains(event));
        var registry = new CachedSchemaRegistryClient(registryUrl(), 10);
        assertThat(registry.getAllSubjects()).contains("orders-value");
    }
}
