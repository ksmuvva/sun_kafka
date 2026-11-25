package com.sunkafka;

import com.sunkafka.consumer.OrderConsumer;
import com.sunkafka.persistence.ProcessedMessageStore;
import com.sunkafka.producer.OrderProducer;
import com.sunkafka.support.LinkingStub;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Shared setup: real Spring context, in-JVM Kafka broker, in-memory Schema Registry, in-memory database and a
 * fake linking service (no Docker needed).
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        // in-memory Schema Registry shared by name with MockSchemaRegistry.getClientForScope("sun-kafka")
        "spring.kafka.properties.schema.registry.url=mock://sun-kafka",
        // short timeouts so "linking service is slow" tests stay fast
        "linking.connect-timeout=500ms",
        "linking.read-timeout=500ms"})
@EmbeddedKafka(partitions = 3, topics = {"orders", "orders.DLT"})
@ActiveProfiles("test")
abstract class KafkaE2ETestBase {

    @Autowired protected OrderProducer producer;
    @Autowired protected OrderConsumer consumer;
    @Autowired protected ProcessedMessageStore store;
    // Ask this context's own broker: the "spring.embedded.kafka.brokers" system property is JVM-wide, so when
    // several Spring contexts exist the latest one overwrites it and tests would talk to the wrong broker.
    @Autowired private EmbeddedKafkaBroker embeddedKafka;
    protected String brokers;

    @DynamicPropertySource
    static void linkingService(DynamicPropertyRegistry registry) {
        registry.add("linking.base-url", LinkingStub::baseUrl);
    }

    @BeforeEach
    void resetState() {
        brokers = embeddedKafka.getBrokersAsString();
        consumer.reset();
        LinkingStub.reset();
    }
}
