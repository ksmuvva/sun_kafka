package com.sunkafka;

import com.sunkafka.consumer.OrderConsumer;
import com.sunkafka.producer.OrderProducer;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

/** Shared setup: real Spring context, in-JVM Kafka broker and in-memory Schema Registry (no Docker needed). */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        // in-memory Schema Registry shared by name with MockSchemaRegistry.getClientForScope("sun-kafka")
        "spring.kafka.properties.schema.registry.url=mock://sun-kafka"})
@EmbeddedKafka(partitions = 3, topics = {"orders", "orders.DLT"})
abstract class KafkaE2ETestBase {

    @Autowired protected OrderProducer producer;
    @Autowired protected OrderConsumer consumer;
    @Value("${spring.embedded.kafka.brokers}") protected String brokers;

    @BeforeEach
    void resetConsumer() {
        consumer.reset();
    }
}
