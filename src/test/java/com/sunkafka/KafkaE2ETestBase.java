package com.sunkafka;

import com.sunkafka.consumer.OrderConsumer;
import com.sunkafka.producer.OrderProducer;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

/** Shared setup: real Spring context talking to an in-JVM Kafka broker (no Docker needed). */
@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
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
