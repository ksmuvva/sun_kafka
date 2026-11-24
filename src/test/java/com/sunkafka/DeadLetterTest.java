package com.sunkafka;

import static com.sunkafka.support.OrderEvents.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.sunkafka.support.TopicReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

/** Failure handling: retries, dead-letter topic, poison pills. */
class DeadLetterTest extends KafkaE2ETestBase {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    @Test
    void invalidEventIsRetriedThenDeadLettered() {
        var id = uniqueId();
        try (var dlt = new TopicReader(brokers, "orders.DLT")) {
            producer.send(withAmount(id, -5)).join();

            var records = dlt.await(id, 1, TIMEOUT);

            assertThat(records).hasSize(1);
            assertThat(records.get(0).value()).contains(id);
            assertThat(records.get(0).headers().lastHeader("kafka_dlt-exception-fqcn")).isNotNull();
            // 1 initial attempt + 2 retries
            assertThat(consumer.attemptsFor(id)).isEqualTo(3);
            assertThat(consumer.getReceived()).noneMatch(e -> e.getOrderId().equals(id));
        }
    }

    @Test
    void poisonPillIsDeadLetteredWithoutBlockingTheConsumer() throws Exception {
        var poisonKey = uniqueId();
        var goodId = uniqueId();
        try (var dlt = new TopicReader(brokers, "orders.DLT");
             var raw = new KafkaProducer<String, String>(Map.<String, Object>of(
                     ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers),
                     new StringSerializer(), new StringSerializer())) {

            raw.send(new ProducerRecord<>("orders", poisonKey, "{this is not json")).get();
            producer.send(valid(goodId)).join();

            var records = dlt.await("{this is not json", 1, TIMEOUT);
            assertThat(records).hasSize(1);
            assertThat(new String(records.get(0).value().getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8))
                    .contains("{this is not json");
            // consumer is not stuck behind the bad record
            await().atMost(TIMEOUT).untilAsserted(() ->
                    assertThat(consumer.getReceived()).anyMatch(e -> e.getOrderId().equals(goodId)));
        }
    }
}
