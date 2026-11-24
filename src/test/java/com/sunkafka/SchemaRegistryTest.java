package com.sunkafka;

import static com.sunkafka.support.OrderEvents.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.sunkafka.model.OrderEvent;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/** Avro + Schema Registry behaviour: registration, defaults and compatibility rules. */
class SchemaRegistryTest extends KafkaE2ETestBase {

    private static final String SUBJECT = "orders-value";
    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    private static String schemaWith(String extraField) {
        return """
                {"type":"record","name":"OrderEvent","namespace":"com.sunkafka.model","fields":[
                  {"name":"orderId","type":"string"},
                  {"name":"customer","type":"string"},
                  {"name":"amount","type":"double"},
                  {"name":"currency","type":"string","default":"USD"}%s]}
                """.formatted(extraField);
    }

    @Test
    void publishingRegistersTheSchemaUnderTheTopicValueSubject() throws Exception {
        var event = valid(uniqueId());

        producer.send(event).join();

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(consumer.getReceived()).contains(event));
        var registry = MockSchemaRegistry.getClientForScope("sun-kafka");
        assertThat(registry.getAllSubjects()).contains(SUBJECT);
        assertThat(registry.getLatestSchemaMetadata(SUBJECT).getSchema()).contains("OrderEvent");
    }

    @Test
    void currencyDefaultsToUsdWhenNotSet() {
        var event = valid(uniqueId());

        producer.send(event).join();

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(consumer.getReceived())
                .anySatisfy(e -> {
                    assertThat(e.getOrderId()).isEqualTo(event.getOrderId());
                    assertThat(e.getCurrency()).isEqualTo("USD");
                }));
    }

    @Test
    void schemaEvolutionRulesAreEnforced() throws Exception {
        producer.send(valid(uniqueId())).join();
        SchemaRegistryClient registry = MockSchemaRegistry.getClientForScope("sun-kafka");

        // Backward compatible: new optional field WITH a default.
        var compatible = new AvroSchema(schemaWith(",{\"name\":\"note\",\"type\":\"string\",\"default\":\"\"}"));
        // Breaking: new required field WITHOUT a default (old data could not be read).
        var breaking = new AvroSchema(schemaWith(",{\"name\":\"note\",\"type\":\"string\"}"));

        assertThat(registry.testCompatibility(SUBJECT, compatible)).isTrue();
        assertThat(registry.testCompatibility(SUBJECT, breaking)).isFalse();
    }

    @Test
    void generatedClassMatchesTheRegisteredSchemaShape() {
        assertThat(OrderEvent.getClassSchema().getFields())
                .extracting(f -> f.name())
                .containsExactly("orderId", "customer", "amount", "currency");
    }
}
