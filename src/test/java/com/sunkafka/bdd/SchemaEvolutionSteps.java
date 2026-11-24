package com.sunkafka.bdd;

import static com.sunkafka.support.OrderEvents.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.sunkafka.consumer.OrderConsumer;
import com.sunkafka.model.OrderEvent;
import com.sunkafka.producer.OrderProducer;
import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Autowired;

public class SchemaEvolutionSteps {

    private static final String SUBJECT = "orders-value";

    @Autowired OrderProducer producer;
    @Autowired OrderConsumer consumer;

    private final SchemaRegistryClient registry = MockSchemaRegistry.getClientForScope("sun-kafka");
    private OrderEvent event;
    private boolean compatible;

    @Before
    public void resetConsumer() {
        consumer.reset();
    }

    @Given("an order is published")
    public void anOrderIsPublished() {
        producer.send(valid(uniqueId())).join();
    }

    @Then("the schema registry has the subject {string}")
    public void registryHasSubject(String subject) throws Exception {
        assertThat(registry.getAllSubjects()).contains(subject);
    }

    @Given("an order that does not set a currency")
    public void orderWithoutCurrency() {
        event = valid(uniqueId()); // currency not set -> schema default applies
    }

    @When("it is published and consumed")
    public void publishedAndConsumed() {
        producer.send(event).join();
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(consumer.getReceived()).contains(event));
    }

    @Then("the consumed order has currency {string}")
    public void consumedCurrency(String currency) {
        var consumed = consumer.getReceived().stream()
                .filter(e -> e.getOrderId().equals(event.getOrderId())).findFirst().orElseThrow();
        assertThat(consumed.getCurrency()).isEqualTo(currency);
    }

    @Given("the orders schema is registered")
    public void ordersSchemaRegistered() {
        producer.send(valid(uniqueId())).join();
    }

    @When("a new version adds a field {string} {word} a default")
    public void newVersionAddsField(String field, String withOrWithout) throws Exception {
        String defaultPart = withOrWithout.equals("with") ? ",\"default\":\"\"" : "";
        var schema = new AvroSchema("""
                {"type":"record","name":"OrderEvent","namespace":"com.sunkafka.model","fields":[
                  {"name":"orderId","type":"string"},
                  {"name":"customer","type":"string"},
                  {"name":"amount","type":"double"},
                  {"name":"currency","type":"string","default":"USD"},
                  {"name":"%s","type":"string"%s}]}
                """.formatted(field, defaultPart));
        compatible = registry.testCompatibility(SUBJECT, schema);
    }

    @Then("the registry {word} the change")
    public void registryVerdict(String verdict) {
        assertThat(compatible).isEqualTo(verdict.equals("accepts"));
    }
}
