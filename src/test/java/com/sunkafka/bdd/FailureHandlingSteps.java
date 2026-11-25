package com.sunkafka.bdd;

import static com.sunkafka.support.OrderEvents.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.sunkafka.consumer.OrderConsumer;
import com.sunkafka.model.OrderEvent;
import com.sunkafka.producer.OrderProducer;
import com.sunkafka.support.TopicReader;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

public class FailureHandlingSteps {

    private static final String MALFORMED = "this-is-not-avro";

    @Autowired OrderProducer producer;
    @Autowired OrderConsumer consumer;
    // this context's own broker (the system property is JVM-wide and may belong to another Spring context)
    @Autowired EmbeddedKafkaBroker embeddedKafka;
    private String brokers;

    private TopicReader deadLetters;
    private OrderEvent invalidOrder;
    private OrderEvent goodOrder;

    @Before
    public void setUp() {
        brokers = embeddedKafka.getBrokersAsString();
        consumer.reset();
        deadLetters = new TopicReader(brokers, "orders.DLT");
    }

    @After
    public void tearDown() {
        deadLetters.close();
    }

    @Given("an order with a non-positive amount of {double}")
    public void invalidOrder(double amount) {
        invalidOrder = withAmount(uniqueId(), amount);
    }

    @When("the invalid order is published")
    public void publishInvalid() {
        producer.send(invalidOrder).join();
    }

    @Then("it lands on the dead-letter topic within {int} seconds")
    public void invalidOrderDeadLettered(int seconds) {
        var records = deadLetters.await(invalidOrder.getOrderId(), 1, Duration.ofSeconds(seconds));
        assertThat(records).hasSize(1);
        assertThat(records.get(0).headers().lastHeader("kafka_dlt-exception-fqcn")).isNotNull();
    }

    @And("the listener tried it {int} time(s)")
    public void listenerAttempts(int attempts) {
        // validation failures are never retried
        assertThat(consumer.attemptsFor(invalidOrder.getOrderId())).isEqualTo(attempts);
    }

    @And("it was never consumed successfully")
    public void neverConsumed() {
        assertThat(consumer.getReceived()).noneMatch(e -> e.getOrderId().equals(invalidOrder.getOrderId()));
    }

    @Given("a malformed message that is not valid Avro")
    public void malformedMessage() {
        goodOrder = valid(uniqueId());
    }

    @When("it is published, followed by a valid order")
    public void publishMalformedThenValid() throws Exception {
        try (var raw = new KafkaProducer<String, String>(
                Map.<String, Object>of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers),
                new StringSerializer(), new StringSerializer())) {
            raw.send(new ProducerRecord<>("orders", uniqueId(), MALFORMED)).get();
        }
        producer.send(goodOrder).join();
    }

    @Then("the malformed message lands on the dead-letter topic within {int} seconds")
    public void malformedDeadLettered(int seconds) {
        assertThat(deadLetters.await(MALFORMED, 1, Duration.ofSeconds(seconds))).hasSize(1);
    }

    @And("the valid order is still consumed")
    public void validOrderConsumed() {
        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(consumer.getReceived()).contains(goodOrder));
    }
}
