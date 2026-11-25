package com.sunkafka.bdd;

import static com.sunkafka.support.OrderEvents.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.sunkafka.consumer.OrderConsumer;
import com.sunkafka.model.OrderEvent;
import com.sunkafka.producer.OrderProducer;
import com.sunkafka.support.LinkingStub;
import com.sunkafka.support.TopicReader;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

public class LinkingServiceSteps {

    @Autowired OrderProducer producer;
    @Autowired OrderConsumer consumer;
    @Autowired EmbeddedKafkaBroker embeddedKafka;

    private OrderEvent order;
    private TopicReader deadLetters;

    @Before
    public void setUp() {
        consumer.reset();
        deadLetters = new TopicReader(embeddedKafka.getBrokersAsString(), "orders.DLT");
    }

    @After
    public void tearDown() {
        deadLetters.close();
    }

    @Given("a valid order")
    public void aValidOrder() {
        order = valid(uniqueId());
    }

    @And("the linking service fails {int} time(s) with status {int} for that order")
    public void linkingFailsThenRecovers(int failures, int status) {
        LinkingStub.failThenSucceed(order.getOrderId(), status, failures);
    }

    @And("the linking service always answers {int} for that order")
    public void linkingAlwaysAnswers(int status) {
        LinkingStub.failWith(order.getOrderId(), status);
    }

    @When("the order is submitted to Kafka")
    public void submit() {
        producer.send(order).join();
    }

    @When("the same Kafka record is delivered twice")
    public void deliveredTwice() {
        long offset = 9_000_000L + (long) (Math.random() * 1_000_000);
        consumer.onMessage(order, "orders", 0, offset);
        consumer.onMessage(order, "orders", 0, offset);
    }

    @Then("the order is linked within {int} seconds")
    public void linked(int seconds) {
        await().atMost(Duration.ofSeconds(seconds))
                .untilAsserted(() -> assertThat(consumer.getReceived()).contains(order));
    }

    @Then("the order lands on the dead-letter topic within {int} seconds")
    public void deadLettered(int seconds) {
        assertThat(deadLetters.await(order.getOrderId(), 1, Duration.ofSeconds(seconds))).hasSize(1);
    }

    @And("the linking service received {int} call(s) for that order")
    public void callsReceived(int calls) {
        assertThat(LinkingStub.requestsFor(order.getOrderId())).hasSize(calls);
    }

    @And("the call carried an idempotency key for the Kafka record")
    public void idempotencyKey() {
        assertThat(LinkingStub.requestsFor(order.getOrderId()).get(0).getHeader("Idempotency-Key"))
                .matches("orders-\\d+-\\d+");
    }

    @And("the listener tried the order {int} time(s)")
    public void attempts(int attempts) {
        assertThat(consumer.attemptsFor(order.getOrderId())).isEqualTo(attempts);
    }
}
