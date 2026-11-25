package com.sunkafka.bdd;

import static com.sunkafka.support.OrderEvents.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.sunkafka.consumer.OrderConsumer;
import com.sunkafka.consumer.OrderConsumer.Delivery;
import com.sunkafka.model.OrderEvent;
import com.sunkafka.producer.OrderProducer;
import io.cucumber.java.Before;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.springframework.beans.factory.annotation.Autowired;

public class OrderMessagingSteps {

    @Autowired OrderProducer producer;
    @Autowired OrderConsumer consumer;

    private final List<OrderEvent> sent = new ArrayList<>();

    @Before
    public void resetConsumer() {
        consumer.reset();
        sent.clear();
    }

    @Given("a new order for customer {string} with amount {double}")
    public void aNewOrder(String customer, double amount) {
        sent.add(OrderEvent.newBuilder()
                .setOrderId(uniqueId()).setCustomer(customer).setAmount(amount).build());
    }

    @Given("{int} successive updates to one order")
    public void successiveUpdates(int count) {
        var id = uniqueId();
        IntStream.rangeClosed(1, count).forEach(i -> sent.add(withAmount(id, i)));
    }

    @Given("{int} orders from different customers")
    public void ordersFromDifferentCustomers(int count) {
        IntStream.range(0, count).forEach(i -> sent.add(valid(uniqueId())));
    }

    @When("the order is published")
    public void theOrderIsPublished() {
        publishAll();
    }

    @When("the orders are published")
    public void theOrdersArePublished() {
        publishAll();
    }

    @When("the updates are published one after another")
    public void theUpdatesArePublished() {
        publishAll();
    }

    private void publishAll() {
        sent.forEach(e -> producer.send(e).join());
    }

    @Then("the order is consumed within {int} seconds")
    public void theOrderIsConsumed(int seconds) {
        awaitAllConsumed(seconds);
    }

    @Then("all updates are consumed within {int} seconds")
    public void allUpdatesConsumed(int seconds) {
        awaitAllConsumed(seconds);
    }

    @Then("all orders are consumed within {int} seconds")
    public void allOrdersConsumed(int seconds) {
        awaitAllConsumed(seconds);
    }

    private void awaitAllConsumed(int seconds) {
        await().atMost(Duration.ofSeconds(seconds))
                .untilAsserted(() -> assertThat(consumer.getReceived()).containsAll(sent));
    }

    @And("they arrive in the order they were sent")
    public void theyArriveInOrder() {
        assertThat(consumer.getDeliveries().stream().map(Delivery::event)).containsExactlyElementsOf(sent);
    }

    @And("they all came from a single partition")
    public void singlePartition() {
        assertThat(partitionsUsed()).hasSize(1);
    }

    @And("more than one partition was used")
    public void moreThanOnePartition() {
        assertThat(partitionsUsed()).hasSizeGreaterThan(1);
    }

    private Set<Integer> partitionsUsed() {
        return consumer.getDeliveries().stream().map(Delivery::partition).collect(Collectors.toSet());
    }
}
