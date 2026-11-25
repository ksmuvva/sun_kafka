Feature: Publishing and consuming orders
  As a downstream service
  I want every order published to Kafka to be delivered to me
  So that I can process it reliably and in order

  @smoke
  Scenario: A published order is delivered to the consumer
    Given a new order for customer "alice" with amount 42.5
    When the order is published
    Then the order is consumed within 15 seconds

  Scenario: Updates to the same order stay in order on one partition
    Given 10 successive updates to one order
    When the updates are published one after another
    Then all updates are consumed within 15 seconds
    And they arrive in the order they were sent
    And they all came from a single partition

  Scenario: Different orders are spread across partitions
    Given 30 orders from different customers
    When the orders are published
    Then all orders are consumed within 15 seconds
    And more than one partition was used
