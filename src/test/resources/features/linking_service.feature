Feature: Linking service integration
  Every valid order read from Kafka must be linked through the EU linking service, exactly once

  @smoke
  Scenario: A valid order is sent to the linking service exactly once
    Given a valid order
    When the order is submitted to Kafka
    Then the order is linked within 15 seconds
    And the linking service received 1 call for that order
    And the call carried an idempotency key for the Kafka record

  Scenario: A temporary linking failure is retried and then succeeds
    Given a valid order
    And the linking service fails 1 time with status 503 for that order
    When the order is submitted to Kafka
    Then the order is linked within 15 seconds
    And the linking service received 2 calls for that order

  Scenario: A linking service that keeps failing sends the order to the dead-letter topic after retries
    Given a valid order
    And the linking service always answers 503 for that order
    When the order is submitted to Kafka
    Then the order lands on the dead-letter topic within 20 seconds
    And the linking service received 3 calls for that order
    And the listener tried the order 3 times

  Scenario: A rejected order is dead-lettered without retries
    Given a valid order
    And the linking service always answers 400 for that order
    When the order is submitted to Kafka
    Then the order lands on the dead-letter topic within 20 seconds
    And the linking service received 1 call for that order
    And the listener tried the order 1 time

  Scenario: A redelivered Kafka record is not linked twice
    Given a valid order
    When the same Kafka record is delivered twice
    Then the linking service received 1 call for that order
