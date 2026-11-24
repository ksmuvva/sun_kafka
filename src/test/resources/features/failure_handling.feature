Feature: Failure handling
  Bad messages must never block good ones, and must never disappear silently

  Scenario: An invalid order is retried and then dead-lettered
    Given an order with a non-positive amount of -5
    When the invalid order is published
    Then it lands on the dead-letter topic within 20 seconds
    And the listener tried it 3 times
    And it was never consumed successfully

  Scenario: A malformed message is dead-lettered without blocking later orders
    Given a malformed message that is not valid Avro
    When it is published, followed by a valid order
    Then the malformed message lands on the dead-letter topic within 20 seconds
    And the valid order is still consumed
