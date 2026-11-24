Feature: Schema evolution with Schema Registry
  Producers and consumers can evolve independently only if schema changes are compatible

  Scenario: Publishing registers the schema under the topic's value subject
    Given an order is published
    Then the schema registry has the subject "orders-value"

  Scenario: A missing currency defaults to USD
    Given an order that does not set a currency
    When it is published and consumed
    Then the consumed order has currency "USD"

  Scenario Outline: The registry enforces compatibility rules
    Given the orders schema is registered
    When a new version adds a field "note" <default_clause> a default
    Then the registry <verdict> the change

    Examples:
      | default_clause | verdict |
      | with           | accepts |
      | without        | rejects |
