Feature: End-to-end order processing
  HTTP request -> Kafka -> consumer -> linking service

  Background:
    * def Kafka = Java.type('com.sunkafka.karate.KafkaSupport')
    * def Linking = Java.type('com.sunkafka.support.LinkingStub')
    * eval Linking.reset()
    * def newId = function(){ return 'karate-' + java.util.UUID.randomUUID() }
    * url baseUrl

  Scenario: An order posted over HTTP is published to Kafka and linked exactly once
    * def id = newId()
    Given path 'api/orders'
    And request { orderId: '#(id)', customer: 'alice', amount: 42.5 }
    When method post
    Then status 202
    And match response == { orderId: '#(id)' }

    # the event is on the topic, with the schema default applied
    * def events = Kafka.readEvents('orders', id, 15)
    * match events == '#[1]'
    * match events[0] contains { orderId: '#(id)', customer: 'alice', amount: 42.5, currency: 'USD' }

    # the consumer called the linking service once, with an idempotency key for the Kafka record
    * def calls = Kafka.awaitLinkingCalls(id, 1, 15)
    * match calls == '#[1]'
    * match calls[0].body contains { orderId: '#(id)', customer: 'alice', currency: 'USD' }
    * match calls[0].idempotencyKey == '#regex orders-\\d+-\\d+'

  Scenario: The service reports healthy while Kafka and the database are reachable
    Given path 'actuator/health'
    When method get
    Then status 200
    And match response.status == 'UP'
    And match response.components.kafka.status == 'UP'
    And match response.components.db.status == 'UP'

  Scenario Outline: An invalid order (<case>) is rejected with 400 and never reaches Kafka
    * def id = newId()
    Given path 'api/orders'
    And request { orderId: '#(id)', customer: '<customer>', amount: <amount> }
    When method post
    Then status 400
    And match response.title == 'Invalid order'
    And match response.detail contains '<problem>'
    * def events = Kafka.readEvents('orders', id, 2)
    * match events == []

    Examples:
      | case             | customer | amount | problem                          |
      | negative amount  | alice    | -3     | amount must be a positive number |
      | zero amount      | alice    | 0      | amount must be a positive number |
      | blank customer   |          | 10     | customer must not be blank       |
