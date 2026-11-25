Feature: Kafka consumer integration
  Karate publishes to Kafka itself (like an upstream producer) and checks what the service does with it

  Background:
    * def Kafka = Java.type('com.sunkafka.karate.KafkaSupport')
    * def Linking = Java.type('com.sunkafka.support.LinkingStub')
    * eval Linking.reset()
    * def newId = function(){ return 'karate-' + java.util.UUID.randomUUID() }

  Scenario: An order published straight to Kafka is linked exactly once
    * def id = newId()
    * eval Kafka.publish(id, 'bob', 10.5, null)
    * def calls = Kafka.awaitLinkingCalls(id, 1, 15)
    * match calls == '#[1]'
    * match calls[0].body contains { orderId: '#(id)', customer: 'bob', amount: 10.5, currency: 'USD' }

  Scenario: A temporary linking failure is retried, then the order is dead-lettered
    * def id = newId()
    * eval Linking.failWith(id, 503)
    * eval Kafka.publish(id, 'carol', 20.0, 'GBP')
    # 1 attempt + 2 retries
    * def calls = Kafka.awaitLinkingCalls(id, 3, 20)
    * match calls == '#[3]'
    * def dead = Kafka.awaitDeadLetters(id, 1, 20)
    * match dead == '#[1]'
    * match dead[0].exception == '#string'

  Scenario: A rejected order is dead-lettered without retries
    * def id = newId()
    * eval Linking.failWith(id, 400)
    * eval Kafka.publish(id, 'dave', 5.0, 'USD')
    * def dead = Kafka.awaitDeadLetters(id, 1, 20)
    * match dead == '#[1]'
    * def calls = Kafka.awaitLinkingCalls(id, 1, 5)
    * match calls == '#[1]'

  Scenario: A malformed message is dead-lettered and does not block the next order
    * def poisonKey = newId()
    * def id = newId()
    * eval Kafka.publishRaw(poisonKey, 'this-is-not-avro')
    * eval Kafka.publish(id, 'erin', 7.0, 'USD')
    * def dead = Kafka.awaitDeadLetters(poisonKey, 1, 20)
    * match dead == '#[1]'
    * def calls = Kafka.awaitLinkingCalls(id, 1, 15)
    * match calls == '#[1]'
