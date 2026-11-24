# sun_kafka

Test framework for Kafka messaging: a **publisher** and a **consumer** built with Spring Boot + Spring Kafka (Java 17, Maven).

## Layout
| Path | Purpose |
|---|---|
| `producer/OrderProducer` | Publishes `OrderEvent` JSON, keyed by `orderId`, `acks=all`, idempotent |
| `consumer/OrderConsumer` | `@KafkaListener` on the `orders` topic |
| `config/KafkaTopicConfig` | Declares the `orders` topic (3 partitions) |
| `OrderMessagingTest` | Publisher -> broker -> consumer test on an embedded broker (no Docker) |

## Run
```bash
mvn test                      # embedded Kafka, no setup needed
docker compose up -d          # real local broker on localhost:9092
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 mvn spring-boot:run
```
