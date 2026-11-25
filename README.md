# sun_kafka

End-to-end test framework for Kafka messaging: a **publisher** and a **consumer** with retry / dead-letter handling,
built with Spring Boot + Spring Kafka (Java 17, Maven). Messages are **Avro**, validated by **Confluent Schema Registry**.
Tests run on an embedded broker (no Docker) and on real Kafka + Schema Registry containers (Testcontainers).

## How it works

### 1. The happy path
```
  OrderProducer                    Kafka topic "orders"                   OrderConsumer
  (KafkaTemplate)                  (3 partitions)                         (@KafkaListener)
        |                                  |                                    |
  OrderEvent (Avro class)                  |                                    |
        |-- key = orderId ---------------> partition = hash(key) % 3            |
        |-- value = Avro bytes ----------> [magic byte][schema id][payload]     |
        |        |                                  |                           |
        |        +--> Schema Registry: schema registered under subject          |
        |             "orders-value" (first publish) and checked for             |
        |             compatibility                                              |
        |                                  |-----------------------------------> deserialize via schema id
        |                                  |                                     -> OrderEvent -> onMessage()
```
- **Event** - `src/main/avro/OrderEvent.avsc` is the contract (`orderId`, `customer`, `amount`, `currency`). The Java
  class `OrderEvent` is *generated from it* at build time; there is no hand-written model.
- **Producer** - `OrderProducer.send()` keys every record by `orderId`, so all events of one order go to the same
  partition and stay in order. It uses `acks=all` and idempotence (no loss, no duplicates from retries).
- **Schema Registry** - the Avro serializer sends only a small schema id with each message; the registry stores the
  schema and rejects incompatible versions. That is what lets producers and consumers evolve independently.
- **Consumer** - `OrderConsumer.onMessage()` receives the typed `OrderEvent`. It rejects non-positive amounts (our
  example of a business rule). It also records what it received so tests can assert on it.

### 2. When something goes wrong
```
  record fails in onMessage()  --> retry (200 ms apart, 2 retries = 3 attempts total)
                                       |-- still failing --> published to "orders.DLT" (same partition,
                                       |                     exception details in the record headers)
  bytes that are not valid Avro --> no retries, straight to "orders.DLT"
```
- Configured in `KafkaErrorHandlingConfig` (`DefaultErrorHandler` + `DeadLetterPublishingRecoverer`).
- Un-deserializable "poison pills" are caught by `ErrorHandlingDeserializer`, so one bad message cannot stall the
  partition; later good messages are still consumed.
- The `Seek to current after exception` ERROR lines in the test log are Spring's normal redelivery mechanism, not
  failures.

### 3. How the tests are built
| Layer | What it uses | Needs Docker? |
|---|---|---|
| JUnit E2E tests (`OrderMessagingTest`, `DeadLetterTest`, `SchemaRegistryTest`) | `@EmbeddedKafka` (a real Kafka broker inside the test JVM) + `mock://` in-memory Schema Registry, via `KafkaE2ETestBase` | No |
| Cucumber BDD (`bdd/CucumberTest`) | the same embedded setup, scenarios written in Gherkin | No |
| `KafkaContainerTest` | Testcontainers: real `cp-kafka` + real `cp-schema-registry` | Yes (auto-skipped without it) |

A typical test: **publish** an event through the real `OrderProducer` -> the embedded broker carries it -> the real
`OrderConsumer` receives it -> the test **awaits** (Awaitility) and asserts. For dead letters, `TopicReader` is an
independent raw consumer that reads `orders.DLT` to prove the message landed there.

The whole application code runs unchanged in every layer; only the broker and registry underneath differ. CI also
fails if `KafkaContainerTest` is skipped, so the real-broker check cannot silently disappear.

## Layout
| Path | Purpose |
|---|---|
| `src/main/avro/OrderEvent.avsc` | The event schema; `OrderEvent` Java class is generated from it at build time |
| `producer/OrderProducer` | Publishes Avro `OrderEvent` keyed by `orderId` (`acks=all`, idempotent) |
| `consumer/OrderConsumer` | `@KafkaListener` on `orders`; rejects non-positive amounts |
| `config/KafkaTopicConfig` | `orders` topic (3 partitions) |
| `config/KafkaErrorHandlingConfig` | 2 retries (200 ms), then publish to `orders.DLT`; poison pills (bytes that are not valid Avro) skip retries |
| `src/main/resources/application.yml` | Topic name, serializers, `KAFKA_BOOTSTRAP_SERVERS` and `SCHEMA_REGISTRY_URL` settings |
| `test/.../support/` | Reusable helpers: `OrderEvents` (builders), `TopicReader` (raw consumer for any topic) |
| `test/.../KafkaE2ETestBase` | Spring context + embedded broker + mock registry shared by the JUnit E2E tests |
| `test/.../bdd/` | Cucumber runner, Spring wiring and step definitions |
| `src/test/resources/features/` | The Gherkin `.feature` files |
| `docker-compose.yml` | Local Kafka + Schema Registry for running the app by hand |
| `.github/workflows/ci.yml` | CI: `mvn verify`, real-broker-skipped guard, Cucumber report upload |

## Test suites
| Test | Proves |
|---|---|
| `OrderMessagingTest` | event delivered; same key stays ordered on one partition; different keys spread across partitions |
| `DeadLetterTest` | invalid event retried 3x total then dead-lettered; malformed (non-Avro) message dead-lettered and consumer not blocked |
| `SchemaRegistryTest` | schema registered under `orders-value`; `currency` defaults to `USD`; compatible change accepted, breaking change rejected |
| `bdd/CucumberTest` | 9 Cucumber (BDD) scenarios over the same behaviour, written in plain Gherkin (see below) |
| `KafkaContainerTest` | Avro round trip on a real Kafka + real Schema Registry (Testcontainers); auto-skipped without Docker |

```bash
mvn test        # embedded broker + in-memory (mock://) Schema Registry, no Docker needed
mvn verify      # same, plus the real-broker test if Docker is running
```

## Run the app against a local broker
```bash
docker compose up -d          # Kafka on :9092, Schema Registry on :8081
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 SCHEMA_REGISTRY_URL=http://localhost:8081 mvn spring-boot:run
```
Note: the `docker-compose.yml` setup has not been run by the authors yet (no Docker was available when it was written).

## BDD (Cucumber)
Scenarios live in `src/test/resources/features/*.feature` and read as plain English:

| Feature | Scenarios |
|---|---|
| `order_messaging.feature` | delivery; same order stays ordered on one partition; different orders spread across partitions |
| `failure_handling.feature` | invalid order retried then dead-lettered; malformed message dead-lettered without blocking later orders |
| `schema_evolution.feature` | subject registered; currency defaults to USD; compatible change accepted / breaking change rejected |

Step definitions are in `src/test/java/com/sunkafka/bdd/` and reuse the producer, consumer and `support/` helpers.
`mvn test` runs them; the HTML report is written to `target/cucumber-reports/report.html` (CI uploads it as the
`cucumber-report` artifact).

## Extending the framework
- **Add a scenario**: write it in a `.feature` file; add any missing step in `bdd/*Steps.java`.
- **Add a JUnit test**: extend `KafkaE2ETestBase` (gives you `producer`, `consumer`, `brokers`); build data with
  `OrderEvents`; use Awaitility to wait for the consumer.
- **Change the event**: edit `OrderEvent.avsc`. A new field **must have a default** (like `currency`) so old data stays
  readable; the registry rejects breaking changes (see `SchemaRegistryTest.schemaEvolutionRulesAreEnforced`).
- **Change retry behaviour**: `KafkaErrorHandlingConfig` (`FixedBackOff(200L, 2L)` = 200 ms apart, 2 retries).
- **Add a new topic or event type**: add a `NewTopic` bean, a schema, a producer and a `@KafkaListener`, then reuse the
  same test base and helpers.

## Build note
The Confluent Avro serializer is published only on `https://packages.confluent.io/maven/` (declared in `pom.xml`).
If your network blocks that host the build cannot resolve dependencies.

CI (`.github/workflows/ci.yml`) runs `mvn verify` on every push to `main` and every PR.
