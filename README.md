# sun_kafka

End-to-end test framework for a service that **consumes orders from Kafka and calls the  linking service**.
Built with Spring Boot + Spring Kafka (Java 21, Maven). Messages are **Avro** checked by **Schema Registry**; processed
records are remembered in **Postgres**. The linking service is faked with WireMock, so everything runs in tests without
Docker; Docker adds real-broker and real-database checks.

Requirements, gaps and the status of each: [docs/REQUIREMENTS.md](docs/REQUIREMENTS.md). Design decisions:
[docs/DECISIONS.md](docs/DECISIONS.md).

## How it works

### 1. The happy path
```
 POST /api/orders ──► validate ──┐
                                 ▼
  OrderProducer ──► Kafka "orders" (3 partitions) ──► OrderConsumer
  key = orderId        Avro value + schema id            1. validate (OrderValidator)
                            │                            2. already processed? skip   (Postgres)
              Schema Registry checks the schema          3. call linking service       (HTTP, Idempotency-Key)
                                                         4. remember the record         (Postgres)
```
- **Event** - `src/main/avro/OrderEvent.avsc` is the contract (`orderId`, `customer`, `amount`, `currency`). The Java
  class `OrderEvent` is *generated from it* at build time.
- **Producer** - keys records by `orderId` (same order -> same partition -> in order); `acks=all`, idempotent.
- **Schema Registry** - messages carry a small schema id; the registry stores schemas and rejects incompatible changes.
- **Validation** - one place, `OrderValidator`, used by both the consumer and the HTTP endpoint.
- **Linking service** - `LinkingClient` posts each valid order to `POST /links` with an `Idempotency-Key` header equal to
  the Kafka record id (`topic-partition-offset`). The real API is not known yet, so path and body are assumptions that
  live only in this class.
- **Idempotency** - after a successful link the record id is stored in `processed_message` (Flyway migration
  `V1__processed_message.sql`). A redelivered record is skipped, so it is not linked twice.
- **Endpoints** - `POST /api/orders` (202 with the order id, or 400 with the reasons) and `GET /actuator/health`
  (`kafka` and `db` components) plus `/actuator/health/readiness`.

### 2. When something goes wrong
| What failed | What happens |
|---|---|
| Order breaks a validation rule | **No retry.** Straight to `orders.DLT` |
| Linking service answers 4xx (e.g. 400) | **No retry.** Straight to `orders.DLT` |
| Linking service answers 5xx, 408, 429, times out, or is unreachable | Retried 2 more times (200 ms apart, 3 attempts total), then `orders.DLT` |
| Message is not valid Avro (poison pill) | **No retry.** Straight to `orders.DLT`; later messages are not blocked |

Dead-lettered records keep the exception details in their headers. Rules live in `KafkaErrorHandlingConfig`. The
`Seek to current after exception` ERROR lines in test logs are Spring's normal redelivery mechanism, not failures.

### 3. How the tests are built
| Layer | Broker / registry / database / linking service | Docker? |
|---|---|---|
| JUnit E2E tests, Cucumber BDD | `@EmbeddedKafka` (real Kafka inside the test JVM), in-memory `mock://` Schema Registry, in-memory H2 (Postgres mode), WireMock | No |
| `KafkaContainerTest` | real `cp-kafka` + `cp-schema-registry` containers (Testcontainers) | Yes |
| `PostgresMigrationTest` | real `postgres` container: runs the Flyway migrations and the store | Yes |

The two Docker tests skip themselves when Docker is missing; **CI fails if either is skipped**. The same application
code runs in every layer; only what sits underneath changes.

## Layout
| Path | Purpose |
|---|---|
| `src/main/avro/OrderEvent.avsc` | The event schema |
| `producer/`, `consumer/` | `OrderProducer`; `OrderConsumer` (validate, link, remember) |
| `linking/` | `LinkingClient` and its two failure types (transient / permanent) |
| `validation/OrderValidator` | The business rules |
| `persistence/ProcessedMessageStore` + `db/migration/` | Idempotency store and Flyway migrations |
| `web/OrderController`, `health/KafkaHealthIndicator` | HTTP endpoint and Kafka health check |
| `config/` | Topics (`orders`, `orders.DLT`) and the error handler |
| `src/main/resources/application.yml` | All settings; environment variables listed below |
| `test/.../support/` | `OrderEvents` (builders), `TopicReader` (raw consumer), `LinkingStub` (fake linking service) |
| `test/.../KafkaE2ETestBase` | Shared Spring + embedded broker setup for JUnit tests |
| `test/.../bdd/`, `src/test/resources/features/` | Cucumber runner, steps and Gherkin features |
| `docker-compose.yml` | Local Kafka + Schema Registry + Postgres |
| `.github/workflows/ci.yml`, `.github/dependabot.yml` | CI and weekly dependency updates |

## Test suites
| Test | Proves |
|---|---|
| `OrderMessagingTest` | delivered; same key ordered on one partition; keys spread across partitions |
| `LinkingServiceTest` | exactly one call with an idempotency key; 503 and 429 retried then dead-lettered; 400 not retried; a slow service times out; recovery after a temporary failure; a failing order does not block later ones; a redelivered record is not linked twice |
| `DeadLetterTest` | invalid order dead-lettered without retry; non-Avro message dead-lettered without blocking the consumer |
| `ValidationTest` | every validation rule, and that all problems are reported together |
| `EndpointTest` | order accepted and flows through Kafka to the linking service; invalid, malformed and wrong-type requests rejected |
| `HealthTest` | health and readiness UP with `kafka` and `db`; Kafka indicator DOWN when unreachable |
| `SchemaRegistryTest` | subject `orders-value` registered; `currency` defaults to `USD`; compatible change accepted, breaking change rejected |
| `bdd/CucumberTest` | 14 Gherkin scenarios (see below) |
| `karate/KarateTest` | Karate proof of concept: 9 scenarios against the running app ([guide](docs/KARATE-GUIDE.md), [results](docs/KARATE-POC-RESULTS.md)) |
| `KafkaContainerTest`, `PostgresMigrationTest` | the same behaviour on real Kafka, Schema Registry and Postgres |

```bash
mvn test        # everything except the Docker tests (they skip themselves)
mvn verify      # + Docker tests if Docker is running, + coverage report and 80% line-coverage gate
```
Coverage report: `target/site/jacoco/index.html`. Cucumber report: `target/cucumber-reports/report.html`.

## BDD (Cucumber)
Scenarios in `src/test/resources/features/*.feature`, step code in `src/test/java/com/sunkafka/bdd/`:

| Feature | Scenarios |
|---|---|
| `order_messaging` | delivery; same order stays ordered on one partition; orders spread across partitions |
| `linking_service` | exactly one call; temporary failure retried; persistent failure dead-lettered after retries; rejection not retried; redelivered record not linked twice |
| `failure_handling` | invalid order dead-lettered immediately; malformed message does not block later orders |
| `schema_evolution` | subject registered; currency defaults to USD; compatible change accepted / breaking change rejected |

Run a subset by tag, e.g. `mvn test -Dtest=CucumberTest -Dcucumber.filter.tags=@smoke`.

## Karate (proof of concept)
[Karate](https://github.com/karatelabs/karate) is an API-testing framework with Gherkin-like syntax. The PoC runs the
real application on a random port (same embedded Kafka, in-memory registry and database as the other tests) and checks
the whole event flow: HTTP -> Kafka -> consumer -> linking service, plus the failure paths. 9 scenarios, no Docker needed.

```bash
mvn test -Dtest=KarateTest        # report: target/karate-reports/karate-summary.html
```

- **How to use and extend it:** [docs/KARATE-GUIDE.md](docs/KARATE-GUIDE.md)
- **What it measured:** [docs/KARATE-POC-RESULTS.md](docs/KARATE-POC-RESULTS.md) (9/9 passing, deliberate-breakage checks, one test-helper bug found and fixed)

## Run the app against local services
```bash
docker compose up -d          # Kafka :9092, Schema Registry :8081, Postgres :5432  (or: podman compose up -d)
mvn spring-boot:run
```
| Environment variable | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Schema Registry |
| `DB_URL`, `DB_USERNAME`, `DB_PASSWORD` | local Postgres, `sunkafka` | Database (defaults are for local use only) |
| `LINKING_BASE_URL` | `http://localhost:8089` | EU linking service |

**Not verified yet:** `docker-compose.yml` has never been run (no Docker or Podman was available when it was written),
and nothing has been run under Podman. For Podman, expect to need: `unqualified-search-registries = ["docker.io"]` in
`registries.conf` (short image names), and for Testcontainers `DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock`
and `TESTCONTAINERS_RYUK_DISABLED=true`.

## Developer tooling for Claude Code (optional)
Configured in `.claude/settings.json` and `CLAUDE.md`. Both are third-party; nothing here is needed to build or test.

| Tool | What it does | What you need to know |
|---|---|---|
| [Ponytail](https://github.com/DietrichGebert/ponytail) plugin | Nudges the coding agent toward the simplest solution (no unrequested abstractions) | Claude Code offers the plugin when you open the repo and trust it. Needs `node` on `PATH` (its hooks are Node scripts). It writes a local task ledger to `~/.config/ponytail/`; its optional webhook is empty by default, so nothing leaves your machine. Manual install: `/plugin marketplace add DietrichGebert/ponytail`, then `/plugin install ponytail@ponytail` |
| [Graphify](https://pypi.org/project/graphifyy/) | Builds a knowledge graph of the code so the agent can run `graphify query/path/explain` instead of grepping | `pip install graphifyy` (Python 3.11.5+), then `graphify update .`. That command is **code-only and local** (tree-sitter, no LLM, no network). The graph lands in `graphify-out/` (git-ignored, about 1 MB, rewritten on every build). The `/graphify` skill's *document* extraction does send file contents to an LLM API, so only use it deliberately |

The graphify hooks in `.claude/settings.json` do nothing unless `graphify` is installed **and** `graphify-out/graph.json`
exists; they only print hints before searches. To remove graphify: `graphify uninstall`, then delete its entries from
`.claude/settings.json` and the section in `CLAUDE.md`. To drop Ponytail: remove the two `ponytail` entries from
`.claude/settings.json`.

## Extending the framework
- **New scenario**: write it in a `.feature` file; add any missing step in `bdd/*Steps.java`.
- **New JUnit test**: extend `KafkaE2ETestBase` (gives `producer`, `consumer`, `store`, `brokers`); build data with
  `OrderEvents`; script the linking service with `LinkingStub`; wait with Awaitility.
- **Change the event**: edit `OrderEvent.avsc`. A new field **must have a default** (like `currency`) so old data stays
  readable; the registry rejects breaking changes.
- **Change the linking contract**: edit `LinkingClient` only (path, body, headers, which statuses are retryable).
- **Change retry behaviour**: `KafkaErrorHandlingConfig` (`FixedBackOff(200L, 2L)`, non-retryable exception list).
- **Switch schema registry** (e.g. to AWS Glue Schema Registry, which has a different message format): change the
  serializer/deserializer classes and registry settings in `application.yml` and the dependency in `pom.xml`. The
  dead-letter publisher follows `spring.kafka.producer.value-serializer`. Not tried with Glue yet.

## Build notes
- The Confluent Avro serializer is published only on `https://packages.confluent.io/maven/` (declared in `pom.xml`); a
  network that blocks that host cannot resolve dependencies.
- `commons-compress` is pinned in `pom.xml` because the version Avro pulls in breaks Testcontainers.
- Tests that start their own Spring context get their own database (random name), because each embedded Kafka broker
  restarts offsets at 0 and the idempotency keys would otherwise collide across contexts.
