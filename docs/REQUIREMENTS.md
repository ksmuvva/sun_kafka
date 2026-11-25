# Test framework requirements

**Scope:** this repository is the *test framework* for a service that consumes messages from a Kafka topic and calls the
**EU linking service**. It is not the production service. These requirements say what the framework must be able to prove
about that service, and what is still missing.

**Basis:** the project summary (Kafka consume -> EU linking service call; AWS with the service still to be decided;
Maven or Gradle; Podman + local Kafka and Postgres; Avro and a schema registry, possibly AWS MSK's built-in one;
Java 21 now, possibly Java 25 / Spring 7 / Jackson 3 later; endpoints, Kafka connectivity and validation in the app).
Anything the summary does not say is listed under [Open questions](#open-questions) and marked **(assumption)**.

Priority uses MoSCoW: **M**ust, **S**hould, **C**ould.

---

## 1. What the framework already covers

| Area | Status | Evidence |
|---|---|---|
| Publish and consume over Kafka, key-based ordering, partition spread | Done | `OrderMessagingTest` |
| Retry, then dead-letter topic; poison pills do not block the consumer | Done | `DeadLetterTest` |
| Avro events, Schema Registry registration and compatibility rules | Done (Confluent registry) | `SchemaRegistryTest` |
| Real Kafka + Schema Registry in containers | Done (Docker) | `KafkaContainerTest` |
| BDD scenarios in Gherkin | Done (14 scenarios) | `bdd/`, `features/` |
| CI that fails if the real-broker test is skipped | Done | `.github/workflows/ci.yml` |
| Maven build, Java 21 | Done | `pom.xml`, CI |
| Calls to the linking service (stub), retry / dead-letter policy | Done | `LinkingServiceTest`, `linking_service.feature` |
| Validation, Postgres idempotency store, endpoint, health | Done | `ValidationTest`, `PostgresMigrationTest`, `EndpointTest`, `HealthTest` |
| Coverage gate, dependency updates | Done | JaCoCo (80% gate), `dependabot.yml` |

The domain is still a **placeholder** (`OrderEvent`). The real message schema and linking-service contract are not known yet.

## 2. Gaps against the project summary

| # | Gap | Why it matters | Status now |
|---|---|---|---|
| G1 | Nothing calls an external HTTP service | The core job, "call the EU linking service", is untested | Closed: stub + failure policy tests (contract still assumed) |
| G2 | No Postgres | The team is installing Postgres locally, so the service will probably persist data | Closed: Flyway + store tested on H2 and real Postgres |
| G3 | Java 17 / Spring Boot 3.3.5, not Java 21 | Team starts on Java 21 | Closed: Java 21 (Spring Boot 3.3.5) |
| G4 | Docker, not Podman | Team will use Podman; Testcontainers and compose behave differently there | Open: written down, **not verified** (no Podman available) |
| G5 | Confluent serializers only | AWS MSK's built-in registry is AWS Glue Schema Registry, which uses a different wire format | Partly: swap points isolated; Glue itself not tried |
| G6 | No MSK / AWS authentication or config tests | AWS service is undecided, but MSK auth (IAM / SASL, TLS) will differ from local plaintext | Open: needs the AWS decision |
| G7 | No endpoint tests | The app will expose endpoints (health, status, etc.) | Closed: `EndpointTest` |
| G8 | Validation covers one rule (`amount > 0`) | Summary lists validation as a core dependency | Closed: `OrderValidator`, 4 rules |
| G9 | No quality gates (coverage, static analysis, dependency scanning) | Needed for a client-facing delivery | Partly: coverage + Dependabot done; static analysis not |
| G10 | No resilience, idempotency or performance tests | Consumers must survive duplicates, restarts and a failing downstream | Partly: linking failures and duplicates done; broker / DB outage not |
| G11 | Compose file never run | `docker-compose.yml` is unverified | Open: file is valid YAML, **never run** |

## 3. Implementation status

Honest status of each requirement in section 4. **Done** = implemented and passing in CI; **Partial** = some of the
acceptance criteria met; **Not verified** = written but never run; **Not done** = nothing yet (reason given).

| Status | Requirements |
|---|---|
| **Done (15)** | R1, R2, R3, R4, R10, R12, R13, R15, R16, R17, R18, R22, R27, R29, R31 |
| **Partial (8)** | R6, R8, R9, R11, R14, R23, R30, R32 |
| **Not verified (2)** | R20, R21 |
| **Not done (9)** | R5, R7, R19, R24, R25, R26, R28, R33, R34 |

| ID | Status | Evidence / what is missing |
|---|---|---|
| R1 | Done | `support/LinkingStub` (WireMock): success, 4xx, 5xx, slow |
| R2 | Done | `LinkingServiceTest.validOrderCallsTheLinkingServiceExactlyOnce`, BDD scenario |
| R3 | Done | 503 and 429 retried then dead-lettered; 400 dead-lettered without retry; recovery after a temporary failure. The policy itself is an assumption until Q6 is answered |
| R4 | Done | A redelivered record (same topic, partition, offset) is skipped. Key caveat: see Q10 |
| R5 | Not done | No linking-service API spec yet (Q1) |
| R6 | Partial | Timeout tested (slow service retried, then dead-lettered). Circuit breaker not implemented |
| R7 | Not done | Real schema unknown (Q2); `OrderEvent` is still the placeholder (it gained `currency`) |
| R8 | Partial | Serializer classes and registry settings are in `application.yml`; the dead-letter publisher follows them. Glue not tried |
| R9 | Partial | `SchemaRegistryTest` shows the registry's rules; the build does not compare against a real registry's latest schema |
| R10 | Done | `ValidationTest` (4 rules, 7 cases), `DeadLetterTest`, `EndpointTest` |
| R11 | Partial | Ordering and partition spread done; no consumer rebalance test |
| R12 | Done | Flyway + H2 in all tests; real Postgres in `PostgresMigrationTest` (CI only, needs Docker) |
| R13 | Done | Row exists after a successful link, none after a dead letter. No separate failure table |
| R14 | Partial | Clean database tested; only one migration exists, so the upgrade path is untested |
| R15 | Done | `EndpointTest` (accepted, invalid, malformed, wrong content type) |
| R16 | Done | `HealthTest`: `kafka` and `db` UP, Kafka indicator DOWN when unreachable. The linking service is not in readiness (no health endpoint known) |
| R17 | Done | `OrderValidator` is the only rule set; consumer and endpoint both use it |
| R18 | Done | Java 21 in `pom.xml` and CI |
| R19 | Not done | Deferred until Q8 is answered |
| R20 | Not verified | Podman notes in README; nothing run under Podman |
| R21 | Not verified | `docker-compose.yml` (now with Postgres) parses as YAML; never started |
| R22 | Done | `docs/DECISIONS.md` |
| R23 | Partial | Environment variables and a `test` profile; no AWS profile yet |
| R24-R26 | Not done | Waiting for the AWS service decision (Q3) |
| R27 | Done | JaCoCo report; build fails below 80% line coverage (currently about 97%) |
| R28 | Not done | No Checkstyle / Spotless / SpotBugs yet |
| R29 | Done | Dependabot for Maven and GitHub Actions (no build-failing vulnerability scan) |
| R30 | Partial | `@smoke` tag on two scenarios; unit vs integration tests not split into separate phases |
| R31 | Done | CI uploads the Cucumber and coverage reports as the `reports` artifact |
| R32 | Partial | Slow, failing and unreachable linking service covered; broker or database outage not |
| R33 | Not done | No agreed volume or latency targets (Q7) |
| R34 | Not done | No correlation id yet |

## 4. Requirements

### 4.1 Downstream service (EU linking service)

| ID | Requirement | Pri | Acceptance criteria |
|---|---|---|---|
| R1 | The framework can simulate the EU linking service (stub server) | M | A stub (e.g. WireMock) runs in tests and returns configurable success, 4xx, 5xx, slow and no-response replies |
| R2 | Every consumed message results in exactly one linking-service call on success | M | Scenario: publish a valid message, assert one request with the expected body and headers reached the stub |
| R3 | Linking-service failures follow a defined policy | M | Scenarios: transient 5xx / timeout is retried then dead-lettered; permanent 4xx goes to the dead-letter topic without retries; consumer keeps processing later messages |
| R4 | Duplicate delivery does not cause duplicate links | M | Publishing the same message twice, or restarting the consumer mid-batch, produces one effective link (needs an idempotency key **(assumption)**) |
| R5 | The linking-service contract is checked, not just mocked | S | Request/response validated against the provider's OpenAPI spec or a contract test (e.g. Pact) once the spec is available |
| R6 | Timeouts, retry back-off and circuit-breaker behaviour are tested | S | Configurable values asserted by tests; downstream outage does not crash or stall the consumer |

### 4.2 Messaging and schemas

| ID | Requirement | Pri | Acceptance criteria |
|---|---|---|---|
| R7 | The real message schema replaces the placeholder `OrderEvent` | M | `.avsc` for the real event committed; generated class used in producer, consumer and tests |
| R8 | Schema registry choice is abstracted | M | Tests run against the Confluent registry today, and the registry-specific code sits behind one configuration point so AWS Glue Schema Registry can be swapped in |
| R9 | Schema compatibility rules are enforced in CI | S | Build fails if a schema change is not backward compatible with the registered version |
| R10 | Producer-side and consumer-side validation are both tested | M | Invalid payloads (missing field, wrong type, out-of-range value) are rejected or dead-lettered with a recorded reason |
| R11 | Ordering, partitioning and consumer-group behaviour are tested | S | Rebalance test: stop and start a second consumer, no message lost, per-key order preserved |

### 4.3 Persistence (Postgres)

| ID | Requirement | Pri | Acceptance criteria |
|---|---|---|---|
| R12 | Postgres is available in tests | M | A Postgres container starts for integration tests; schema created by migrations (Flyway or Liquibase) **(assumption: migrations tool)** |
| R13 | Processing state is verified in the database | S | Scenario: after a successful link the database row exists once; after a dead-letter, a failure record exists |
| R14 | Migrations are tested from empty and from the previous version | S | CI applies all migrations to a clean database |

### 4.4 Application surface

| ID | Requirement | Pri | Acceptance criteria |
|---|---|---|---|
| R15 | Exposed endpoints are tested | M | HTTP tests (MockMvc or RestAssured) cover each endpoint, including error responses and input validation |
| R16 | Health checks report Kafka (and DB / linking service) status | S | Readiness endpoint turns unhealthy when the broker or database is unavailable; tested |
| R17 | Request validation uses one shared approach | M | Bean Validation on inputs, with tests for each constraint; same rules reused where messages and endpoints overlap |

### 4.5 Environments and tooling

| ID | Requirement | Pri | Acceptance criteria |
|---|---|---|---|
| R18 | Java 21 is the build and runtime baseline | M | `java.version` = 21; CI runs on Java 21; Spring Boot version supports Java 21 |
| R19 | A migration path to Java 25 / Spring 7 / Jackson 3 is kept visible | C | Optional CI job on Java 25; a note listing the breaking changes found. Decision is the client's **(see open questions)** |
| R20 | The stack runs under Podman | M | `podman compose up` starts Kafka, Schema Registry and Postgres; Testcontainers tests pass with Podman's socket; steps documented in README |
| R21 | The compose file is verified | M | `docker-compose.yml` actually run and checked (Kafka reachable on 9092, registry on 8081) |
| R22 | Build tool decision recorded | S | Short decision note: Maven kept (declarative, already in use) or Gradle chosen, with reasons |
| R23 | Configuration separates local, test and AWS | M | Spring profiles / environment variables for bootstrap servers, registry URL, auth; no secrets in the repo |

### 4.6 AWS readiness

| ID | Requirement | Pri | Acceptance criteria |
|---|---|---|---|
| R24 | MSK connection settings are testable | S | Config for TLS and IAM or SASL auth is covered by tests that run without a real MSK (unit-level) and a documented manual check against a dev MSK |
| R25 | AWS services can be faked locally once chosen | C | LocalStack (or equivalent) added for whichever AWS service is selected (queue, storage, secrets, etc.) |
| R26 | CI runs against AWS-like conditions | C | Optional pipeline stage against a real dev environment; deferred until the AWS service and account access exist |

### 4.7 Quality gates and reporting

| ID | Requirement | Pri | Acceptance criteria |
|---|---|---|---|
| R27 | Code coverage is measured and reported | S | JaCoCo report in CI; agreed minimum threshold enforced |
| R28 | Static analysis and formatting are enforced | S | Spotless / Checkstyle (and optionally SpotBugs) run in CI and fail the build |
| R29 | Dependencies are scanned for known vulnerabilities | S | Dependabot or OWASP dependency check enabled; critical findings fail the build |
| R30 | Unit, integration and slow tests are separated and tagged | S | `mvn test` runs fast tests only; `mvn verify` runs everything; BDD tags (e.g. `@smoke`) select subsets |
| R31 | Test reports are visible to non-developers | C | Cucumber HTML report (already uploaded in CI) published to an accessible place |

### 4.8 Resilience and performance

| ID | Requirement | Pri | Acceptance criteria |
|---|---|---|---|
| R32 | Failure scenarios beyond bad messages are tested | S | Broker unavailable at start and mid-run; database unavailable; linking service down; each recovers without message loss |
| R33 | Throughput and latency baseline is measured | C | A repeatable load test (e.g. 10,000 messages) reports messages per second and end-to-end latency; target numbers agreed with the client |
| R34 | Logs and metrics allow tracing a message end to end | C | Each message carries a correlation id into logs and the linking-service call; checked in a test |

## 5. Suggested order

1. **Foundation (do first):** R18 Java 21, R7 real schema, R1 linking-service stub, R2-R3 call and failure scenarios, R17 / R10 validation.
2. **Persistence and surface:** R12-R13 Postgres, R15-R16 endpoints and health.
3. **Environment:** R20-R21 Podman and compose verification, R23 configuration profiles.
4. **Hardening:** R4 idempotency, R8 registry abstraction, R27-R30 quality gates, R32 resilience.
5. **Later / when decisions land:** R19 Java 25, R24-R26 AWS, R33-R34 performance and tracing.

## 6. Open questions

These block or change the requirements above; the client or team needs to answer them.

| # | Question | Affects |
|---|---|---|
| Q1 | What is the EU linking service API (protocol, auth, rate limits, idempotency key, error codes)? Is there an OpenAPI spec or sandbox? | R1-R6 |
| Q2 | What is the real message schema, and who owns it? | R7, R9, R10 |
| Q3 | Which AWS service runs the application (container, serverless, other)? | R24-R26 |
| Q4 | Confluent Schema Registry or AWS Glue Schema Registry (MSK's built-in)? | R8, R24 |
| Q5 | Does the service store anything in Postgres, and what? Migrations tool? | R12-R14 |
| Q6 | What should happen to a message the linking service permanently rejects (dead-letter, alert, manual replay)? | R3 |
| Q7 | Expected volume and latency targets? | R33 |
| Q8 | Java 21 or Java 25 for delivery? Which Spring Boot version does the client require? | R18-R19 |
| Q9 | Maven or Gradle: is the choice final? | R22 |
| Q10 | Idempotency key: the framework uses `topic-partition-offset`, which collides if a topic is recreated or consumers move to another cluster (offsets restart). Is there a business key (or message id) to use instead? | R4 |

## 7. Out of scope

Building the production service itself, deploying to AWS, and production monitoring. This repository only provides the means to
test the service.
