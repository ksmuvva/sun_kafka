# Karate PoC: results

**Goal:** show that Karate (Java + Gherkin-style features) can validate Kafka producer and consumer integration and
end-to-end event processing. How to use it: [KARATE-GUIDE.md](KARATE-GUIDE.md).
**Run date:** 2026-11-25, in the development sandbox (no Docker).

## Verdict

| Question | Answer | Evidence |
|---|---|---|
| Can Karate validate the **producer** side (HTTP -> Kafka)? | Yes | 1 end-to-end scenario + 3 rejection scenarios pass |
| Can it validate the **consumer** side (Kafka -> linking service)? | Yes | 4 scenarios pass: success, retry then dead-letter, rejection, poison message |
| Does it work end to end? | Yes | `POST /api/orders` -> Avro event on Kafka -> exactly one linking call, in one scenario |
| Can the tests actually fail? | Yes, after one fix | 3 deliberate breakages: 2 caught at once, 1 slipped through until a bug in the test helper was fixed |
| Is it stable? | Yes in 5 of 5 full runs | all 9 scenarios passed every time |
| Does it need extra work? | Yes: Kafka access | Karate (1.4.1) has no built-in Kafka support that I found; a 157-line Java helper does the Kafka work |

## How the work was done

- **Superpowers `verification-before-completion`** (loaded as a skill): no result below comes from memory. Each claim has a
  command run in this session, and every test was also run *red* (broken on purpose), not just green.
- **Ponytail**: the plugin is configured in the repo but only loads when a session starts, so it was **not available as a
  skill in this session**. I read its published rules (`skills/ponytail/SKILL.md`) and followed them by hand: smallest
  complete change, no new framework code, one small fix, two documents. This is not the same as running the plugin.
- The PoC itself already existed on `main` (PR #15). This work verified it, fixed what the verification found, and
  documented it.

## Setup under test

Java 21.0.13, Spring Boot 3.3.5, Karate 1.4.1, embedded Kafka (spring-kafka-test), in-memory Schema Registry (`mock://`),
in-memory H2 database, fake linking service (WireMock 3.9.2). The real application runs on a random HTTP port.
2 feature files, 9 scenarios, 98 lines of features.

## Results

### Scenarios (final run, after the fix)

| Feature | Scenario | Result | Time |
|---|---|---|---|
| `kafka-consumer` | An order published straight to Kafka is linked exactly once | PASS | 871 ms |
| `kafka-consumer` | A temporary linking failure is retried, then the order is dead-lettered (3 calls) | PASS | 2178 ms |
| `kafka-consumer` | A rejected order is dead-lettered without retries (1 call) | PASS | 1664 ms |
| `kafka-consumer` | A malformed message is dead-lettered and does not block the next order | PASS | 1196 ms |
| `orders-e2e` | An order posted over HTTP is published to Kafka and linked exactly once | PASS | 1482 ms |
| `orders-e2e` | The service reports healthy while Kafka and the database are reachable | PASS | 55 ms |
| `orders-e2e` | Invalid order (negative amount): 400, never reaches Kafka | PASS | 2073 ms |
| `orders-e2e` | Invalid order (zero amount): 400, never reaches Kafka | PASS | 2071 ms |
| `orders-e2e` | Invalid order (blank customer): 400, never reaches Kafka | PASS | 2056 ms |

9 of 9 passed. Scenario time adds up to 13.6 s; Karate reported 17.1 s elapsed.

### Repeated runs

| Run | Karate elapsed | Result | Whole `KarateTest` |
|---|---|---|---|
| 1 (before the fix) | 15.6 s | 9/9 | 25.3 s |
| 2 | 17.4 s | 9/9 | 26.2 s |
| 3 | 16.9 s | 9/9 | not recorded |
| 4 | 17.7 s | 9/9 | not recorded |
| 5 (inside the full build) | 17.1 s | 9/9 | 18.2 s |

Runs 2 to 5 are after the fix, which adds about 1.5 to 2 s in total (see finding F1). "Whole `KarateTest`" includes
starting Kafka, the database and the application; it is shorter inside the full build because the JVM is already warm.

### Can the tests fail? (red-green check)

| # | Deliberate breakage | Expected | Actual |
|---|---|---|---|
| M1 | Expect 2 linking calls instead of 3 (wrong retry count), **before** the fix | Fail | **No failure: 9/9 passed** |
| M1 | Same breakage, **after** the fix | Fail | 8/9: `actual array length is 3` |
| M2 | Expect HTTP 200 instead of 400 for invalid orders | Fail | 6/9: `status code was: 400, expected: 200`, all 3 outline rows |
| M3 | Expect amount 99 instead of 10.5 in the published event | Fail | 8/9: `expected has un-matched keys - [amount]` |

Each breakage was reverted afterwards (`git diff` clean) and the unmodified suite then passed 9/9.

### Full build, to check nothing else was disturbed

`mvn verify`: **53 tests, 0 failures, 0 errors, 3 skipped**, coverage gate met, build success in about 54 s. The 3 skipped
are the Docker-only tests (`KafkaContainerTest`, `PostgresMigrationTest`), which cannot run in this sandbox. Alongside
Karate's 9 scenarios the Cucumber layer ran 14 and the JUnit tests the rest.

## Findings

**F1. The test helper let wrong counts through (fixed).** `awaitLinkingCalls` returned the moment the call count reached the
target, so "exactly N calls" could pass just before call N+1 arrived. M1 exposed it: a wrong retry count passed, and a
bug that linked an order twice would have passed the "exactly once" scenarios too. Fix: pause 500 ms after reaching the
target (retries are 200 ms apart), in `KafkaSupport.java`. After it, M1 fails with the true count.
This is the main reason to red-test a suite: all 9 scenarios were green and one of them could not detect the thing it
claimed to check.

**F2. Karate needs `karate-config.js` at the classpath root.** First run failed instantly (`"baseUrl" is not defined`)
because the file sat in the `karate/` folder. Moved; documented.

**F3. `--name` did not filter.** `-Dkarate.options="... --name 'healthy'"` still ran the whole feature (5 scenarios). Filtering by
feature file works (`-Dkarate.options="classpath:karate/kafka-consumer.feature"` ran exactly 4 scenarios). Tags were not tried.

**F4. Proving "nothing was published" is slow.** The three invalid-order scenarios each wait a 2 s window, about 6.2 s of
the 13.6 s scenario time (45%). The wait is the proof of absence; it can be shortened at the cost of a weaker proof.

**F5. The real cost of Karate here is Kafka access.** The features are short (98 lines) but need `KafkaSupport.java`
(157 lines) to publish, read topics, read the dead-letter topic and list linking calls, because I found no
built-in Kafka support in Karate 1.4.1 (I did not search other Karate modules or plugins). Anyone adding scenarios mostly reuses those helpers.

**F6. Karate is not the Cucumber runtime.** "Java + Cucumber" is only partly accurate: Karate uses Gherkin-like syntax but
its own engine. This repository runs both, so the comparison is on real code (table in the guide).

## What this does not show

- **Not run against real Kafka or a real Schema Registry.** The PoC uses an embedded broker and an in-memory registry.
  Real-broker behaviour is covered by the Docker-based JUnit tests in CI, not by Karate.
- **Not run against a deployed environment.** `baseUrl` is the local random port; pointing Karate at another environment
  is not built.
- **CI has not run this change yet.** Results above are from this sandbox only. (Earlier, the Karate suite itself did pass in CI
  on PR #15.)
- **`failThenSucceed` and `delay` are documented in the guide but no Karate scenario uses them yet.** They are exercised by the JUnit
  and Cucumber layers, and use the same call mechanism as `failWith`, which Karate scenarios do use.
- **Single-threaded only.** Scenarios share one fake linking service.
- Timings are from one machine and a handful of runs; treat them as order-of-magnitude.

## Recommendation

For the stated goal (validate producer and consumer integration and the event flow end to end), the PoC works and is
small. Keep Karate if the team values HTTP-level checks with built-in JSON matching and does not mind owning a Kafka
helper; keep Cucumber if business-readable scenarios matter more. Until that is decided both can stay (D8). Next steps
if Karate is kept: run it against the Docker-based environment in CI, and add a scenario that uses `failThenSucceed`.
