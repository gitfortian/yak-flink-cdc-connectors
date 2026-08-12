# Production E2E Test Specification

This module defines the production-oriented end-to-end verification contract for Yak Flink CDC connectors.

The purpose is not to test isolated SQL builders. The purpose is to prove that a connector works when executed through the **real Apache Flink CDC Pipeline runtime**, against **real database processes**, using the **same Factory SPI and distribution artifact** that production deployment uses.

## 1. Test topology

The baseline production gate is:

```text
MySQL 8
  │
  │ snapshot + binlog CDC
  ▼
Apache Flink CDC 3.6.x / Flink 1.20.x
  │
  │ DataSinkFactory(type = yak-jdbc)
  ▼
yak-flink-cdc-connector-jdbc-<version>.jar
  │
  │ PostgreSQL dialect inside the shaded bundle
  ▼
PostgreSQL 16
```

Databases run in Testcontainers. The Flink CDC job runs with the real `FlinkPipelineComposer`; Source and Sink are discovered and constructed through Flink CDC connector factories.

The E2E runtime depends on the **final shaded distribution bundle**, not directly on `jdbc-core` or dialect modules. Maven exclusions prevent those internal modules from also appearing as separate runtime JARs. This mirrors the documented production deployment model and avoids testing an artifact topology that users never deploy.

## 2. Mandatory rules

Every production E2E case MUST follow these rules:

1. **No mocked database.** Source and target must be real database containers.
2. **No direct Writer invocation.** Do not call `YakJdbcWriter` directly as a substitute for Pipeline execution.
3. **Use Flink CDC Factory SPI.** The sink must be selected through `type = yak-jdbc` / `SinkDef("yak-jdbc", ...)`.
4. **Use the final distribution bundle.** E2E must not place Yak core and dialect modules on the runtime classpath as separate connector JARs.
5. **Do not force Yak packages parent-first.** The test must preserve Flink's normal user-code classloader behavior so serialization/classloader bugs are caught before release.
6. **Validate final data, not only counts.** Row count equality alone is insufficient.
7. **Validate schema changes in the target database.** A schema event is successful only when target metadata and subsequent data writes both reflect it.
8. **Use bounded waits.** Every asynchronous assertion must have a timeout and fail with useful diagnostics.
9. **Inject at least one recoverable failure.** A production gate must prove that new CDC records still arrive after the failure.
10. **Tests must be deterministic.** Avoid sleeps as synchronization. Poll observable database state instead.
11. **Always clean up containers.** Tests must not depend on state left by previous runs.
12. **Never log credentials or row payloads containing secrets.**

## 3. Classloader and serialization contract

Flink serializes connector objects between the client, JobMaster and TaskManagers. Production tests must therefore verify the real classloader boundary.

Connector runtime rules:

- serializable `DataSink`, Flink `Sink` and `MetadataApplier` objects must carry stable configuration/state, not concrete plugin implementation instances;
- concrete `JdbcDialect` implementations are runtime resources and must be resolved inside the runtime classloader boundary;
- the runtime connector artifact must contain both the `JdbcDialect` SPI and its providers so a single user-code classloader owns their Java type identity;
- `ServiceLoader` must load dialect factories from that runtime bundle;
- E2E tests must not hide type-identity bugs by adding `io.yak.*` to Flink parent-first loader patterns.

A `ClassCastException` or `ServiceConfigurationError ... not a subtype` involving Yak classes is treated as a production-blocking classloader/artifact-layout defect.

## 4. Baseline scenario

`MySqlToPostgresPipelineITCase` is the initial production gate and must cover the following sequence in one real Pipeline:

### Phase A — snapshot

- create the source table and seed rows before starting Flink CDC;
- start the Pipeline;
- verify PostgreSQL table creation;
- verify every seed row by primary key and values.

### Phase B — incremental DML

Apply source-side operations after snapshot synchronization:

- `INSERT`;
- `UPDATE`;
- `DELETE`.

The target must converge to exactly the expected row set. The assertion must verify row identity and values, not only `COUNT(*)`.

### Phase C — schema evolution

Execute an `ADD COLUMN` in MySQL, then write data using the new column.

The test must verify:

- the new PostgreSQL column exists;
- subsequent CDC writes use the evolved schema successfully;
- existing expected rows contain the expected new-column values.

### Phase D — recoverable target connection failure

Terminate the PostgreSQL backend connection used by the sink, then produce another source-side CDC record.

The connector passes only when it reconnects and the new record appears in PostgreSQL.

This phase verifies the current MVP's connection-level recovery behavior. A later recovery suite will add checkpoint/savepoint and TaskManager/process restart scenarios as a separate production gate.

## 5. Data correctness contract

For primary-key tables, target verification should compare a canonical ordered representation, for example:

```text
1|Alice|active
2|Bobby|active
4|Dave|active
```

Do not treat these as sufficient by themselves:

```text
source count == target count
MAX(id) is equal
at least one expected row exists
```

Those checks can pass while UPDATE/DELETE handling is wrong.

## 6. Timeout policy

Default asynchronous convergence timeout: **120 seconds**.

Recommended polling interval: **500 ms**.

A timeout failure should include:

- the phase being awaited;
- current target rows or target metadata when safe;
- any Pipeline execution failure captured by the test harness.

Do not solve flakes by adding arbitrary `Thread.sleep(...)` calls.

## 7. CI policy

Normal connector build:

```bash
mvn clean verify
```

Docker E2E is intentionally skipped by default.

Production E2E:

```bash
mvn -Pe2e -pl yak-flink-cdc-connector-e2e-tests -am verify
```

GitHub Actions runs this as a dedicated Java 11 job with Docker available. A PR is not production-safe when the E2E job is red, even if compile/unit jobs are green.

## 8. Adding new E2E cases

New cases should be named `*ITCase.java` and answer one concrete production-risk question.

Good examples:

- `MySqlToMySqlPipelineITCase`
- `PostgresConnectionRecoveryITCase`
- `SchemaEvolutionIdempotencyITCase`
- `CheckpointRecoveryITCase`

Avoid broad tests that combine unrelated failure modes and become impossible to diagnose.

## 9. Production gate roadmap

Current gate:

- snapshot;
- insert/update/delete;
- `ADD COLUMN` schema evolution;
- PostgreSQL connection termination and reconnect;
- exact target-state comparison;
- Flink client/JobMaster/TaskManager classloader serialization boundary;
- final distribution-bundle artifact topology.

Next recovery gate:

- checkpoint enabled;
- TaskManager/job failure after completed checkpoint;
- restore from checkpoint;
- replay/idempotency verification;
- schema cache recovery verification.

Next performance gate:

- large snapshot dataset;
- sustained incremental throughput;
- backpressure observation;
- batch writer once batching is implemented.

The E2E specification evolves with production semantics. New connector features are not considered production-ready until their failure mode has a deterministic E2E test.
