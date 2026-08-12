# Production E2E Test Specification

This module defines the production-oriented end-to-end verification contract for Yak Flink CDC connectors.

The purpose is not to test isolated SQL builders. The purpose is to prove that a connector works when executed through the **real Apache Flink CDC Pipeline runtime**, against **real database processes**, using the **same Factory SPI** that production deployment uses.

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
Yak JDBC Sink / PostgreSQL dialect
  │
  ▼
PostgreSQL 16
```

Databases run in Testcontainers. The Flink CDC job runs with the real `FlinkPipelineComposer`; Source and Sink are discovered and constructed through Flink CDC connector factories.

## 2. Mandatory rules

Every production E2E case MUST follow these rules:

1. **No mocked database.** Source and target must be real database containers.
2. **No direct Writer invocation.** Do not call `YakJdbcWriter` directly as a substitute for Pipeline execution.
3. **Use Flink CDC Factory SPI.** The sink must be selected through `type = yak-jdbc` / `SinkDef("yak-jdbc", ...)`.
4. **Validate final data, not only counts.** Row count equality alone is insufficient.
5. **Validate schema changes in the target database.** A schema event is successful only when target metadata and subsequent data writes both reflect it.
6. **Use bounded waits.** Every asynchronous assertion must have a timeout and fail with useful diagnostics.
7. **Inject at least one recoverable failure.** A production gate must prove that new CDC records still arrive after the failure.
8. **Tests must be deterministic.** Avoid sleeps as synchronization. Poll observable database state instead.
9. **Always clean up containers.** Tests must not depend on state left by previous runs.
10. **Never log credentials or row payloads containing secrets.**

## 3. Baseline scenario

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

## 4. Data correctness contract

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

## 5. Timeout policy

Default asynchronous convergence timeout: **120 seconds**.

Recommended polling interval: **500 ms**.

A timeout failure should include:

- the phase being awaited;
- current target rows or target metadata when safe;
- any Pipeline execution failure captured by the test harness.

Do not solve flakes by adding arbitrary `Thread.sleep(...)` calls.

## 6. CI policy

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

## 7. Adding new E2E cases

New cases should be named `*ITCase.java` and answer one concrete production-risk question.

Good examples:

- `MySqlToMySqlPipelineITCase`
- `PostgresConnectionRecoveryITCase`
- `SchemaEvolutionIdempotencyITCase`
- `CheckpointRecoveryITCase`

Avoid broad tests that combine unrelated failure modes and become impossible to diagnose.

## 8. Production gate roadmap

Current gate:

- snapshot;
- insert/update/delete;
- `ADD COLUMN` schema evolution;
- PostgreSQL connection termination and reconnect;
- exact target-state comparison.

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
