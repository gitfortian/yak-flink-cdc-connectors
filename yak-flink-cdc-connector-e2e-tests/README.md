# Production E2E Test Specification

This module defines the production-oriented end-to-end verification contract for Yak Flink CDC connectors.

The purpose is not to test isolated SQL builders. The purpose is to prove that a connector works when executed through the **real Apache Flink CDC Pipeline runtime**, against **real database processes**, using the **same Factory SPI and distribution artifact** that production deployment uses.

## 1. Test topology

The baseline production gates are MySQL -> PostgreSQL and MySQL -> MySQL. The former exercises
checkpoint/restart, connection recovery, DDL replay, and full schema-cache recovery; the latter is
a real Pipeline/Factory-SPI sink test covering snapshot, incremental DML, ordered batching, and DDL.

The cross-database topology is:

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
9. **Inject recoverable failures at two levels.** Verify both connector-level JDBC reconnection and Flink-level checkpoint/restart recovery.
10. **Wait for an observable completed checkpoint before failover.** A restart test that cannot prove a checkpoint existed is not a checkpoint-recovery test.
11. **Tests must be deterministic.** Avoid sleeps as synchronization. Poll observable database state, checkpoint files, or Flink job status instead.
12. **Always clean up containers and checkpoint state.** Tests must not depend on state left by previous runs.
13. **Never log credentials or row payloads containing secrets.**
14. **Schema cache recovery is connector-owned.** A recovered JDBC writer must not depend on Flink CDC replaying `CreateTableEvent` before the first post-recovery DML event.
15. **Ambiguous recovered schemas must fail fast.** Rescaling or restore must never silently choose one of two conflicting schema states for the same table.
16. **DDL replay must be state-based, not exception-based.** Replaying the same schema event is successful only when target metadata proves the requested state is already present.
17. **Ambiguous DDL acknowledgement loss must be reconciled.** After a JDBC failure, reconnect and inspect target metadata before deciding whether to retry or fail.
18. **Same-name schema conflicts must never be hidden by `IF EXISTS` / `IF NOT EXISTS`.** Existing tables and columns must be structurally compatible with the requested event.
19. **Permanent DDL failures must fail fast.** Syntax, permission, unsupported-operation and structural-conflict errors must not consume the transient retry budget.

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

`MySqlToPostgresPipelineITCase` is the initial production gate and must cover the following sequence in one real Pipeline.

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

### Phase D — connector-level JDBC recovery

Terminate the PostgreSQL backend connection used by the sink while PostgreSQL remains healthy, then produce another source-side CDC record.

The connector passes only when it reconnects and the new record appears in PostgreSQL without requiring a Flink job restart.

### Phase E — checkpoint and Flink restart recovery

The test must then prove recovery at the Flink runtime level:

1. enable periodic checkpoints and a bounded fixed-delay restart strategy;
2. wait until at least two newer completed checkpoints are observable after the last verified target state;
3. pause the PostgreSQL container so the database becomes unavailable while its container identity, mapped port and data remain intact;
4. insert a new MySQL CDC row while the target is unavailable;
5. verify the Flink job enters `RESTARTING`;
6. unpause PostgreSQL and wait for JDBC readiness;
7. verify the Flink job returns to `RUNNING`;
8. verify the post-checkpoint CDC row reaches PostgreSQL;
9. verify the complete final target state remains exact and duplicate-free.

Pausing is deliberate: this phase tests **target unavailability and Flink recovery**, not Docker container recreation or port-remapping behavior.

This phase validates the interaction of source checkpoint state, at-least-once replay, PK upsert idempotency, sink recreation and Writer schema-cache restoration after a real task failure.

## 5. Schema cache recovery contract

The JDBC writer requires the latest table schema to bind `DataChangeEvent` values correctly. Keeping that schema only in an in-memory `Map<TableId, Schema>` is not production-safe because a TaskManager restart creates a new writer instance.

Production behavior is therefore defined as:

```text
SchemaChangeEvent
       │
       ▼
Writer schema cache
       │
       │ snapshotState(checkpointId)
       ▼
YakJdbcWriterState
       │
       │ versioned serializer
       ▼
Flink checkpoint / savepoint
       │
       │ restoreWriter(...)
       ▼
Recovered schema cache
       │
       ▼
first post-recovery DataChangeEvent
```

Required invariants:

- the latest known `TableId -> Schema` map is part of writer checkpoint state;
- evolved columns present before a completed checkpoint must be present after restore;
- the first post-recovery DML event must be writable even if no new `CreateTableEvent` has arrived;
- a dropped table must not remain in recovered cache state after a checkpoint containing its drop event;
- duplicate identical states from multiple recovered subtasks may be de-duplicated;
- conflicting states for the same table must fail recovery with a clear error;
- state serialization must be explicitly versioned;
- changing the writer-state byte format requires a serializer version/migration decision rather than an implicit incompatible change;
- state deserialization must respect the Flink task context classloader.

The initial state serializer version is `1` and is scoped to the supported Flink CDC 3.6.x artifact line. Compatibility with a future Flink CDC line must be proven before claiming old checkpoint/savepoint restore compatibility.

Unit tests must cover at least:

- state serializer round-trip;
- evolved schema preservation;
- duplicate-state merge after rescaling;
- conflicting schema rejection;
- unsupported serializer-version rejection;
- empty recovery state.

The real E2E must continue to pass checkpoint/restart after schema evolution. Unit tests prove the state representation; E2E proves that it survives the actual Flink runtime/classloader boundary.

## 5.1. DDL application safety contract

Schema evolution is an at-least-once operation from the connector's point of view. A database can commit DDL before the JDBC client learns that it succeeded, so correctness must not depend on replaying the exact SQL and interpreting the second exception.

Production behavior is defined as:

```text
SchemaChangeEvent
       │
       ▼
inspect target JDBC metadata
       │
       ├─ requested state already present ──────────► success
       │
       ├─ incompatible same-name state ─────────────► fail fast
       │
       ▼
execute DDL
       │
       ├─ success ──────────────────────────────────► success
       │
       └─ SQLException
              │
              ▼
       reconnect + inspect target metadata
              │
              ├─ requested state now present ───────► success
              ├─ incompatible target state ─────────► fail fast
              └─ state not applied
                       │
                       ├─ transient/recoverable ─────► bounded retry
                       └─ permanent ─────────────────► fail fast
```

The reconciliation state has three meanings:

- `APPLIED`: target metadata already represents the requested schema event; replay is successful and no SQL is executed again;
- `NOT_APPLIED`: target metadata is safe for the DDL to run or retry;
- `CONFLICT`: target metadata cannot be explained as either pre-event or post-event state and requires operator intervention.

Required invariants:

- `CREATE TABLE` replay validates target columns, JDBC-compatible types, nullability and primary keys instead of trusting `IF NOT EXISTS`;
- `ADD COLUMN` replay succeeds only when the same-name column has a compatible definition;
- `RENAME COLUMN` replay treats `old missing + new present` as applied, while `old present + new present` and `old missing + new missing` are conflicts;
- `DROP COLUMN` replay succeeds when the target column is already absent but does not silently accept a missing target table;
- `ALTER COLUMN TYPE` replay succeeds only when target JDBC metadata is compatible with the requested new type;
- `DROP TABLE` replay succeeds when the target table is already absent;
- `TRUNCATE TABLE` may be replayed because the schema operator blocks subsequent data events while applying the schema event;
- retryable failures are limited to recoverable/transient JDBC exceptions and SQLState classes representing connection failure or transaction rollback;
- retry delay is bounded and permanent SQL failures do not spin through the retry budget;
- dialect-specific metadata comparison must cover at least type family, length where relevant, precision/scale where relevant, and nullability;
- an external dialect that cannot prove metadata compatibility must fail explicitly rather than guessing that an existing object is safe.

`PostgresDdlReplayITCase` is the focused real-database gate. It must prove:

- CREATE / ADD / RENAME / ALTER TYPE / DROP COLUMN / DROP TABLE can each be replayed against PostgreSQL;
- incompatible same-name tables and columns fail fast;
- a real PostgreSQL DDL can commit successfully while a test JDBC wrapper deliberately discards the acknowledgement and throws `SQLRecoverableException`;
- the connector reconnects after that simulated lost acknowledgement, observes the committed target state, and reports schema evolution success without re-applying a conflicting DDL.

This focused MetadataApplier IT complements the full Pipeline E2E: the Pipeline test protects runtime integration, while the replay IT deterministically exercises DDL ambiguity that is difficult to inject at an exact network packet boundary.

## 6. Data correctness contract

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

Those checks can pass while UPDATE/DELETE handling, replay, or recovery is wrong.

After checkpoint recovery, the assertion must also prove that replay did not introduce duplicate logical rows.

## 7. Checkpoint policy

The baseline E2E enables Flink checkpointing with one concurrent checkpoint at a time.

Before failure injection, record the latest completed checkpoint id and wait for **two newer completed checkpoints**. Waiting for two generations prevents a checkpoint that started before the last verified CDC event from being mistaken for a recovery point that contains that event.

Completed checkpoints are observed through their `_metadata` files in a dedicated temporary checkpoint directory. Do not replace this with a fixed sleep.

## 8. Timeout policy

Default asynchronous convergence timeout: **120 seconds**.

Recommended polling interval: **500 ms**.

A timeout failure should include:

- the phase being awaited;
- current Flink job status when relevant;
- current target rows or target metadata when safe;
- any Pipeline execution failure captured by the test harness.

Do not solve flakes by adding arbitrary `Thread.sleep(...)` calls as synchronization barriers.

## 9. CI policy

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

## 10. Adding new E2E cases

New cases should be named `*ITCase.java` and answer one concrete production-risk question.

Good examples:

- `MySqlToMySqlPipelineITCase`
- `SchemaEvolutionIdempotencyITCase`
- `JobManagerFailoverITCase`
- `LargeSnapshotITCase`

Avoid broad tests that combine unrelated database-specific edge cases and become impossible to diagnose. The baseline MySQL-to-PostgreSQL test intentionally combines only the minimum lifecycle that every production JDBC sink must survive.

## 11. Production gate roadmap

Current gate:

- snapshot;
- insert/update/delete;
- `ADD COLUMN` schema evolution;
- PostgreSQL connection termination and connector reconnect;
- completed checkpoint observation;
- target outage causing a real Flink task failure;
- Flink `RESTARTING -> RUNNING` recovery;
- post-checkpoint CDC replay and exact final-state verification;
- checkpointed Writer schema-cache restoration after restart;
- rescale-state conflict detection;
- versioned writer-state serialization;
- replay-safe JDBC DDL reconciliation against target metadata;
- same-name schema conflict detection;
- ambiguous committed-DDL acknowledgement-loss recovery;
- transient/permanent DDL retry classification and bounded backoff;
- Flink client/JobMaster/TaskManager classloader serialization boundary;
- final distribution-bundle artifact topology.

Next reliability gates:

- explicit JobManager leadership failover;
- repeated restart loops and bounded retry behavior;
- no-primary-key policy tests.

Next performance gate:

- large snapshot dataset;
- sustained incremental throughput;
- backpressure observation;
- batch writer once batching is implemented.

The E2E specification evolves with production semantics. New connector features are not considered production-ready until their failure mode has a deterministic E2E test.
