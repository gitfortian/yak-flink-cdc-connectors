# Yak Flink CDC Connectors

Independent Apache Flink CDC Pipeline sink connectors maintained by the Yak project.

The project intentionally **does not fork Apache Flink CDC**. It uses Flink CDC's public connector SPI and keeps its own release/compatibility lifecycle, so Yak connectors can evolve without carrying a patched Flink CDC distribution.

## MVP

The first MVP provides a generic JDBC Pipeline sink with two dialects:

- MySQL
- PostgreSQL

Supported data operations:

- `INSERT` / `UPDATE` / `REPLACE` as database-native upsert when a primary key exists;
- `DELETE` by primary key;
- primary-key-changing `UPDATE` as ordered `DELETE old PK -> UPSERT new PK`;
- no-primary-key `INSERT` only through an explicit unsafe append-only opt-in.

Supported schema evolution:

- create table
- add column
- rename column
- drop column
- alter column type
- truncate table
- drop table

The connector provides **at-least-once** delivery. Strict 2PC/exactly-once is intentionally left for a later milestone. Production defaults therefore make replay safety explicit and conservative rather than pretending at-least-once is exactly-once.

## JDBC batch writing

Production writes are buffered and flushed through JDBC `PreparedStatement.addBatch()` / `executeBatch()` instead of issuing one autocommit `executeUpdate()` per CDC record.

One flush is one JDBC transaction:

```text
CDC events
   │
   ▼
ordered bounded in-memory buffer
   │
   ├─ adjacent same table + SQL ──► one JDBC batch segment
   │
   ▼
executeBatch() segment(s)
   │
   ▼
commit once
```

The buffer preserves the original event order. Only **adjacent** records using the same table and SQL are coalesced. The connector never globally groups all upserts/deletes because doing so could reorder a stream such as `UPSERT -> DELETE -> UPSERT` and change the final database state.

### Flush boundaries

A pending batch is flushed when any of these conditions is reached:

- `batch-size` records are buffered;
- the estimated retained payload reaches `max-batch-bytes`;
- `flush-interval-ms` processing time has elapsed since the first currently pending record;
- Flink calls `SinkWriter.flush(...)` for a checkpoint or end-of-input;
- Flink CDC sends a `FlushEvent` before schema evolution;
- the writer closes.

`batch-size` and `max-batch-bytes` are independent safety limits: the first threshold reached triggers the flush. `max-batch-bytes` is a conservative estimate of JVM-retained batch payload, not a database network packet-size setting. This prevents a snapshot containing large `TEXT`, `VARCHAR` or binary values from retaining an unexpectedly large amount of TaskManager heap merely because the record-count limit has not yet been reached.

If a single record is itself larger than `max-batch-bytes`, the connector first flushes any existing batch, accepts that record as the only buffered record and immediately flushes it. Therefore the writer is bounded to the configured batch budget plus at most one unavoidable input record.

Timed flushing is demand-driven. The writer registers a one-shot processing-time timer only after the first pending record enters an empty buffer. A successful flush cancels that timer, so an idle sink does not continuously wake up to perform empty flushes.

### PreparedStatement cache

Prepared statements are reused across normal flushes. The production cache is:

- keyed by `TableId + SQL`, not only SQL text;
- access-order LRU;
- bounded by `statement-cache-size` (default `128`);
- closed immediately on LRU eviction;
- invalidated only for the changed table at a schema boundary;
- fully invalidated when a JDBC connection is replaced or the writer closes.

This prevents the old unbounded cache from growing with dynamic table counts while avoiding a full re-prepare storm when one table evolves its schema.

### Failure and retry semantics

A failed batch is rolled back as one JDBC transaction. The connector only retries failures that JDBC or SQLState identifies as transient/recoverable, including connection failures (`08`), transaction rollback/serialization failures (`40`), `SQLTransientException` and `SQLRecoverableException`. Retries use bounded backoff, recreate the connection and replay the complete ordered batch.

Permanent SQL failures such as constraint violations (`23`), syntax errors (`42`), type/data errors (`22`) and permission errors fail fast after rollback; they do **not** consume `max-retries` by repeatedly submitting the same invalid batch.

## At-least-once replay safety boundary

The default `replay-safety=strict` mode permits only DML shapes whose replay is idempotent under the connector's current SQL strategy.

| CDC shape | Default `strict` | Replay behavior |
|---|---|---|
| PK `INSERT` | allowed | native upsert, replay-safe |
| PK `UPDATE` / `REPLACE` | allowed | native upsert, replay-safe |
| PK `DELETE` | allowed | delete by primary key, replay-safe |
| UPDATE changes PK | allowed | `DELETE old PK -> UPSERT new PK`, replay-safe |
| no-PK `INSERT` | **rejected** | plain insert can duplicate after ambiguous replay |
| no-PK `UPDATE` / `REPLACE` / `DELETE` | **always rejected** | connector cannot express deterministic replay-safe identity |
| null primary-key value | **rejected** | replay-safe identity is invalid |

A database can commit a transaction and the JDBC client can lose the commit acknowledgement. The connector must then treat the outcome as ambiguous and may replay the whole batch. For primary-key upsert/delete operations the target converges to the same logical state. A plain append-only insert has no such idempotency key and can duplicate.

If duplicate append-only rows are an accepted application-level risk, the operator may explicitly set:

```yaml
replay-safety: allow-append-only
```

That option permits **only no-primary-key INSERT**. It does not make those inserts replay-safe, and it never enables no-primary-key UPDATE/REPLACE/DELETE. The opt-in exists to make the risk deliberate and auditable rather than silently inherited from at-least-once delivery.

Batch and safety options:

| Option | Default | Meaning |
|---|---:|---|
| `batch-size` | `1000` | maximum buffered records before an immediate flush |
| `max-batch-bytes` | `16777216` (16 MiB) | estimated maximum JVM-retained batch payload before an immediate flush |
| `flush-interval-ms` | `2000` | maximum age of a non-empty batch; `0` disables timed flush |
| `statement-cache-size` | `128` | maximum open cached PreparedStatements per writer connection |
| `max-retries` | `3` | maximum retry count for transient/recoverable JDBC batch failures |
| `replay-safety` | `strict` | `strict` or explicit-risk `allow-append-only` |

## Compatibility

| Yak Connector | Flink CDC release | Maven artifact line | Flink | Java | Status |
|---|---|---|---|---|---|
| `0.1.x` | `3.6.x` | `3.6.x-1.20` | `1.20.x` | 11+ | MVP target |
| planned | `3.6.x` | `3.6.x-2.2` | `2.2.x` | 11+ | compatibility adapter |

The initial build uses `flink-cdc-common:3.6.0-1.20` with Flink `1.20.3`.

See [docs/compatibility.md](docs/compatibility.md).

## Modules

```text
yak-flink-cdc-connectors
├── yak-flink-cdc-connector-jdbc-core
├── yak-flink-cdc-connector-jdbc-mysql
├── yak-flink-cdc-connector-jdbc-postgres
├── yak-flink-cdc-connector-jdbc
└── yak-flink-cdc-connector-e2e-tests
```

`yak-flink-cdc-connector-jdbc` is the distribution bundle. It packages the Yak JDBC core and dialect plugins into one JAR while deliberately excluding Flink, Flink CDC, and JDBC drivers.

`yak-flink-cdc-connector-e2e-tests` is a production-oriented verification module. It starts real databases with Testcontainers and executes a real Flink CDC Pipeline through the same Factory SPI used by deployment.

## Build

```bash
mvn clean verify
```

The default build intentionally skips Docker E2E tests so normal development remains fast.

Run the production E2E suite explicitly:

```bash
mvn -Pe2e -pl yak-flink-cdc-connector-e2e-tests -am verify
```

The E2E test specification and contribution rules are documented in [`yak-flink-cdc-connector-e2e-tests/README.md`](yak-flink-cdc-connector-e2e-tests/README.md).

The bundle is generated at:

```text
yak-flink-cdc-connector-jdbc/target/yak-flink-cdc-connector-jdbc-0.1.0-SNAPSHOT.jar
```

Put the bundle JAR and the target database JDBC driver JAR into the Flink CDC `lib` directory.

## Example: MySQL CDC -> PostgreSQL

```yaml
source:
  type: mysql
  hostname: localhost
  port: 3306
  username: root
  password: root
  tables: app_db.\.*

sink:
  type: yak-jdbc
  url: jdbc:postgresql://localhost:5432/ods
  driver: org.postgresql.Driver
  username: postgres
  password: postgres
  dialect: postgres
  batch-size: 1000
  max-batch-bytes: 16777216
  flush-interval-ms: 2000
  statement-cache-size: 128
  replay-safety: strict

pipeline:
  name: mysql-to-postgres
  parallelism: 1
  schema.change.behavior: evolve
```

The connector identifier is deliberately **`yak-jdbc`**, not `jdbc`. This avoids a future SPI identifier collision if Apache Flink CDC ships its own generic JDBC Pipeline connector.

More examples are under [`examples/`](examples/).

## Design principles

1. Flink/Flink CDC dependencies are `provided` and never shaded into the connector.
2. Database dialects are discovered by an internal SPI and can live in separate modules.
3. The public Flink CDC dependency is isolated in the connector core.
4. Compatibility is explicit and CI-tested per supported Flink CDC line.
5. Production E2E tests must use real Flink CDC Pipeline execution and real databases, not mocks.
6. DML batching must preserve CDC event order across tables and operation types.
7. Batch buffering must be bounded by both record count and estimated retained payload.
8. PreparedStatement caching must be bounded, close on eviction and invalidate only the schema-changed table when possible.
9. Permanent SQL failures must fail fast after rollback; only transient/recoverable failures may consume retries.
10. At-least-once safety must be enforced by executable replay policy; unsafe no-PK append semantics require explicit opt-in.
11. Schema evolution must flush old-schema DML before DDL and invalidate cached statements afterward.
12. Prefer upstream Apache capabilities when they become stable; keep Yak-specific dialects as extensions.

See [docs/architecture.md](docs/architecture.md).

## License

Apache License 2.0.
