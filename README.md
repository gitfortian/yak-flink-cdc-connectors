# Yak Flink CDC Connectors

Independent Apache Flink CDC Pipeline sink connectors maintained by the Yak project.

The project intentionally **does not fork Apache Flink CDC**. It uses Flink CDC's public connector SPI and keeps its own release/compatibility lifecycle, so Yak connectors can evolve without carrying a patched Flink CDC distribution.

## MVP

The first MVP provides a generic JDBC Pipeline sink with two dialects:

- MySQL
- PostgreSQL

Supported data operations:

- `INSERT`
- `UPDATE` / `REPLACE` as idempotent upsert when a primary key exists
- `DELETE` by primary key

Supported schema evolution:

- create table
- add column
- rename column
- drop column
- alter column type
- truncate table
- drop table

The connector provides **at-least-once** delivery. Tables with primary keys are replay-safe because writes use database-native upsert/delete semantics. Strict 2PC/exactly-once is intentionally left for a later milestone.

## JDBC batch writing

Production writes are buffered and flushed through JDBC `PreparedStatement.addBatch()` / `executeBatch()` instead of issuing one autocommit `executeUpdate()` per CDC record.

One flush is one JDBC transaction:

```text
CDC events
   │
   ▼
ordered in-memory buffer
   │
   ├─ adjacent same SQL ──► one JDBC batch segment
   │
   ▼
executeBatch() segment(s)
   │
   ▼
commit once
```

The buffer preserves the original event order. Only **adjacent** records using the same SQL are coalesced. The connector never globally groups all upserts/deletes because doing so could reorder a stream such as `UPSERT -> DELETE -> UPSERT` and change the final database state.

A pending batch is flushed when any of these conditions is reached:

- `batch-size` records are buffered;
- `flush-interval-ms` processing-time timer fires;
- Flink calls `SinkWriter.flush(...)` for a checkpoint or end-of-input;
- Flink CDC sends a `FlushEvent` before schema evolution;
- the writer closes.

Prepared statements are cached across normal flushes. A schema-change boundary flushes pending data and invalidates the statement cache before the writer adopts the new schema.

If a batch fails, the connector rolls back the transaction, recreates the JDBC connection and retries the full ordered batch up to `max-retries`. An ambiguous commit can therefore replay a batch. Primary-key upserts/deletes remain idempotent under the connector's at-least-once contract; append-only tables without a primary key still carry duplicate risk after replay.

Batch options:

| Option | Default | Meaning |
|---|---:|---|
| `batch-size` | `1000` | maximum buffered records before an immediate flush |
| `flush-interval-ms` | `2000` | processing-time flush interval; `0` disables timed flush |
| `max-retries` | `3` | maximum retry count after a JDBC batch failure |

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
  flush-interval-ms: 2000

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
7. Schema evolution must flush old-schema DML before DDL and invalidate cached statements afterward.
8. Prefer upstream Apache capabilities when they become stable; keep Yak-specific dialects as extensions.

See [docs/architecture.md](docs/architecture.md).

## License

Apache License 2.0.
