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

The MVP uses row-wise JDBC writes and provides **at-least-once** delivery. Tables with primary keys are replay-safe because writes use database-native upsert semantics. Batch writing and 2PC/exactly-once are intentionally left for later milestones.

## Compatibility

| Yak Connector | Flink CDC | Flink | Java | Status |
|---|---|---|---|---|
| `0.1.x` | `3.6.x` | `1.20.x` | 11+ | MVP target |
| planned | `3.6.x` | `2.2.x` | 11+ | compatibility adapter |

See [docs/compatibility.md](docs/compatibility.md).

## Modules

```text
yak-flink-cdc-connectors
├── yak-flink-cdc-connector-jdbc-core
├── yak-flink-cdc-connector-jdbc-mysql
├── yak-flink-cdc-connector-jdbc-postgres
└── yak-flink-cdc-connector-jdbc
```

`yak-flink-cdc-connector-jdbc` is the distribution bundle. It packages the Yak JDBC core and dialect plugins into one JAR while deliberately excluding Flink, Flink CDC, and JDBC drivers.

## Build

```bash
mvn clean verify
```

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
5. Prefer upstream Apache capabilities when they become stable; keep Yak-specific dialects as extensions.

See [docs/architecture.md](docs/architecture.md).

## License

Apache License 2.0.
