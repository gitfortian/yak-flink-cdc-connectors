# Architecture

## Goals

Yak Flink CDC Connectors is an independent plugin project. Apache Flink CDC is the runtime host, not a source-code dependency that is forked and republished.

```text
Apache Flink CDC
       │
       │ public Pipeline connector SPI
       ▼
yak-flink-cdc-connector-jdbc-core
       │
       ├── JdbcDataSinkFactory (type = yak-jdbc)
       ├── DataSink / Flink Sink adapter
       ├── schema cache
       ├── JDBC writer
       └── MetadataApplier
                │
                ▼
        JdbcDialectFactory SPI
          ┌─────┴─────┐
          ▼           ▼
        MySQL      PostgreSQL
```

## Why `yak-jdbc`

Apache Flink CDC has had community work toward a generic JDBC Pipeline sink. If an official connector eventually uses `type: jdbc`, a third-party connector with the same factory identifier would be ambiguous.

Yak therefore owns the stable identifier:

```yaml
sink:
  type: yak-jdbc
```

## Dependency boundary

`flink-cdc-common` and `flink-core` use Maven `provided` scope. The bundle must not package Flink/Flink CDC classes.

JDBC drivers are also external runtime dependencies. This prevents the connector from forcing driver versions on a Flink CDC installation.

## Dialect SPI

Adding a database should normally require a dialect module, not a new Pipeline connector implementation.

A dialect owns:

- identifier quoting
- Flink CDC type -> database type mapping
- create/alter/drop DDL syntax
- insert/upsert/delete SQL

Dialect factories are discovered with Java `ServiceLoader`.

## Delivery semantics

The MVP uses synchronous row-wise JDBC writes:

- primary-key table: `INSERT` / `UPDATE` / `REPLACE` -> database-native upsert
- primary-key table: `DELETE` -> delete by primary key
- no-primary-key table: inserts are allowed
- no-primary-key table: update/delete fail fast because replay-safe semantics cannot be guaranteed

This provides at-least-once delivery. Primary-key upserts make common CDC replays idempotent.

Future milestones can add batching and checkpoint-aware 2PC without changing the dialect SPI.

## Schema evolution

Flink CDC sends schema events through the sink stream while separately invoking `MetadataApplier`.

The writer maintains an in-memory schema map so it can serialize `RecordData` using the current column order/types. The metadata applier executes DDL against the target database.

Supported MVP schema events:

- `CREATE_TABLE`
- `ADD_COLUMN`
- `RENAME_COLUMN`
- `DROP_COLUMN`
- `ALTER_COLUMN_TYPE`
- `TRUNCATE_TABLE`
- `DROP_TABLE`

Table comments are not handled in the MVP.
