# Yak CDC Runtime (Phase 0)

This directory is an **independent runtime project**: it is not a Maven module, is not shaded into
the Connector bundle, and has its own image/build contract. It may be split into a separate local
Git repository when release ownership is established; Phase 0 deliberately creates no remote
repository and changes nothing in Yak Ops.

## Fixed stack and layout

The immutable image assembles Java 11, Flink 1.20.5, Flink CDC 3.6.0, the Yak JDBC Connector,
MySQL Connector/J 8.4.0, and PostgreSQL JDBC 42.7.7. Flink and Flink CDC remain `provided` in the
Connector build. `runtime-manifest.json` is both image metadata and the response of `capabilities`.

```text
Yak Ops (future; API client only)
       | HTTP
       v
Phase-0 gateway ----> one local Flink CDC submission
       |                    |
       |                    +--> MySQL source
       +--> ephemeral env credentials --> MySQL/PostgreSQL Yak JDBC sink
```

There is deliberately no arbitrary JAR upload, online Connector install, multi-Flink selection,
cluster registry, or Yak Ops synchronization module.

## Gateway contract

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | process liveness |
| `GET` | `/capabilities` | exact versions, connectors, DDL and delivery semantics |
| `POST` | `/validate` | validate an allow-listed Pipeline YAML without saving it |
| `POST` | `/deploy` | launch the one permitted local Pipeline |
| `GET` | `/status` | current gateway-owned process state |
| `POST` | `/stop` | stop the current process |
| `GET` | `/logs` | bounded runtime log access with credential redaction |

`validate` and `deploy` accept `text/yaml`. Source must be `mysql`, sink must be `yak-jdbc`, and
password values must be environment references such as `password: ${ENV:SOURCE_PASSWORD}`. The
gateway persists references only (never resolved values), removes the launch definition on stop,
and never returns the submitted definition. Flink resolves the references from the process
environment; operators must inject them through their secret manager. All API and manifest output calls the delivery contract
**at-least-once**.

The interface is intentionally minimal rather than a production control plane. Authentication,
TLS/mTLS, authorization, audit storage, HA gateway state, log retention/rotation, Flink REST job-ID
reconciliation, externalized checkpoint lifecycle, secret-manager integration, resource quotas,
network policy, and observability are production risks left for later phases.

## Build and verification

Build the Connector first, then provide the official release SHA-512 values:

```bash
mvn -DskipTests package
docker build -f yak-cdc-runtime/Dockerfile \
  --build-arg FLINK_SHA512="$FLINK_SHA512" \
  --build-arg FLINK_CDC_SHA512="$FLINK_CDC_SHA512" \
  -t yak-cdc-runtime:0.1.0-phase0 .
```

Run `scripts/smoke-test.sh`; it obtains the signed release checksum files from the Apache archive
unless those two values are exported explicitly. The smoke verifies Connector isolation,
builds the image, starts it as the non-root user, and checks health, capabilities, validation,
credential rejection, status, logs, and stop. Full database correctness and checkpoint/recovery are
covered by the Maven E2E suite.
