# Compatibility Policy

Flink CDC connector APIs are `PublicEvolving`, so minor Flink CDC upgrades can contain breaking API changes.

Yak connectors therefore use an explicit compatibility matrix instead of claiming unbounded compatibility.

## Supported line

| Yak Connector | Flink CDC | Flink | Java |
|---|---|---|---|
| `0.1.x` | `3.6.x` | `1.20.x` | 11+ |

The initial build pins:

- Flink CDC `3.6.0`
- Flink `1.20.3`

## Upgrade policy

For each new Flink CDC minor line:

1. run compile/unit tests against the new line;
2. run MySQL -> MySQL and MySQL -> PostgreSQL integration tests;
3. verify factory discovery and schema evolution;
4. publish compatibility only after those tests pass.

Flink 2.2 support should be added through a small runtime compatibility layer if the Sink API requires it. Dialects should not depend directly on Flink runtime APIs.
