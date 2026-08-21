# Compatibility Policy

Flink CDC connector APIs are `PublicEvolving`, so minor Flink CDC upgrades can contain breaking API changes.

Yak connectors therefore use an explicit compatibility matrix instead of claiming unbounded compatibility.

## Supported line

| Yak Connector | Flink CDC release | Maven artifact line | Flink | Java |
|---|---|---|---|---|
| `0.1.x` | `3.6.0` | `3.6.0-1.20` | `1.20.5` | 11 |

The initial build pins:

- Flink CDC release `3.6.0`
- Flink CDC Maven artifacts `3.6.0-1.20`
- Flink `1.20.5`

Flink CDC 3.6 publishes runtime-facing artifacts with the Flink major version suffix, so the release version and Maven artifact version are deliberately tracked separately.

## Upgrade policy

For each new Flink CDC minor line:

1. run compile/unit tests against the new line;
2. run MySQL -> MySQL and MySQL -> PostgreSQL integration tests;
3. verify factory discovery and schema evolution;
4. verify the final bundle does not contain Flink/Flink CDC classes;
5. publish compatibility only after those tests pass.

Flink 2.2 support should be added through a small runtime compatibility layer if the Sink API requires it. Dialects should not depend directly on Flink runtime APIs.
