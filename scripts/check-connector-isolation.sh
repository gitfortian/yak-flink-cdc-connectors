#!/usr/bin/env bash
set -euo pipefail
jar_file="${1:-$(find yak-flink-cdc-connector-jdbc/target -maxdepth 1 -name 'yak-flink-cdc-connector-jdbc-*.jar' ! -name 'original-*' | head -n 1)}"
test -n "$jar_file"
entries=$(jar tf "$jar_file")
printf '%s\n' "$entries" | grep -q 'META-INF/services/org.apache.flink.cdc.common.factories.Factory'
if printf '%s\n' "$entries" | grep -Eq '^(org/apache/flink/|com/mysql/|org/postgresql/)'; then
  echo "ERROR: connector bundle contains runtime or JDBC driver classes" >&2
  exit 1
fi
echo "Connector isolation verified: $jar_file"
