package io.yak.flink.cdc.connectors.jdbc.runtime;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

/** One executable and independently reconcilable unit inside a schema-change event. */
public final class SchemaChangeStep {
    private final String description;
    private final String sql;
    private final StateProbe stateProbe;

    private SchemaChangeStep(String description, String sql, StateProbe stateProbe) {
        this.description = Objects.requireNonNull(description, "description");
        this.sql = Objects.requireNonNull(sql, "sql");
        this.stateProbe = stateProbe;
    }

    public static SchemaChangeStep reconciled(
            String description, String sql, StateProbe stateProbe) {
        return new SchemaChangeStep(
                description, sql, Objects.requireNonNull(stateProbe, "stateProbe"));
    }

    public static SchemaChangeStep replaySafe(String description, String sql) {
        return new SchemaChangeStep(description, sql, null);
    }

    public String getDescription() {
        return description;
    }

    public String getSql() {
        return sql;
    }

    public boolean isStateReconciled() {
        return stateProbe != null;
    }

    public SchemaChangeInspectionResult inspect(Connection connection) throws SQLException {
        if (stateProbe == null) {
            return SchemaChangeInspectionResult.notApplied(
                    "step has no observable metadata post-condition and must execute");
        }
        return stateProbe.inspect(connection);
    }

    @FunctionalInterface
    public interface StateProbe {
        SchemaChangeInspectionResult inspect(Connection connection) throws SQLException;
    }
}
