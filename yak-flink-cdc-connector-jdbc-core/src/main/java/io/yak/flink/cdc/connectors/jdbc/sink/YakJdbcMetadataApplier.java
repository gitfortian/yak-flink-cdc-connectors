package io.yak.flink.cdc.connectors.jdbc.sink;

import io.yak.flink.cdc.connectors.jdbc.JdbcSinkConfig;
import io.yak.flink.cdc.connectors.jdbc.dialect.JdbcDialect;
import io.yak.flink.cdc.connectors.jdbc.dialect.JdbcDialectRegistry;
import io.yak.flink.cdc.connectors.jdbc.runtime.JdbcConnectionProvider;
import io.yak.flink.cdc.connectors.jdbc.runtime.JdbcSchemaChangeExecutor;
import io.yak.flink.cdc.connectors.jdbc.runtime.JdbcSchemaChangeInspector;
import io.yak.flink.cdc.connectors.jdbc.runtime.JdbcSchemaChangePlanner;

import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.sink.MetadataApplier;

import java.sql.SQLException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * Flink CDC MetadataApplier adapter for JDBC targets.
 *
 * <p>Production execution is intentionally delegated to a planner/executor pipeline so the Flink
 * protocol boundary stays small while schema reconciliation, preflight and retry semantics remain
 * independently testable.
 */
public final class YakJdbcMetadataApplier implements MetadataApplier {
    private static final long serialVersionUID = 1L;

    private final JdbcSinkConfig config;
    private Set<SchemaChangeEventType> acceptedTypes = supportedTypes();

    // Runtime resources must not cross Flink's client/JobMaster classloader serialization boundary.
    private transient JdbcDialect dialect;
    private transient JdbcConnectionProvider connectionProvider;
    private transient JdbcSchemaChangeInspector schemaInspector;
    private transient JdbcSchemaChangePlanner schemaPlanner;
    private transient JdbcSchemaChangeExecutor schemaExecutor;

    public YakJdbcMetadataApplier(JdbcSinkConfig config) {
        this.config = config;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) {
        if (!acceptsSchemaEvolutionType(event.getType())) {
            return;
        }
        executor().execute(planner().plan(event));
    }

    @Override
    public MetadataApplier setAcceptedSchemaEvolutionTypes(
            Set<SchemaChangeEventType> schemaEvolutionTypes) {
        EnumSet<SchemaChangeEventType> accepted = supportedTypes();
        accepted.retainAll(schemaEvolutionTypes);
        this.acceptedTypes = Collections.unmodifiableSet(accepted);
        return this;
    }

    @Override
    public boolean acceptsSchemaEvolutionType(SchemaChangeEventType schemaChangeEventType) {
        return acceptedTypes.contains(schemaChangeEventType);
    }

    @Override
    public Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
        return Collections.unmodifiableSet(supportedTypes());
    }

    private JdbcDialect dialect() {
        if (dialect == null) {
            dialect = JdbcDialectRegistry.discoverRuntime(config.getDialect(), config.getUrl());
        }
        return dialect;
    }

    private JdbcConnectionProvider connectionProvider() {
        if (connectionProvider == null) {
            connectionProvider = new JdbcConnectionProvider(config);
        }
        return connectionProvider;
    }

    private JdbcSchemaChangeInspector inspector() {
        if (schemaInspector == null) {
            schemaInspector = new JdbcSchemaChangeInspector(dialect());
        }
        return schemaInspector;
    }

    private JdbcSchemaChangePlanner planner() {
        if (schemaPlanner == null) {
            schemaPlanner = new JdbcSchemaChangePlanner(dialect(), inspector());
        }
        return schemaPlanner;
    }

    private JdbcSchemaChangeExecutor executor() {
        if (schemaExecutor == null) {
            schemaExecutor = new JdbcSchemaChangeExecutor(config, connectionProvider());
        }
        return schemaExecutor;
    }

    private static EnumSet<SchemaChangeEventType> supportedTypes() {
        return EnumSet.of(
                SchemaChangeEventType.CREATE_TABLE,
                SchemaChangeEventType.ADD_COLUMN,
                SchemaChangeEventType.RENAME_COLUMN,
                SchemaChangeEventType.DROP_COLUMN,
                SchemaChangeEventType.ALTER_COLUMN_TYPE,
                SchemaChangeEventType.TRUNCATE_TABLE,
                SchemaChangeEventType.DROP_TABLE);
    }

    // Kept package-visible so the existing retry-classification contract remains directly testable.
    static boolean isRetryableDdlFailure(SQLException failure) {
        return JdbcSchemaChangeExecutor.isRetryableDdlFailure(failure);
    }
}
