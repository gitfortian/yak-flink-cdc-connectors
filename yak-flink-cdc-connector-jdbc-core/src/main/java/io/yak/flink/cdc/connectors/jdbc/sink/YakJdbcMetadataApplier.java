package io.yak.flink.cdc.connectors.jdbc.sink;

import io.yak.flink.cdc.connectors.jdbc.JdbcSinkConfig;
import io.yak.flink.cdc.connectors.jdbc.dialect.JdbcDialect;
import io.yak.flink.cdc.connectors.jdbc.dialect.JdbcDialectRegistry;
import io.yak.flink.cdc.connectors.jdbc.runtime.JdbcConnectionProvider;
import io.yak.flink.cdc.connectors.jdbc.runtime.JdbcSchemaChangeInspector;
import io.yak.flink.cdc.connectors.jdbc.runtime.SchemaChangeInspectionResult;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.sink.MetadataApplier;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

public final class YakJdbcMetadataApplier implements MetadataApplier {
    private static final long serialVersionUID = 1L;
    private static final long INITIAL_RETRY_BACKOFF_MILLIS = 100L;
    private static final long MAX_RETRY_BACKOFF_MILLIS = 1_000L;

    private final JdbcSinkConfig config;
    private Set<SchemaChangeEventType> acceptedTypes = supportedTypes();

    // Runtime resources must not cross Flink's client/JobMaster classloader serialization boundary.
    private transient JdbcDialect dialect;
    private transient JdbcConnectionProvider connectionProvider;
    private transient JdbcSchemaChangeInspector schemaInspector;

    public YakJdbcMetadataApplier(JdbcSinkConfig config) {
        this.config = config;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) {
        if (!acceptsSchemaEvolutionType(event.getType())) {
            return;
        }

        if (event instanceof CreateTableEvent) {
            CreateTableEvent create = (CreateTableEvent) event;
            execute(
                    dialect().buildCreateTableStatement(event.tableId(), create.getSchema()),
                    connection ->
                            inspector()
                                    .inspectCreateTable(
                                            connection, event.tableId(), create.getSchema()));
            return;
        }
        if (event instanceof AddColumnEvent) {
            for (AddColumnEvent.ColumnWithPosition added :
                    ((AddColumnEvent) event).getAddedColumns()) {
                execute(
                        dialect().buildAddColumnStatement(event.tableId(), added.getAddColumn()),
                        connection ->
                                inspector()
                                        .inspectAddColumn(
                                                connection,
                                                event.tableId(),
                                                added.getAddColumn()));
            }
            return;
        }
        if (event instanceof RenameColumnEvent) {
            ((RenameColumnEvent) event)
                    .getNameMapping()
                    .forEach(
                            (oldName, newName) ->
                                    execute(
                                            dialect()
                                                    .buildRenameColumnStatement(
                                                            event.tableId(), oldName, newName),
                                            connection ->
                                                    inspector()
                                                            .inspectRenameColumn(
                                                                    connection,
                                                                    event.tableId(),
                                                                    oldName,
                                                                    newName)));
            return;
        }
        if (event instanceof DropColumnEvent) {
            for (String column : ((DropColumnEvent) event).getDroppedColumnNames()) {
                execute(
                        dialect().buildDropColumnStatement(event.tableId(), column),
                        connection ->
                                inspector()
                                        .inspectDropColumn(connection, event.tableId(), column));
            }
            return;
        }
        if (event instanceof AlterColumnTypeEvent) {
            ((AlterColumnTypeEvent) event)
                    .getTypeMapping()
                    .forEach(
                            (column, type) ->
                                    execute(
                                            dialect()
                                                    .buildAlterColumnTypeStatement(
                                                            event.tableId(), column, type),
                                            connection ->
                                                    inspector()
                                                            .inspectAlterColumnType(
                                                                    connection,
                                                                    event.tableId(),
                                                                    column,
                                                                    type)));
            return;
        }
        if (event instanceof TruncateTableEvent) {
            // TRUNCATE is safe to replay while the schema operator is blocking subsequent data
            // events, so no target-state probe is required.
            execute(
                    dialect().buildTruncateTableStatement(event.tableId()),
                    connection ->
                            SchemaChangeInspectionResult.notApplied(
                                    "TRUNCATE must execute; replay is idempotent"));
            return;
        }
        if (event instanceof DropTableEvent) {
            execute(
                    dialect().buildDropTableStatement(event.tableId()),
                    connection -> inspector().inspectDropTable(connection, event.tableId()));
            return;
        }

        throw new UnsupportedOperationException(
                "Unsupported schema change event: " + event.getClass().getName());
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

    private void execute(String sql, DdlStateProbe stateProbe) {
        SQLException last = null;
        for (int attempt = 0; attempt <= config.getMaxRetries(); attempt++) {
            try (Connection connection = connectionProvider().open()) {
                SchemaChangeInspectionResult before = stateProbe.inspect(connection);
                if (before.isApplied()) {
                    return;
                }
                if (before.isConflict()) {
                    throw schemaConflict(sql, before, null);
                }

                try (Statement statement = connection.createStatement()) {
                    statement.executeUpdate(sql);
                    return;
                }
            } catch (SQLException failure) {
                last = failure;

                SchemaChangeInspectionResult after = inspectAfterFailure(stateProbe, failure);
                if (after != null && after.isApplied()) {
                    // The database committed the DDL but the JDBC client observed an ambiguous
                    // failure. Target metadata is the source of truth, so treat it as success.
                    return;
                }
                if (after != null && after.isConflict()) {
                    throw schemaConflict(sql, after, failure);
                }

                if (attempt == config.getMaxRetries() || !isRetryableDdlFailure(failure)) {
                    break;
                }
                sleepBeforeRetry(attempt);
            }
        }
        throw new IllegalStateException("JDBC schema change failed. SQL: " + sql, last);
    }

    private SchemaChangeInspectionResult inspectAfterFailure(
            DdlStateProbe stateProbe, SQLException originalFailure) {
        try (Connection connection = connectionProvider().open()) {
            return stateProbe.inspect(connection);
        } catch (SQLException inspectionFailure) {
            originalFailure.addSuppressed(inspectionFailure);
            return null;
        }
    }

    private static IllegalStateException schemaConflict(
            String sql, SchemaChangeInspectionResult result, SQLException cause) {
        String message =
                "JDBC schema change conflicts with target metadata. "
                        + result.getDetail()
                        + ". SQL: "
                        + sql;
        return cause == null
                ? new IllegalStateException(message)
                : new IllegalStateException(message, cause);
    }

    static boolean isRetryableDdlFailure(SQLException failure) {
        for (SQLException current = failure; current != null; current = current.getNextException()) {
            if (current instanceof SQLTransientException
                    || current instanceof SQLRecoverableException) {
                return true;
            }
            String sqlState = current.getSQLState();
            if (sqlState != null
                    && (sqlState.startsWith("08") || sqlState.startsWith("40"))) {
                return true;
            }
        }
        return false;
    }

    private static void sleepBeforeRetry(int attempt) {
        long shift = Math.min(attempt, 4);
        long backoff =
                Math.min(
                        MAX_RETRY_BACKOFF_MILLIS,
                        INITIAL_RETRY_BACKOFF_MILLIS * (1L << shift));
        try {
            Thread.sleep(backoff);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while waiting to retry JDBC schema change", interrupted);
        }
    }

    @FunctionalInterface
    private interface DdlStateProbe {
        SchemaChangeInspectionResult inspect(Connection connection) throws SQLException;
    }
}
