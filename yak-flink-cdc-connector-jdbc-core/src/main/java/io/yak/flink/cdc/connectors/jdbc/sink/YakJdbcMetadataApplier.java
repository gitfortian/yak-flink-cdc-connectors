package io.yak.flink.cdc.connectors.jdbc.sink;

import io.yak.flink.cdc.connectors.jdbc.JdbcSinkConfig;
import io.yak.flink.cdc.connectors.jdbc.dialect.JdbcDialect;
import io.yak.flink.cdc.connectors.jdbc.runtime.JdbcConnectionProvider;

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
import java.sql.Statement;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

public final class YakJdbcMetadataApplier implements MetadataApplier {
    private static final long serialVersionUID = 1L;

    private final JdbcSinkConfig config;
    private final JdbcDialect dialect;
    private final JdbcConnectionProvider connectionProvider;
    private Set<SchemaChangeEventType> acceptedTypes = supportedTypes();

    public YakJdbcMetadataApplier(JdbcSinkConfig config, JdbcDialect dialect) {
        this.config = config;
        this.dialect = dialect;
        this.connectionProvider = new JdbcConnectionProvider(config);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) {
        if (!acceptsSchemaEvolutionType(event.getType())) {
            return;
        }

        if (event instanceof CreateTableEvent) {
            execute(
                    dialect.buildCreateTableStatement(
                            event.tableId(), ((CreateTableEvent) event).getSchema()));
            return;
        }
        if (event instanceof AddColumnEvent) {
            for (AddColumnEvent.ColumnWithPosition added :
                    ((AddColumnEvent) event).getAddedColumns()) {
                execute(dialect.buildAddColumnStatement(event.tableId(), added.getAddColumn()));
            }
            return;
        }
        if (event instanceof RenameColumnEvent) {
            ((RenameColumnEvent) event)
                    .getNameMapping()
                    .forEach(
                            (oldName, newName) ->
                                    execute(
                                            dialect.buildRenameColumnStatement(
                                                    event.tableId(), oldName, newName)));
            return;
        }
        if (event instanceof DropColumnEvent) {
            for (String column : ((DropColumnEvent) event).getDroppedColumnNames()) {
                execute(dialect.buildDropColumnStatement(event.tableId(), column));
            }
            return;
        }
        if (event instanceof AlterColumnTypeEvent) {
            ((AlterColumnTypeEvent) event)
                    .getTypeMapping()
                    .forEach(
                            (column, type) ->
                                    execute(
                                            dialect.buildAlterColumnTypeStatement(
                                                    event.tableId(), column, type)));
            return;
        }
        if (event instanceof TruncateTableEvent) {
            execute(dialect.buildTruncateTableStatement(event.tableId()));
            return;
        }
        if (event instanceof DropTableEvent) {
            execute(dialect.buildDropTableStatement(event.tableId()));
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

    private void execute(String sql) {
        SQLException last = null;
        for (int attempt = 0; attempt <= config.getMaxRetries(); attempt++) {
            try (Connection connection = connectionProvider.open();
                    Statement statement = connection.createStatement()) {
                statement.executeUpdate(sql);
                return;
            } catch (SQLException e) {
                last = e;
                if (attempt == config.getMaxRetries()) {
                    break;
                }
            }
        }
        throw new IllegalStateException("JDBC schema change failed. SQL: " + sql, last);
    }
}
