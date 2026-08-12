package io.yak.flink.cdc.connectors.jdbc.sink;

import io.yak.flink.cdc.connectors.jdbc.JdbcSinkConfig;
import io.yak.flink.cdc.connectors.jdbc.dialect.JdbcDialect;
import io.yak.flink.cdc.connectors.jdbc.dialect.JdbcDialectRegistry;
import io.yak.flink.cdc.connectors.jdbc.runtime.JdbcConnectionProvider;
import io.yak.flink.cdc.connectors.jdbc.runtime.JdbcValueConverter;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class YakJdbcWriter implements SinkWriter<Event> {

    private final JdbcSinkConfig config;
    private final JdbcDialect dialect;
    private final JdbcConnectionProvider connectionProvider;
    private final Map<TableId, Schema> schemas = new HashMap<>();

    private transient Connection connection;

    public YakJdbcWriter(JdbcSinkConfig config) throws IOException {
        this.config = config;
        this.dialect = JdbcDialectRegistry.discoverRuntime(config.getDialect(), config.getUrl());
        this.connectionProvider = new JdbcConnectionProvider(config);
        this.connection = openConnection();
    }

    @Override
    public void write(Event event, Context context) throws IOException {
        if (event instanceof SchemaChangeEvent) {
            applySchemaEvent((SchemaChangeEvent) event);
            return;
        }
        if (!(event instanceof DataChangeEvent)) {
            return;
        }

        DataChangeEvent change = (DataChangeEvent) event;
        Schema schema = schemas.get(change.tableId());
        if (schema == null) {
            throw new IOException(
                    "No schema cached for "
                            + change.tableId()
                            + ". A CreateTableEvent must reach the sink before data events.");
        }

        writeDataChange(change, schema);
    }

    private void applySchemaEvent(SchemaChangeEvent event) {
        if (event instanceof CreateTableEvent) {
            schemas.put(event.tableId(), ((CreateTableEvent) event).getSchema());
            return;
        }
        if (event instanceof DropTableEvent) {
            schemas.remove(event.tableId());
            return;
        }

        Schema current = schemas.get(event.tableId());
        if (current == null) {
            throw new IllegalStateException(
                    "Cannot apply schema change before CreateTableEvent for " + event.tableId());
        }
        schemas.put(event.tableId(), SchemaUtils.applySchemaChangeEvent(current, event));
    }

    private void writeDataChange(DataChangeEvent event, Schema schema) throws IOException {
        OperationType operation = event.op();

        if (operation == OperationType.DELETE) {
            executeDelete(event, schema);
            return;
        }

        if (operation != OperationType.INSERT
                && operation != OperationType.UPDATE
                && operation != OperationType.REPLACE) {
            throw new IOException("Unsupported operation: " + operation);
        }

        if (schema.primaryKeys().isEmpty() && operation != OperationType.INSERT) {
            throw new IOException(
                    operation
                            + " requires a primary key for replay-safe JDBC CDC writes to "
                            + event.tableId());
        }

        String sql =
                schema.primaryKeys().isEmpty()
                        ? dialect.buildInsertStatement(event.tableId(), schema)
                        : dialect.buildUpsertStatement(event.tableId(), schema);
        List<Object> values = JdbcValueConverter.toJdbcValues(event.after(), schema);
        execute(sql, values);
    }

    private void executeDelete(DataChangeEvent event, Schema schema) throws IOException {
        if (schema.primaryKeys().isEmpty()) {
            throw new IOException(
                    "DELETE requires a primary key for table " + event.tableId().identifier());
        }

        RecordData before = event.before();
        List<Object> allValues = JdbcValueConverter.toJdbcValues(before, schema);
        List<String> names = schema.getColumnNames();
        java.util.ArrayList<Object> keyValues = new java.util.ArrayList<>();
        for (String primaryKey : schema.primaryKeys()) {
            int index = names.indexOf(primaryKey);
            if (index < 0) {
                throw new IOException("Primary key column not found in schema: " + primaryKey);
            }
            keyValues.add(allValues.get(index));
        }
        execute(dialect.buildDeleteStatement(event.tableId(), schema), keyValues);
    }

    private void execute(String sql, List<Object> values) throws IOException {
        SQLException last = null;
        for (int attempt = 0; attempt <= config.getMaxRetries(); attempt++) {
            try {
                ensureConnection();
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    for (int i = 0; i < values.size(); i++) {
                        statement.setObject(i + 1, values.get(i));
                    }
                    statement.executeUpdate();
                    return;
                }
            } catch (SQLException e) {
                last = e;
                closeQuietly();
                if (attempt == config.getMaxRetries()) {
                    break;
                }
            }
        }
        throw new IOException("JDBC write failed after retries. SQL: " + sql, last);
    }

    private void ensureConnection() throws IOException {
        try {
            if (connection == null || connection.isClosed() || !connection.isValid(2)) {
                connection = openConnection();
            }
        } catch (SQLException e) {
            throw new IOException("Unable to validate JDBC connection", e);
        }
    }

    private Connection openConnection() throws IOException {
        try {
            Connection opened = connectionProvider.open();
            opened.setAutoCommit(true);
            return opened;
        } catch (SQLException | RuntimeException e) {
            throw new IOException("Unable to open JDBC connection to " + config.getUrl(), e);
        }
    }

    @Override
    public void flush(boolean endOfInput) {
        // MVP uses autocommit row-wise writes; there is no pending client-side buffer.
    }

    @Override
    public void close() {
        closeQuietly();
    }

    private void closeQuietly() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ignored) {
                // best effort
            } finally {
                connection = null;
            }
        }
    }
}
