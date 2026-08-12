package io.yak.flink.cdc.connectors.jdbc.sink;

import io.yak.flink.cdc.connectors.jdbc.JdbcSinkConfig;
import io.yak.flink.cdc.connectors.jdbc.dialect.JdbcDialect;
import io.yak.flink.cdc.connectors.jdbc.dialect.JdbcDialectRegistry;
import io.yak.flink.cdc.connectors.jdbc.runtime.JdbcBatchBuffer;
import io.yak.flink.cdc.connectors.jdbc.runtime.JdbcBatchExecutor;
import io.yak.flink.cdc.connectors.jdbc.runtime.JdbcConnectionProvider;
import io.yak.flink.cdc.connectors.jdbc.runtime.JdbcValueConverter;
import io.yak.flink.cdc.connectors.jdbc.state.YakJdbcWriterState;

import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

public final class YakJdbcWriter implements StatefulSinkWriter<Event, YakJdbcWriterState> {

    private final JdbcSinkConfig config;
    private final JdbcDialect dialect;
    private final Map<TableId, Schema> schemas;
    private final JdbcBatchBuffer batchBuffer;
    private final JdbcBatchExecutor batchExecutor;
    private final ProcessingTimeService processingTimeService;

    private transient ScheduledFuture<?> flushTimer;
    private transient boolean closed;

    public YakJdbcWriter(JdbcSinkConfig config) throws IOException {
        this(config, Collections.emptyMap(), null);
    }

    public YakJdbcWriter(JdbcSinkConfig config, Map<TableId, Schema> recoveredSchemas)
            throws IOException {
        this(config, recoveredSchemas, null);
    }

    public YakJdbcWriter(
            JdbcSinkConfig config,
            Map<TableId, Schema> recoveredSchemas,
            ProcessingTimeService processingTimeService)
            throws IOException {
        this.config = config;
        this.dialect = JdbcDialectRegistry.discoverRuntime(config.getDialect(), config.getUrl());
        this.schemas = new HashMap<>(recoveredSchemas);
        this.batchBuffer = new JdbcBatchBuffer();
        this.batchExecutor =
                new JdbcBatchExecutor(config, new JdbcConnectionProvider(config));
        this.processingTimeService = processingTimeService;
        scheduleNextFlush();
    }

    @Override
    public void write(Event event, SinkWriter.Context context) throws IOException {
        if (event instanceof SchemaChangeEvent) {
            // Flink CDC 3.6 emits FlushEvent before schema evolution and its sink wrapper invokes
            // SinkWriter.flush(false). Keep this defensive flush as well so direct/runtime variants
            // cannot carry old-schema buffered rows across a schema boundary.
            flush(false);
            batchExecutor.invalidateStatements();
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
                            + ". The table schema must arrive before data or be restored from a checkpoint.");
        }

        bufferDataChange(change, schema);
        if (batchBuffer.size() >= config.getBatchSize()) {
            flush(false);
        }
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
                    "Cannot apply schema change before CreateTableEvent or state recovery for "
                            + event.tableId());
        }
        schemas.put(event.tableId(), SchemaUtils.applySchemaChangeEvent(current, event));
    }

    private void bufferDataChange(DataChangeEvent event, Schema schema) throws IOException {
        OperationType operation = event.op();

        if (operation == OperationType.DELETE) {
            bufferDelete(event, schema);
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
        batchBuffer.add(sql, values);
    }

    private void bufferDelete(DataChangeEvent event, Schema schema) throws IOException {
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
        batchBuffer.add(dialect.buildDeleteStatement(event.tableId(), schema), keyValues);
    }

    @Override
    public List<YakJdbcWriterState> snapshotState(long checkpointId) throws IOException {
        // The Flink Sink V2 operator also flushes on checkpoints. Flushing here makes the writer
        // state contract explicit: a checkpoint never snapshots schema state while DML is still
        // only present in the local JVM buffer.
        flush(false);
        return Collections.singletonList(new YakJdbcWriterState(schemas));
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        if (batchBuffer.isEmpty()) {
            return;
        }
        batchExecutor.execute(batchBuffer);
        batchBuffer.clear();
    }

    private void scheduleNextFlush() {
        if (processingTimeService == null
                || config.getFlushIntervalMillis() == 0
                || closed) {
            return;
        }
        long next =
                processingTimeService.getCurrentProcessingTime()
                        + config.getFlushIntervalMillis();
        flushTimer = processingTimeService.registerTimer(next, this::onProcessingTime);
    }

    private void onProcessingTime(long timestamp) throws Exception {
        if (closed) {
            return;
        }
        flush(false);
        scheduleNextFlush();
    }

    @Override
    public void close() throws Exception {
        closed = true;
        if (flushTimer != null) {
            flushTimer.cancel(false);
            flushTimer = null;
        }
        try {
            flush(true);
        } finally {
            batchExecutor.close();
        }
    }
}
