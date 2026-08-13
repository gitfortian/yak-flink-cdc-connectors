package io.yak.flink.cdc.connectors.jdbc.sink;

import io.yak.flink.cdc.connectors.jdbc.JdbcSinkConfig;
import io.yak.flink.cdc.connectors.jdbc.ReplaySafetyMode;
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
import java.util.ArrayList;
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
    }

    @Override
    public void write(Event event, SinkWriter.Context context) throws IOException {
        if (event instanceof SchemaChangeEvent) {
            SchemaChangeEvent schemaEvent = (SchemaChangeEvent) event;
            // Flink CDC 3.6 emits FlushEvent before schema evolution and its sink wrapper invokes
            // SinkWriter.flush(false). Keep this defensive flush as well so direct/runtime variants
            // cannot carry old-schema buffered rows across a schema boundary.
            flush(false);
            batchExecutor.invalidateStatements(schemaEvent.tableId());
            applySchemaEvent(schemaEvent);
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

        if (schema.primaryKeys().isEmpty()) {
            bufferNoPrimaryKeyChange(event, schema);
            return;
        }

        if (operation == OperationType.DELETE) {
            bufferDelete(event.tableId(), event.before(), schema);
            return;
        }

        if (operation != OperationType.INSERT
                && operation != OperationType.UPDATE
                && operation != OperationType.REPLACE) {
            throw new IOException("Unsupported operation: " + operation);
        }

        RecordData after = event.after();
        if (after == null) {
            throw new IOException(operation + " event has no after image for " + event.tableId());
        }

        // An UPDATE may change a primary-key value. A plain upsert of the after image would leave
        // the old primary-key row behind. Treat DELETE old key + UPSERT new row as one logical
        // mutation so batch-size/max-batch-bytes can never split the pair across transactions.
        if (operation == OperationType.UPDATE) {
            RecordData before = event.before();
            if (before == null) {
                throw new IOException(
                        "UPDATE requires a before image to verify primary-key replay safety for "
                                + event.tableId());
            }
            List<Object> oldKey = extractPrimaryKeyValues(before, schema, event.tableId());
            List<Object> newKey = extractPrimaryKeyValues(after, schema, event.tableId());
            if (!oldKey.equals(newKey)) {
                bufferPrimaryKeyMutation(
                        event.tableId(),
                        dialect.buildDeleteStatement(event.tableId(), schema),
                        oldKey,
                        dialect.buildUpsertStatement(event.tableId(), schema),
                        JdbcValueConverter.toJdbcValues(after, schema));
                return;
            }
        } else {
            // Validate key values before buffering INSERT/REPLACE as well.
            extractPrimaryKeyValues(after, schema, event.tableId());
        }

        String sql = dialect.buildUpsertStatement(event.tableId(), schema);
        List<Object> values = JdbcValueConverter.toJdbcValues(after, schema);
        buffer(event.tableId(), sql, values);
    }

    private void bufferNoPrimaryKeyChange(DataChangeEvent event, Schema schema) throws IOException {
        if (event.op() != OperationType.INSERT) {
            throw new IOException(
                    event.op()
                            + " requires a primary key for replay-safe JDBC CDC writes to "
                            + event.tableId()
                            + ". No replay-safety mode permits no-primary-key UPDATE/REPLACE/DELETE.");
        }

        if (config.getReplaySafetyMode() != ReplaySafetyMode.ALLOW_APPEND_ONLY) {
            throw new IOException(
                    "INSERT into no-primary-key table "
                            + event.tableId()
                            + " is rejected by replay-safety=strict. At-least-once replay after an "
                            + "ambiguous commit can duplicate append-only rows. Set "
                            + "replay-safety=allow-append-only only when duplicate rows are an "
                            + "accepted application-level risk.");
        }

        RecordData after = event.after();
        if (after == null) {
            throw new IOException("INSERT event has no after image for " + event.tableId());
        }
        buffer(
                event.tableId(),
                dialect.buildInsertStatement(event.tableId(), schema),
                JdbcValueConverter.toJdbcValues(after, schema));
    }

    private void bufferDelete(TableId tableId, RecordData before, Schema schema) throws IOException {
        if (before == null) {
            throw new IOException("DELETE event has no before image for " + tableId);
        }
        List<Object> keyValues = extractPrimaryKeyValues(before, schema, tableId);
        buffer(tableId, dialect.buildDeleteStatement(tableId, schema), keyValues);
    }

    private static List<Object> extractPrimaryKeyValues(
            RecordData record, Schema schema, TableId tableId) throws IOException {
        List<Object> allValues = JdbcValueConverter.toJdbcValues(record, schema);
        List<String> names = schema.getColumnNames();
        List<Object> keyValues = new ArrayList<>(schema.primaryKeys().size());
        for (String primaryKey : schema.primaryKeys()) {
            int index = names.indexOf(primaryKey);
            if (index < 0) {
                throw new IOException("Primary key column not found in schema: " + primaryKey);
            }
            Object keyValue = allValues.get(index);
            if (keyValue == null) {
                throw new IOException(
                        "Primary key column "
                                + primaryKey
                                + " is null in CDC record for "
                                + tableId
                                + "; replay-safe DML requires non-null primary keys");
            }
            keyValues.add(keyValue);
        }
        return keyValues;
    }

    private void bufferPrimaryKeyMutation(
            TableId tableId,
            String deleteSql,
            List<Object> oldKey,
            String upsertSql,
            List<Object> newRow)
            throws IOException {
        // This two-statement logical event must never straddle two commits. Flush older pending work
        // first, then add the pair without intermediate threshold checks. The pair may temporarily
        // exceed a configured size/byte threshold, just like one oversized input record, and is
        // immediately flushed afterward when required.
        if (!batchBuffer.isEmpty()) {
            flush(false);
        }

        batchBuffer.add(tableId, deleteSql, oldKey);
        batchBuffer.add(tableId, upsertSql, newRow);
        scheduleFlushIfNeeded();

        if (batchBuffer.size() >= config.getBatchSize()
                || batchBuffer.estimatedBytes() >= config.getMaxBatchBytes()) {
            flush(false);
        }
    }

    private void buffer(TableId tableId, String sql, List<Object> values) throws IOException {
        // Flush the existing batch before retaining a record that would cross the configured memory
        // boundary. A single record may itself be larger than max-batch-bytes; in that case it is
        // accepted as the only record and flushed immediately below.
        if (!batchBuffer.isEmpty()
                && batchBuffer.wouldExceed(config.getMaxBatchBytes(), tableId, sql, values)) {
            flush(false);
        }

        batchBuffer.add(tableId, sql, values);
        scheduleFlushIfNeeded();

        if (batchBuffer.size() >= config.getBatchSize()
                || batchBuffer.estimatedBytes() >= config.getMaxBatchBytes()) {
            flush(false);
        }
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
            cancelFlushTimer();
            return;
        }

        batchExecutor.execute(batchBuffer);
        batchBuffer.clear();
        cancelFlushTimer();
    }

    private void scheduleFlushIfNeeded() {
        if (processingTimeService == null
                || config.getFlushIntervalMillis() == 0
                || closed
                || batchBuffer.isEmpty()
                || flushTimer != null) {
            return;
        }
        long next =
                processingTimeService.getCurrentProcessingTime()
                        + config.getFlushIntervalMillis();
        flushTimer = processingTimeService.registerTimer(next, this::onProcessingTime);
    }

    private void onProcessingTime(long timestamp) throws Exception {
        flushTimer = null;
        if (closed) {
            return;
        }
        flush(false);
    }

    private void cancelFlushTimer() {
        if (flushTimer != null) {
            flushTimer.cancel(false);
            flushTimer = null;
        }
    }

    @Override
    public void close() throws Exception {
        closed = true;
        cancelFlushTimer();
        try {
            flush(true);
        } finally {
            batchExecutor.close();
        }
    }
}
