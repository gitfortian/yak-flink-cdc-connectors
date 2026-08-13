package io.yak.flink.cdc.connectors.jdbc.runtime;

import org.apache.flink.cdc.common.event.TableId;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;

/**
 * Ordered in-memory JDBC batch buffer.
 *
 * <p>Only adjacent records with the same table and SQL statement are coalesced into one JDBC batch
 * segment. This deliberately preserves the original CDC event order. Globally grouping identical
 * SQL could reorder a stream such as UPSERT -> DELETE -> UPSERT and produce a different final
 * database state.
 *
 * <p>The byte accounting is an intentionally conservative JVM-retained-memory estimate. It is used
 * to bound how much payload a writer keeps alive between flushes; it is not a database wire-packet
 * size calculation.
 */
public final class JdbcBatchBuffer {
    private static final long ROW_OVERHEAD_BYTES = 32L;
    private static final long VALUE_REFERENCE_BYTES = 8L;
    private static final long SEGMENT_OVERHEAD_BYTES = 64L;

    private final List<BatchSegment> segments = new ArrayList<>();
    private int recordCount;
    private long estimatedBytes;

    /** Backward-compatible helper for tests/direct runtime use without table-scoped invalidation. */
    public void add(String sql, List<Object> values) {
        add(null, sql, values);
    }

    public void add(TableId tableId, String sql, List<Object> values) {
        Objects.requireNonNull(sql, "sql");
        Objects.requireNonNull(values, "values");

        BatchSegment segment =
                segments.isEmpty() ? null : segments.get(segments.size() - 1);
        if (segment == null || !segment.matches(tableId, sql)) {
            segment = new BatchSegment(tableId, sql);
            segments.add(segment);
            estimatedBytes += estimateSegmentBytes(tableId, sql);
        }
        segment.add(values);
        recordCount++;
        estimatedBytes += estimateRowBytes(values);
    }

    public int size() {
        return recordCount;
    }

    public long estimatedBytes() {
        return estimatedBytes;
    }

    public boolean isEmpty() {
        return recordCount == 0;
    }

    public boolean wouldExceed(long maxBytes, String sql, List<Object> values) {
        return wouldExceed(maxBytes, null, sql, values);
    }

    /**
     * Returns whether retaining the next record would cross the configured memory boundary.
     *
     * <p>A single record may itself exceed the boundary. Writers should flush an existing batch
     * before adding it and then flush that oversized record immediately after adding it. That keeps
     * memory bounded to the configured threshold plus at most one unavoidable input record.
     */
    public boolean wouldExceed(
            long maxBytes, TableId tableId, String sql, List<Object> values) {
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be > 0");
        }
        return estimatedBytes + estimateAdditionalBytes(tableId, sql, values) > maxBytes;
    }

    public List<BatchSegment> getSegments() {
        return Collections.unmodifiableList(segments);
    }

    public void clear() {
        segments.clear();
        recordCount = 0;
        estimatedBytes = 0L;
    }

    private long estimateAdditionalBytes(TableId tableId, String sql, List<Object> values) {
        Objects.requireNonNull(sql, "sql");
        Objects.requireNonNull(values, "values");
        long additional = estimateRowBytes(values);
        BatchSegment current =
                segments.isEmpty() ? null : segments.get(segments.size() - 1);
        if (current == null || !current.matches(tableId, sql)) {
            additional += estimateSegmentBytes(tableId, sql);
        }
        return additional;
    }

    private static long estimateSegmentBytes(TableId tableId, String sql) {
        long tableBytes = tableId == null ? 0L : 2L * tableId.identifier().length();
        return SEGMENT_OVERHEAD_BYTES + tableBytes + 2L * sql.length();
    }

    private static long estimateRowBytes(List<Object> values) {
        long bytes = ROW_OVERHEAD_BYTES + VALUE_REFERENCE_BYTES * values.size();
        for (Object value : values) {
            bytes += estimateValueBytes(value);
        }
        return bytes;
    }

    private static long estimateValueBytes(Object value) {
        if (value == null) {
            return 1L;
        }
        if (value instanceof byte[]) {
            return 16L + ((byte[]) value).length;
        }
        if (value instanceof CharSequence) {
            return 40L + 2L * ((CharSequence) value).length();
        }
        if (value instanceof BigDecimal) {
            return 32L + Math.max(8L, ((BigDecimal) value).precision());
        }
        if (value instanceof Date) {
            return 24L;
        }
        if (value instanceof Number || value instanceof Boolean || value instanceof Character) {
            return 24L;
        }
        return 64L;
    }

    public static final class BatchSegment {
        private final TableId tableId;
        private final String sql;
        private final List<List<Object>> rows = new ArrayList<>();

        private BatchSegment(TableId tableId, String sql) {
            this.tableId = tableId;
            this.sql = sql;
        }

        private boolean matches(TableId candidateTableId, String candidateSql) {
            return Objects.equals(tableId, candidateTableId) && sql.equals(candidateSql);
        }

        private void add(List<Object> values) {
            rows.add(Collections.unmodifiableList(new ArrayList<>(values)));
        }

        public TableId getTableId() {
            return tableId;
        }

        public String getSql() {
            return sql;
        }

        public List<List<Object>> getRows() {
            return Collections.unmodifiableList(rows);
        }

        public int size() {
            return rows.size();
        }
    }
}
