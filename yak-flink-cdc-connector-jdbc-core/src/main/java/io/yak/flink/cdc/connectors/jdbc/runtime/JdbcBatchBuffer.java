package io.yak.flink.cdc.connectors.jdbc.runtime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Ordered in-memory JDBC batch buffer.
 *
 * <p>Only adjacent records with the same SQL statement are coalesced into one JDBC batch segment.
 * This deliberately preserves the original CDC event order. Globally grouping identical SQL could
 * reorder a stream such as UPSERT -> DELETE -> UPSERT and produce a different final database state.
 */
public final class JdbcBatchBuffer {
    private final List<BatchSegment> segments = new ArrayList<>();
    private int recordCount;

    public void add(String sql, List<Object> values) {
        Objects.requireNonNull(sql, "sql");
        Objects.requireNonNull(values, "values");

        BatchSegment segment =
                segments.isEmpty() ? null : segments.get(segments.size() - 1);
        if (segment == null || !segment.getSql().equals(sql)) {
            segment = new BatchSegment(sql);
            segments.add(segment);
        }
        segment.add(values);
        recordCount++;
    }

    public int size() {
        return recordCount;
    }

    public boolean isEmpty() {
        return recordCount == 0;
    }

    public List<BatchSegment> getSegments() {
        return Collections.unmodifiableList(segments);
    }

    public void clear() {
        segments.clear();
        recordCount = 0;
    }

    public static final class BatchSegment {
        private final String sql;
        private final List<List<Object>> rows = new ArrayList<>();

        private BatchSegment(String sql) {
            this.sql = sql;
        }

        private void add(List<Object> values) {
            rows.add(Collections.unmodifiableList(new ArrayList<>(values)));
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
