package io.yak.flink.cdc.connectors.jdbc.state;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Checkpointed JDBC writer state containing the latest known schema for every target table. */
public final class YakJdbcWriterState implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<TableId, Schema> schemas;

    public YakJdbcWriterState(Map<TableId, Schema> schemas) {
        Objects.requireNonNull(schemas, "schemas");
        this.schemas = Collections.unmodifiableMap(new LinkedHashMap<>(schemas));
    }

    public Map<TableId, Schema> getSchemas() {
        return schemas;
    }

    /**
     * Merges recovered states after restart or rescaling.
     *
     * <p>Schema events are expected to be consistent across writer subtasks. Identical copies are
     * de-duplicated. Conflicting schemas fail recovery instead of silently choosing one and risking
     * incorrect JDBC binding.
     */
    public static Map<TableId, Schema> merge(Collection<YakJdbcWriterState> recoveredStates)
            throws IOException {
        if (recoveredStates == null || recoveredStates.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<TableId, Schema> merged = new LinkedHashMap<>();
        for (YakJdbcWriterState state : recoveredStates) {
            if (state == null) {
                continue;
            }
            for (Map.Entry<TableId, Schema> entry : state.schemas.entrySet()) {
                Schema existing = merged.putIfAbsent(entry.getKey(), entry.getValue());
                if (existing != null && !existing.equals(entry.getValue())) {
                    throw new IOException(
                            "Conflicting recovered schema state for table "
                                    + entry.getKey().identifier()
                                    + ". Refusing to restore with an ambiguous JDBC schema cache.");
                }
            }
        }
        return Collections.unmodifiableMap(merged);
    }
}
