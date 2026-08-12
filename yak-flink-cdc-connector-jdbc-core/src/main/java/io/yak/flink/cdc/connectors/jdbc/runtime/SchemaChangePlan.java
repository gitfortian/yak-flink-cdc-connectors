package io.yak.flink.cdc.connectors.jdbc.runtime;

import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Immutable execution plan for one Flink CDC schema-change event. */
public final class SchemaChangePlan {
    private final TableId tableId;
    private final SchemaChangeEventType eventType;
    private final List<SchemaChangeStep> steps;

    public SchemaChangePlan(
            TableId tableId, SchemaChangeEventType eventType, List<SchemaChangeStep> steps) {
        this.tableId = Objects.requireNonNull(tableId, "tableId");
        this.eventType = Objects.requireNonNull(eventType, "eventType");
        this.steps = Collections.unmodifiableList(new ArrayList<>(steps));
    }

    public TableId getTableId() {
        return tableId;
    }

    public SchemaChangeEventType getEventType() {
        return eventType;
    }

    public List<SchemaChangeStep> getSteps() {
        return steps;
    }

    public boolean isNoOp() {
        return steps.isEmpty();
    }

    public String describe() {
        return eventType + " on " + tableId.identifier() + " (" + steps.size() + " step(s))";
    }
}
