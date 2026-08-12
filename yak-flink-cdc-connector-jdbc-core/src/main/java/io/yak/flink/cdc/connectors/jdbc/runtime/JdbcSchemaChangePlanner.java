package io.yak.flink.cdc.connectors.jdbc.runtime;

import io.yak.flink.cdc.connectors.jdbc.dialect.JdbcDialect;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TruncateTableEvent;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Converts a Flink CDC schema event into deterministic, independently verifiable DDL steps. */
public final class JdbcSchemaChangePlanner {
    private final JdbcDialect dialect;
    private final JdbcSchemaChangeInspector inspector;

    public JdbcSchemaChangePlanner(JdbcDialect dialect, JdbcSchemaChangeInspector inspector) {
        this.dialect = dialect;
        this.inspector = inspector;
    }

    public SchemaChangePlan plan(SchemaChangeEvent event) {
        List<SchemaChangeStep> steps = new ArrayList<>();

        if (event instanceof CreateTableEvent) {
            CreateTableEvent create = (CreateTableEvent) event;
            steps.add(
                    SchemaChangeStep.reconciled(
                            "create table " + event.tableId().identifier(),
                            dialect.buildCreateTableStatement(event.tableId(), create.getSchema()),
                            connection ->
                                    inspector.inspectCreateTable(
                                            connection, event.tableId(), create.getSchema())));
        } else if (event instanceof AddColumnEvent) {
            AddColumnEvent add = (AddColumnEvent) event;
            validateUniqueAddedColumns(add);
            for (AddColumnEvent.ColumnWithPosition added : add.getAddedColumns()) {
                steps.add(
                        SchemaChangeStep.reconciled(
                                "add column " + added.getAddColumn().getName(),
                                dialect.buildAddColumnStatement(
                                        event.tableId(), added.getAddColumn()),
                                connection ->
                                        inspector.inspectAddColumn(
                                                connection,
                                                event.tableId(),
                                                added.getAddColumn())));
            }
        } else if (event instanceof RenameColumnEvent) {
            RenameColumnEvent rename = (RenameColumnEvent) event;
            validateRenameMapping(rename.getNameMapping());
            rename.getNameMapping().entrySet().stream()
                    .filter(entry -> !entry.getKey().equals(entry.getValue()))
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(
                            entry -> {
                                String oldName = entry.getKey();
                                String newName = entry.getValue();
                                steps.add(
                                        SchemaChangeStep.reconciled(
                                                "rename column " + oldName + " -> " + newName,
                                                dialect.buildRenameColumnStatement(
                                                        event.tableId(), oldName, newName),
                                                connection ->
                                                        inspector.inspectRenameColumn(
                                                                connection,
                                                                event.tableId(),
                                                                oldName,
                                                                newName)));
                            });
        } else if (event instanceof DropColumnEvent) {
            List<String> columns =
                    new ArrayList<>(((DropColumnEvent) event).getDroppedColumnNames());
            columns.sort(Comparator.naturalOrder());
            for (String column : columns) {
                steps.add(
                        SchemaChangeStep.reconciled(
                                "drop column " + column,
                                dialect.buildDropColumnStatement(event.tableId(), column),
                                connection ->
                                        inspector.inspectDropColumn(
                                                connection, event.tableId(), column)));
            }
        } else if (event instanceof AlterColumnTypeEvent) {
            ((AlterColumnTypeEvent) event)
                    .getTypeMapping().entrySet().stream()
                            .sorted(Map.Entry.comparingByKey())
                            .forEach(
                                    entry -> {
                                        String column = entry.getKey();
                                        steps.add(
                                                SchemaChangeStep.reconciled(
                                                        "alter column type " + column,
                                                        dialect.buildAlterColumnTypeStatement(
                                                                event.tableId(),
                                                                column,
                                                                entry.getValue()),
                                                        connection ->
                                                                inspector.inspectAlterColumnType(
                                                                        connection,
                                                                        event.tableId(),
                                                                        column,
                                                                        entry.getValue())));
                                    });
        } else if (event instanceof TruncateTableEvent) {
            steps.add(
                    SchemaChangeStep.replaySafe(
                            "truncate table " + event.tableId().identifier(),
                            dialect.buildTruncateTableStatement(event.tableId())));
        } else if (event instanceof DropTableEvent) {
            steps.add(
                    SchemaChangeStep.reconciled(
                            "drop table " + event.tableId().identifier(),
                            dialect.buildDropTableStatement(event.tableId()),
                            connection -> inspector.inspectDropTable(connection, event.tableId())));
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported schema change event: " + event.getClass().getName());
        }

        return new SchemaChangePlan(event.tableId(), event.getType(), steps);
    }

    private static void validateUniqueAddedColumns(AddColumnEvent event) {
        Set<String> names = new HashSet<>();
        for (AddColumnEvent.ColumnWithPosition column : event.getAddedColumns()) {
            String name = column.getAddColumn().getName();
            if (!names.add(name)) {
                throw new IllegalArgumentException(
                        "ADD COLUMN event contains duplicate target column '" + name + "'");
            }
        }
    }

    private static void validateRenameMapping(Map<String, String> mapping) {
        Set<String> oldNames = new HashSet<>();
        Set<String> newNames = new HashSet<>();
        mapping.forEach(
                (oldName, newName) -> {
                    if (oldName.equals(newName)) {
                        return;
                    }
                    oldNames.add(oldName);
                    if (!newNames.add(newName)) {
                        throw new IllegalArgumentException(
                                "RENAME COLUMN event maps multiple columns to '" + newName + "'");
                    }
                });

        Set<String> dependentNames = new HashSet<>(oldNames);
        dependentNames.retainAll(newNames);
        if (!dependentNames.isEmpty()) {
            throw new UnsupportedOperationException(
                    "Dependent multi-column renames are not production-safe as sequential JDBC DDL: "
                            + dependentNames);
        }
    }
}
