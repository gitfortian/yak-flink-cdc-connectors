package io.yak.flink.cdc.connectors.jdbc.dialect;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class AbstractJdbcDialect implements JdbcDialect {

    protected String tableName(TableId tableId) {
        String table = quoteIdentifier(tableId.getTableName());
        String schema = tableId.getSchemaName();
        return schema == null || schema.isEmpty()
                ? table
                : quoteIdentifier(schema) + "." + table;
    }

    protected String columnDefinition(Column column) {
        String nullable = column.getType().isNullable() ? "" : " NOT NULL";
        return quoteIdentifier(column.getName())
                + " "
                + toDatabaseType(column.getType())
                + nullable;
    }

    protected String columnNames(Schema schema) {
        return schema.getColumns().stream()
                .map(Column::getName)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
    }

    protected String placeholders(int count) {
        return IntStream.range(0, count).mapToObj(i -> "?").collect(Collectors.joining(", "));
    }

    @Override
    public String buildCreateTableStatement(TableId tableId, Schema schema) {
        List<String> definitions =
                schema.getColumns().stream()
                        .map(this::columnDefinition)
                        .collect(Collectors.toCollection(ArrayList::new));
        if (!schema.primaryKeys().isEmpty()) {
            definitions.add(
                    "PRIMARY KEY ("
                            + schema.primaryKeys().stream()
                                    .map(this::quoteIdentifier)
                                    .collect(Collectors.joining(", "))
                            + ")");
        }
        return "CREATE TABLE IF NOT EXISTS "
                + tableName(tableId)
                + " ("
                + String.join(", ", definitions)
                + ")";
    }

    @Override
    public String buildAddColumnStatement(TableId tableId, Column column) {
        return "ALTER TABLE "
                + tableName(tableId)
                + " ADD COLUMN "
                + columnDefinition(column);
    }

    @Override
    public String buildRenameColumnStatement(TableId tableId, String oldName, String newName) {
        return "ALTER TABLE "
                + tableName(tableId)
                + " RENAME COLUMN "
                + quoteIdentifier(oldName)
                + " TO "
                + quoteIdentifier(newName);
    }

    @Override
    public String buildDropColumnStatement(TableId tableId, String columnName) {
        return "ALTER TABLE "
                + tableName(tableId)
                + " DROP COLUMN "
                + quoteIdentifier(columnName);
    }

    @Override
    public String buildTruncateTableStatement(TableId tableId) {
        return "TRUNCATE TABLE " + tableName(tableId);
    }

    @Override
    public String buildDropTableStatement(TableId tableId) {
        return "DROP TABLE IF EXISTS " + tableName(tableId);
    }

    @Override
    public String buildInsertStatement(TableId tableId, Schema schema) {
        return "INSERT INTO "
                + tableName(tableId)
                + " ("
                + columnNames(schema)
                + ") VALUES ("
                + placeholders(schema.getColumnCount())
                + ")";
    }

    @Override
    public String buildDeleteStatement(TableId tableId, Schema schema) {
        if (schema.primaryKeys().isEmpty()) {
            throw new IllegalArgumentException(
                    "DELETE requires a primary key for table " + tableId.identifier());
        }
        String where =
                schema.primaryKeys().stream()
                        .map(key -> quoteIdentifier(key) + " = ?")
                        .collect(Collectors.joining(" AND "));
        return "DELETE FROM " + tableName(tableId) + " WHERE " + where;
    }

    protected List<Integer> primaryKeyIndexes(Schema schema) {
        List<String> names = schema.getColumnNames();
        return schema.primaryKeys().stream()
                .map(
                        key -> {
                            int index = names.indexOf(key);
                            if (index < 0) {
                                throw new IllegalArgumentException(
                                        "Primary key column '" + key + "' not found in schema");
                            }
                            return index;
                        })
                .collect(Collectors.toList());
    }
}
