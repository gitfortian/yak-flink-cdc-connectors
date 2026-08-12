package io.yak.flink.cdc.connectors.jdbc.mysql;

import io.yak.flink.cdc.connectors.jdbc.dialect.AbstractJdbcDialect;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeChecks;

import java.util.List;
import java.util.stream.Collectors;

public final class MySqlJdbcDialect extends AbstractJdbcDialect {
    private static final long serialVersionUID = 1L;

    @Override
    public String identifier() {
        return "mysql";
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    @Override
    public String toDatabaseType(DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return "BOOLEAN";
            case TINYINT:
                return "TINYINT";
            case SMALLINT:
                return "SMALLINT";
            case INTEGER:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case DECIMAL:
                return "DECIMAL("
                        + DataTypeChecks.getPrecision(type)
                        + ","
                        + DataTypeChecks.getScale(type)
                        + ")";
            case CHAR:
                return "CHAR(" + DataTypeChecks.getLength(type) + ")";
            case VARCHAR:
                int varcharLength = DataTypeChecks.getLength(type);
                return varcharLength > 65535 ? "TEXT" : "VARCHAR(" + varcharLength + ")";
            case BINARY:
            case VARBINARY:
                return "BLOB";
            case DATE:
                return "DATE";
            case TIME_WITHOUT_TIME_ZONE:
                return "TIME";
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return "DATETIME(" + Math.min(DataTypeChecks.getPrecision(type), 6) + ")";
            default:
                throw new UnsupportedOperationException(
                        "Unsupported MySQL target type in MVP: " + type.asSummaryString());
        }
    }

    @Override
    public String buildUpsertStatement(TableId tableId, Schema schema) {
        if (schema.primaryKeys().isEmpty()) {
            return buildInsertStatement(tableId, schema);
        }

        List<String> nonPrimary =
                schema.getColumns().stream()
                        .map(Column::getName)
                        .filter(name -> !schema.primaryKeys().contains(name))
                        .collect(Collectors.toList());

        if (nonPrimary.isEmpty()) {
            nonPrimary = schema.primaryKeys().subList(0, 1);
        }

        String updates =
                nonPrimary.stream()
                        .map(
                                name ->
                                        quoteIdentifier(name)
                                                + " = VALUES("
                                                + quoteIdentifier(name)
                                                + ")")
                        .collect(Collectors.joining(", "));

        return buildInsertStatement(tableId, schema) + " ON DUPLICATE KEY UPDATE " + updates;
    }

    @Override
    public String buildAlterColumnTypeStatement(
            TableId tableId, String columnName, DataType newType) {
        String nullable = newType.isNullable() ? " NULL" : " NOT NULL";
        return "ALTER TABLE "
                + tableName(tableId)
                + " MODIFY COLUMN "
                + quoteIdentifier(columnName)
                + " "
                + toDatabaseType(newType)
                + nullable;
    }
}
