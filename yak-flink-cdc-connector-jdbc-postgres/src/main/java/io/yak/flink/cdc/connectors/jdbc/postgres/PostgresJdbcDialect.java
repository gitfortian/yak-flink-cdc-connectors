package io.yak.flink.cdc.connectors.jdbc.postgres;

import io.yak.flink.cdc.connectors.jdbc.dialect.AbstractJdbcDialect;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeChecks;

import java.util.List;
import java.util.stream.Collectors;

public final class PostgresJdbcDialect extends AbstractJdbcDialect {
    private static final long serialVersionUID = 1L;

    @Override
    public String identifier() {
        return "postgres";
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public String toDatabaseType(DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return "BOOLEAN";
            case TINYINT:
            case SMALLINT:
                return "SMALLINT";
            case INTEGER:
                return "INTEGER";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "REAL";
            case DOUBLE:
                return "DOUBLE PRECISION";
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
                return varcharLength >= 10_485_760 ? "TEXT" : "VARCHAR(" + varcharLength + ")";
            case BINARY:
            case VARBINARY:
                return "BYTEA";
            case DATE:
                return "DATE";
            case TIME_WITHOUT_TIME_ZONE:
                return "TIME";
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return "TIMESTAMP(" + Math.min(DataTypeChecks.getPrecision(type), 6) + ")";
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                return "TIMESTAMP(" + Math.min(DataTypeChecks.getPrecision(type), 6) + ") WITH TIME ZONE";
            default:
                throw new UnsupportedOperationException(
                        "Unsupported PostgreSQL target type in MVP: " + type.asSummaryString());
        }
    }

    @Override
    public String buildUpsertStatement(TableId tableId, Schema schema) {
        if (schema.primaryKeys().isEmpty()) {
            return buildInsertStatement(tableId, schema);
        }

        String conflictColumns =
                schema.primaryKeys().stream()
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));

        List<String> nonPrimary =
                schema.getColumns().stream()
                        .map(Column::getName)
                        .filter(name -> !schema.primaryKeys().contains(name))
                        .collect(Collectors.toList());

        String conflictAction;
        if (nonPrimary.isEmpty()) {
            conflictAction = "DO NOTHING";
        } else {
            conflictAction =
                    "DO UPDATE SET "
                            + nonPrimary.stream()
                                    .map(
                                            name ->
                                                    quoteIdentifier(name)
                                                            + " = EXCLUDED."
                                                            + quoteIdentifier(name))
                                    .collect(Collectors.joining(", "));
        }

        return buildInsertStatement(tableId, schema)
                + " ON CONFLICT ("
                + conflictColumns
                + ") "
                + conflictAction;
    }

    @Override
    public String buildAlterColumnTypeStatement(
            TableId tableId, String columnName, DataType newType) {
        return "ALTER TABLE "
                + tableName(tableId)
                + " ALTER COLUMN "
                + quoteIdentifier(columnName)
                + " TYPE "
                + toDatabaseType(newType);
    }
}
