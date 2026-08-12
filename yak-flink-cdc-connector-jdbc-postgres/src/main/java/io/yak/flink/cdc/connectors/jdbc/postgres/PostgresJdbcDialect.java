package io.yak.flink.cdc.connectors.jdbc.postgres;

import io.yak.flink.cdc.connectors.jdbc.dialect.AbstractJdbcDialect;
import io.yak.flink.cdc.connectors.jdbc.dialect.JdbcColumnMetadata;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeChecks;

import java.sql.Types;
import java.util.List;
import java.util.Locale;
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
    public boolean isColumnTypeCompatible(DataType expectedType, JdbcColumnMetadata actualColumn) {
        if (expectedType.isNullable() != actualColumn.isNullable()) {
            return false;
        }

        int jdbcType = actualColumn.getJdbcType();
        String typeName = actualColumn.getTypeName().toLowerCase(Locale.ROOT);
        switch (expectedType.getTypeRoot()) {
            case BOOLEAN:
                return jdbcType == Types.BOOLEAN || jdbcType == Types.BIT;
            case TINYINT:
            case SMALLINT:
                return jdbcType == Types.SMALLINT;
            case INTEGER:
                return jdbcType == Types.INTEGER;
            case BIGINT:
                return jdbcType == Types.BIGINT;
            case FLOAT:
                return jdbcType == Types.REAL || jdbcType == Types.FLOAT;
            case DOUBLE:
                return jdbcType == Types.DOUBLE;
            case DECIMAL:
                return (jdbcType == Types.DECIMAL || jdbcType == Types.NUMERIC)
                        && actualColumn.getColumnSize() == DataTypeChecks.getPrecision(expectedType)
                        && actualColumn.getDecimalDigits() == DataTypeChecks.getScale(expectedType);
            case CHAR:
                return jdbcType == Types.CHAR
                        && actualColumn.getColumnSize() == DataTypeChecks.getLength(expectedType);
            case VARCHAR:
                int expectedLength = DataTypeChecks.getLength(expectedType);
                if (expectedLength >= 10_485_760) {
                    return typeName.equals("text")
                            || jdbcType == Types.LONGVARCHAR
                            || (jdbcType == Types.VARCHAR
                                    && actualColumn.getColumnSize() > 10_485_760);
                }
                return jdbcType == Types.VARCHAR
                        && actualColumn.getColumnSize() == expectedLength;
            case BINARY:
            case VARBINARY:
                return typeName.equals("bytea")
                        || jdbcType == Types.BINARY
                        || jdbcType == Types.VARBINARY
                        || jdbcType == Types.LONGVARBINARY;
            case DATE:
                return jdbcType == Types.DATE;
            case TIME_WITHOUT_TIME_ZONE:
                return jdbcType == Types.TIME || jdbcType == Types.TIME_WITH_TIMEZONE;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return jdbcType == Types.TIMESTAMP
                        && !typeName.contains("tz")
                        && !typeName.contains("time zone");
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                return jdbcType == Types.TIMESTAMP_WITH_TIMEZONE
                        || typeName.contains("timestamptz")
                        || typeName.contains("time zone");
            default:
                return false;
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
