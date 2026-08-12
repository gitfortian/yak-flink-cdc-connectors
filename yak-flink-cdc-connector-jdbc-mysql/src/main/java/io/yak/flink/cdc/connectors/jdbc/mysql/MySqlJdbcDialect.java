package io.yak.flink.cdc.connectors.jdbc.mysql;

import io.yak.flink.cdc.connectors.jdbc.dialect.AbstractJdbcDialect;
import io.yak.flink.cdc.connectors.jdbc.dialect.JdbcColumnMetadata;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeChecks;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Locale;
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
    public String metadataCatalog(Connection connection, TableId tableId) throws SQLException {
        // MySQL exposes databases as JDBC catalogs rather than JDBC schemas.
        return tableId.getSchemaName();
    }

    @Override
    public String metadataSchema(Connection connection, TableId tableId) throws SQLException {
        return null;
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
                // Connector/J may expose TINYINT(1)/BOOLEAN as BIT depending on driver settings.
                return jdbcType == Types.BIT
                        || jdbcType == Types.BOOLEAN
                        || jdbcType == Types.TINYINT;
            case TINYINT:
                return jdbcType == Types.TINYINT;
            case SMALLINT:
                return jdbcType == Types.SMALLINT;
            case INTEGER:
                return jdbcType == Types.INTEGER;
            case BIGINT:
                return jdbcType == Types.BIGINT;
            case FLOAT:
                return jdbcType == Types.FLOAT || jdbcType == Types.REAL;
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
                if (expectedLength > 65535) {
                    return jdbcType == Types.LONGVARCHAR
                            || typeName.equals("text")
                            || typeName.equals("mediumtext")
                            || typeName.equals("longtext");
                }
                return jdbcType == Types.VARCHAR
                        && actualColumn.getColumnSize() == expectedLength;
            case BINARY:
            case VARBINARY:
                return jdbcType == Types.BLOB
                        || jdbcType == Types.LONGVARBINARY
                        || typeName.endsWith("blob");
            case DATE:
                return jdbcType == Types.DATE;
            case TIME_WITHOUT_TIME_ZONE:
                return jdbcType == Types.TIME;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return jdbcType == Types.TIMESTAMP || typeName.equals("datetime");
            default:
                return false;
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
