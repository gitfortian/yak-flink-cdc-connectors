package io.yak.flink.cdc.connectors.jdbc.dialect;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

public interface JdbcDialect extends Serializable {

    String identifier();

    String quoteIdentifier(String identifier);

    String toDatabaseType(DataType type);

    /**
     * Returns the JDBC catalog used to inspect a Flink CDC table through {@link
     * java.sql.DatabaseMetaData}. The default mapping treats the TableId namespace as catalog.
     */
    default String metadataCatalog(Connection connection, TableId tableId) throws SQLException {
        return tableId.getNamespace();
    }

    /**
     * Returns the JDBC schema used to inspect a Flink CDC table through {@link
     * java.sql.DatabaseMetaData}. The default mapping treats TableId.schemaName as schema.
     */
    default String metadataSchema(Connection connection, TableId tableId) throws SQLException {
        return tableId.getSchemaName();
    }

    /**
     * Returns whether target JDBC metadata represents the requested Flink CDC column type.
     *
     * <p>This is a default method so third-party dialect implementations compiled against the
     * earlier SPI remain loadable. A dialect that wants replay-safe schema reconciliation must
     * implement the method explicitly; silently guessing compatibility is not safe.
     */
    default boolean isColumnTypeCompatible(
            DataType expectedType, JdbcColumnMetadata actualColumn) {
        throw new UnsupportedOperationException(
                "JDBC dialect '"
                        + identifier()
                        + "' must implement column metadata compatibility for replay-safe DDL");
    }

    String buildCreateTableStatement(TableId tableId, Schema schema);

    String buildAddColumnStatement(TableId tableId, Column column);

    String buildRenameColumnStatement(TableId tableId, String oldName, String newName);

    String buildDropColumnStatement(TableId tableId, String columnName);

    String buildAlterColumnTypeStatement(TableId tableId, String columnName, DataType newType);

    String buildTruncateTableStatement(TableId tableId);

    String buildDropTableStatement(TableId tableId);

    String buildInsertStatement(TableId tableId, Schema schema);

    String buildUpsertStatement(TableId tableId, Schema schema);

    String buildDeleteStatement(TableId tableId, Schema schema);
}
