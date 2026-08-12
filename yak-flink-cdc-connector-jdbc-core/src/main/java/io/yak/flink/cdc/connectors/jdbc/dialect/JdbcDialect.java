package io.yak.flink.cdc.connectors.jdbc.dialect;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;

import java.io.Serializable;

public interface JdbcDialect extends Serializable {

    String identifier();

    String quoteIdentifier(String identifier);

    String toDatabaseType(DataType type);

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
