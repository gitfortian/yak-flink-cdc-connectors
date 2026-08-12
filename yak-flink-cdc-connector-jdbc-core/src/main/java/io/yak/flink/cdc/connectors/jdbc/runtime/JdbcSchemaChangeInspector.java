package io.yak.flink.cdc.connectors.jdbc.runtime;

import io.yak.flink.cdc.connectors.jdbc.dialect.JdbcColumnMetadata;
import io.yak.flink.cdc.connectors.jdbc.dialect.JdbcDialect;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Inspects target JDBC metadata before and after DDL execution.
 *
 * <p>The inspector distinguishes an already-applied replay from a conflicting target schema. This
 * is required for ambiguous failures where the database may have committed a DDL before the JDBC
 * client observed a connection error.
 */
public final class JdbcSchemaChangeInspector {
    private final JdbcDialect dialect;

    public JdbcSchemaChangeInspector(JdbcDialect dialect) {
        this.dialect = dialect;
    }

    public SchemaChangeInspectionResult inspectCreateTable(
            Connection connection, TableId tableId, Schema expectedSchema) throws SQLException {
        if (!tableExists(connection, tableId)) {
            return SchemaChangeInspectionResult.notApplied(
                    "target table does not exist: " + tableId.identifier());
        }

        Map<String, JdbcColumnMetadata> actualColumns = readColumns(connection, tableId);
        if (actualColumns.size() != expectedSchema.getColumnCount()) {
            return SchemaChangeInspectionResult.conflict(
                    "target table "
                            + tableId.identifier()
                            + " has "
                            + actualColumns.size()
                            + " columns but source schema requires "
                            + expectedSchema.getColumnCount());
        }

        for (Column expected : expectedSchema.getColumns()) {
            JdbcColumnMetadata actual = actualColumns.get(expected.getName());
            if (actual == null) {
                return SchemaChangeInspectionResult.conflict(
                        "target table "
                                + tableId.identifier()
                                + " is missing expected column '"
                                + expected.getName()
                                + "'");
            }
            if (!dialect.isColumnTypeCompatible(expected.getType(), actual)) {
                return SchemaChangeInspectionResult.conflict(
                        "target column "
                                + tableId.identifier()
                                + "."
                                + expected.getName()
                                + " is incompatible. expected="
                                + expected.getType().asSummaryString()
                                + ", actual="
                                + actual);
            }
        }

        List<String> actualPrimaryKeys = readPrimaryKeys(connection, tableId);
        if (!actualPrimaryKeys.equals(expectedSchema.primaryKeys())) {
            return SchemaChangeInspectionResult.conflict(
                    "target primary key for "
                            + tableId.identifier()
                            + " is "
                            + actualPrimaryKeys
                            + " but source schema requires "
                            + expectedSchema.primaryKeys());
        }

        return SchemaChangeInspectionResult.applied(
                "target table already matches source schema: " + tableId.identifier());
    }

    public SchemaChangeInspectionResult inspectAddColumn(
            Connection connection, TableId tableId, Column expectedColumn) throws SQLException {
        if (!tableExists(connection, tableId)) {
            return SchemaChangeInspectionResult.conflict(
                    "cannot add column because target table does not exist: " + tableId.identifier());
        }

        JdbcColumnMetadata actual = readColumns(connection, tableId).get(expectedColumn.getName());
        if (actual == null) {
            return SchemaChangeInspectionResult.notApplied(
                    "target column does not exist yet: "
                            + tableId.identifier()
                            + "."
                            + expectedColumn.getName());
        }
        if (!dialect.isColumnTypeCompatible(expectedColumn.getType(), actual)) {
            return SchemaChangeInspectionResult.conflict(
                    "target column "
                            + tableId.identifier()
                            + "."
                            + expectedColumn.getName()
                            + " already exists with an incompatible definition. expected="
                            + expectedColumn.getType().asSummaryString()
                            + ", actual="
                            + actual);
        }
        return SchemaChangeInspectionResult.applied(
                "target column already exists with the expected definition: "
                        + tableId.identifier()
                        + "."
                        + expectedColumn.getName());
    }

    public SchemaChangeInspectionResult inspectRenameColumn(
            Connection connection, TableId tableId, String oldName, String newName)
            throws SQLException {
        if (!tableExists(connection, tableId)) {
            return SchemaChangeInspectionResult.conflict(
                    "cannot rename column because target table does not exist: "
                            + tableId.identifier());
        }

        Map<String, JdbcColumnMetadata> columns = readColumns(connection, tableId);
        boolean oldExists = columns.containsKey(oldName);
        boolean newExists = columns.containsKey(newName);
        if (oldExists && !newExists) {
            return SchemaChangeInspectionResult.notApplied(
                    "source column still has its old target name: " + oldName);
        }
        if (!oldExists && newExists) {
            return SchemaChangeInspectionResult.applied(
                    "rename is already reflected in target metadata: " + oldName + " -> " + newName);
        }
        if (oldExists) {
            return SchemaChangeInspectionResult.conflict(
                    "both old and new target columns exist for rename "
                            + oldName
                            + " -> "
                            + newName);
        }
        return SchemaChangeInspectionResult.conflict(
                "neither old nor new target column exists for rename " + oldName + " -> " + newName);
    }

    public SchemaChangeInspectionResult inspectDropColumn(
            Connection connection, TableId tableId, String columnName) throws SQLException {
        if (!tableExists(connection, tableId)) {
            return SchemaChangeInspectionResult.conflict(
                    "target table disappeared while dropping column: " + tableId.identifier());
        }
        if (readColumns(connection, tableId).containsKey(columnName)) {
            return SchemaChangeInspectionResult.notApplied(
                    "target column still exists: " + tableId.identifier() + "." + columnName);
        }
        return SchemaChangeInspectionResult.applied(
                "target column is already absent: " + tableId.identifier() + "." + columnName);
    }

    public SchemaChangeInspectionResult inspectAlterColumnType(
            Connection connection, TableId tableId, String columnName, DataType expectedType)
            throws SQLException {
        if (!tableExists(connection, tableId)) {
            return SchemaChangeInspectionResult.conflict(
                    "cannot alter column because target table does not exist: "
                            + tableId.identifier());
        }
        JdbcColumnMetadata actual = readColumns(connection, tableId).get(columnName);
        if (actual == null) {
            return SchemaChangeInspectionResult.conflict(
                    "cannot alter missing target column: "
                            + tableId.identifier()
                            + "."
                            + columnName);
        }
        if (dialect.isColumnTypeCompatible(expectedType, actual)) {
            return SchemaChangeInspectionResult.applied(
                    "target column already has the requested type: "
                            + tableId.identifier()
                            + "."
                            + columnName);
        }
        return SchemaChangeInspectionResult.notApplied(
                "target column type still differs from requested schema: "
                        + tableId.identifier()
                        + "."
                        + columnName);
    }

    public SchemaChangeInspectionResult inspectDropTable(Connection connection, TableId tableId)
            throws SQLException {
        if (tableExists(connection, tableId)) {
            return SchemaChangeInspectionResult.notApplied(
                    "target table still exists: " + tableId.identifier());
        }
        return SchemaChangeInspectionResult.applied(
                "target table is already absent: " + tableId.identifier());
    }

    private boolean tableExists(Connection connection, TableId tableId) throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        try (ResultSet rs =
                metadata.getTables(
                        dialect.metadataCatalog(connection, tableId),
                        dialect.metadataSchema(connection, tableId),
                        tableId.getTableName(),
                        new String[] {"TABLE"})) {
            return rs.next();
        }
    }

    private Map<String, JdbcColumnMetadata> readColumns(Connection connection, TableId tableId)
            throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        Map<String, JdbcColumnMetadata> columns = new LinkedHashMap<>();
        try (ResultSet rs =
                metadata.getColumns(
                        dialect.metadataCatalog(connection, tableId),
                        dialect.metadataSchema(connection, tableId),
                        tableId.getTableName(),
                        null)) {
            while (rs.next()) {
                int nullableCode = rs.getInt("NULLABLE");
                boolean nullable = nullableCode != DatabaseMetaData.columnNoNulls;
                columns.put(
                        rs.getString("COLUMN_NAME"),
                        new JdbcColumnMetadata(
                                rs.getInt("DATA_TYPE"),
                                rs.getString("TYPE_NAME"),
                                rs.getInt("COLUMN_SIZE"),
                                rs.getInt("DECIMAL_DIGITS"),
                                nullable));
            }
        }
        return columns;
    }

    private List<String> readPrimaryKeys(Connection connection, TableId tableId)
            throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        Map<Short, String> bySequence = new TreeMap<>();
        try (ResultSet rs =
                metadata.getPrimaryKeys(
                        dialect.metadataCatalog(connection, tableId),
                        dialect.metadataSchema(connection, tableId),
                        tableId.getTableName())) {
            while (rs.next()) {
                bySequence.put(rs.getShort("KEY_SEQ"), rs.getString("COLUMN_NAME"));
            }
        }
        return new ArrayList<>(bySequence.values());
    }
}
