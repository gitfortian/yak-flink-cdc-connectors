package io.yak.flink.cdc.connectors.jdbc.postgres;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PostgresJdbcDialectTest {

    private final PostgresJdbcDialect dialect = new PostgresJdbcDialect();

    @Test
    void buildsCreateUpsertAndDeleteStatements() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(64))
                        .primaryKey("id")
                        .build();
        TableId tableId = TableId.tableId("public", "users");

        assertThat(dialect.buildCreateTableStatement(tableId, schema))
                .isEqualTo(
                        "CREATE TABLE IF NOT EXISTS \"public\".\"users\" (\"id\" BIGINT NOT NULL, \"name\" VARCHAR(64), PRIMARY KEY (\"id\"))");
        assertThat(dialect.buildUpsertStatement(tableId, schema))
                .isEqualTo(
                        "INSERT INTO \"public\".\"users\" (\"id\", \"name\") VALUES (?, ?) ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\"");
        assertThat(dialect.buildDeleteStatement(tableId, schema))
                .isEqualTo("DELETE FROM \"public\".\"users\" WHERE \"id\" = ?");
    }
}
