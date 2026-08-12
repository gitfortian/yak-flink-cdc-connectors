package io.yak.flink.cdc.connectors.jdbc.mysql;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MySqlJdbcDialectTest {

    private final MySqlJdbcDialect dialect = new MySqlJdbcDialect();

    @Test
    void buildsCreateUpsertAndDeleteStatements() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(64))
                        .primaryKey("id")
                        .build();
        TableId tableId = TableId.tableId("ods", "users");

        assertThat(dialect.buildCreateTableStatement(tableId, schema))
                .isEqualTo(
                        "CREATE TABLE IF NOT EXISTS `ods`.`users` (`id` BIGINT NOT NULL, `name` VARCHAR(64), PRIMARY KEY (`id`))");
        assertThat(dialect.buildUpsertStatement(tableId, schema))
                .isEqualTo(
                        "INSERT INTO `ods`.`users` (`id`, `name`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)");
        assertThat(dialect.buildDeleteStatement(tableId, schema))
                .isEqualTo("DELETE FROM `ods`.`users` WHERE `id` = ?");
    }
}
