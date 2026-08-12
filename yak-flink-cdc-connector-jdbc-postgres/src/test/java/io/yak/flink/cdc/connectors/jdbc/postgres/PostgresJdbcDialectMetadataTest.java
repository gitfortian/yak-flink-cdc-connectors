package io.yak.flink.cdc.connectors.jdbc.postgres;

import io.yak.flink.cdc.connectors.jdbc.dialect.JdbcColumnMetadata;

import org.apache.flink.cdc.common.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;

class PostgresJdbcDialectMetadataTest {
    private final PostgresJdbcDialect dialect = new PostgresJdbcDialect();

    @Test
    void matchesCompatibleColumnMetadata() {
        assertThat(
                        dialect.isColumnTypeCompatible(
                                DataTypes.INT().notNull(),
                                new JdbcColumnMetadata(
                                        Types.INTEGER, "int4", 10, 0, false)))
                .isTrue();
        assertThat(
                        dialect.isColumnTypeCompatible(
                                DataTypes.VARCHAR(64),
                                new JdbcColumnMetadata(
                                        Types.VARCHAR, "varchar", 64, 0, true)))
                .isTrue();
        assertThat(
                        dialect.isColumnTypeCompatible(
                                DataTypes.DECIMAL(18, 4),
                                new JdbcColumnMetadata(
                                        Types.NUMERIC, "numeric", 18, 4, true)))
                .isTrue();
    }

    @Test
    void rejectsWrongTypeShapeOrNullability() {
        assertThat(
                        dialect.isColumnTypeCompatible(
                                DataTypes.VARCHAR(64),
                                new JdbcColumnMetadata(
                                        Types.VARCHAR, "varchar", 32, 0, true)))
                .isFalse();
        assertThat(
                        dialect.isColumnTypeCompatible(
                                DataTypes.INT().notNull(),
                                new JdbcColumnMetadata(
                                        Types.INTEGER, "int4", 10, 0, true)))
                .isFalse();
    }
}
