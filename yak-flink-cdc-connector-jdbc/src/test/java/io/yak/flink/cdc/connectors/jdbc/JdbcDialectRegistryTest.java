package io.yak.flink.cdc.connectors.jdbc;

import io.yak.flink.cdc.connectors.jdbc.dialect.JdbcDialect;
import io.yak.flink.cdc.connectors.jdbc.dialect.JdbcDialectRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcDialectRegistryTest {

    @Test
    void discoversMysqlAndPostgresByJdbcUrl() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        JdbcDialect mysql =
                JdbcDialectRegistry.discover(
                        "auto", "jdbc:mysql://localhost:3306/demo", classLoader);
        JdbcDialect postgres =
                JdbcDialectRegistry.discover(
                        "auto", "jdbc:postgresql://localhost:5432/demo", classLoader);

        assertThat(mysql.identifier()).isEqualTo("mysql");
        assertThat(postgres.identifier()).isEqualTo("postgres");
    }

    @Test
    void connectorUsesDedicatedFactoryIdentifier() {
        assertThat(new YakJdbcDataSinkFactory().identifier()).isEqualTo("yak-jdbc");
    }
}
