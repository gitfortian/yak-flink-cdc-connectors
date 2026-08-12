package io.yak.flink.cdc.connectors.jdbc;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcSinkConfigTest {

    @Test
    void legacyConstructorKeepsProductionBatchDefaults() {
        JdbcSinkConfig config =
                new JdbcSinkConfig(
                        "jdbc:postgresql://localhost/db",
                        "org.postgresql.Driver",
                        "user",
                        "password",
                        "postgres",
                        3);

        assertThat(config.getBatchSize()).isEqualTo(1000);
        assertThat(config.getFlushIntervalMillis()).isEqualTo(2000L);
    }

    @Test
    void validatesBatchConfiguration() {
        assertThatThrownBy(
                        () ->
                                new JdbcSinkConfig(
                                        "jdbc:postgresql://localhost/db",
                                        "org.postgresql.Driver",
                                        "user",
                                        "password",
                                        "postgres",
                                        3,
                                        0,
                                        2000L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch-size");

        assertThatThrownBy(
                        () ->
                                new JdbcSinkConfig(
                                        "jdbc:postgresql://localhost/db",
                                        "org.postgresql.Driver",
                                        "user",
                                        "password",
                                        "postgres",
                                        3,
                                        1000,
                                        -1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("flush-interval-ms");
    }
}
