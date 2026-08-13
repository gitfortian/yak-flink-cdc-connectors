package io.yak.flink.cdc.connectors.jdbc;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcSinkConfigTest {

    @Test
    void legacyConstructorKeepsProductionDefaults() {
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
        assertThat(config.getMaxBatchBytes()).isEqualTo(16L * 1024L * 1024L);
        assertThat(config.getStatementCacheSize()).isEqualTo(128);
        assertThat(config.getReplaySafetyMode()).isEqualTo(ReplaySafetyMode.STRICT);
    }

    @Test
    void validatesProductionWriterConfiguration() {
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
                                        2000L,
                                        0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max-batch-bytes");

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
                                        2000L,
                                        16L * 1024L * 1024L,
                                        0,
                                        ReplaySafetyMode.STRICT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("statement-cache-size");
    }

    @Test
    void parsesOnlyExplicitReplaySafetyModes() {
        assertThat(ReplaySafetyMode.fromOption("strict")).isEqualTo(ReplaySafetyMode.STRICT);
        assertThat(ReplaySafetyMode.fromOption("ALLOW-APPEND-ONLY"))
                .isEqualTo(ReplaySafetyMode.ALLOW_APPEND_ONLY);

        assertThatThrownBy(() -> ReplaySafetyMode.fromOption("exactly-once"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("replay-safety");
    }
}
