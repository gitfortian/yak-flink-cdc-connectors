package io.yak.flink.cdc.connectors.jdbc.sink;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;

import static org.assertj.core.api.Assertions.assertThat;

class YakJdbcMetadataApplierTest {

    @Test
    void classifiesRecoverableAndTransientFailuresAsRetryable() {
        assertThat(
                        YakJdbcMetadataApplier.isRetryableDdlFailure(
                                new SQLRecoverableException("connection lost")))
                .isTrue();
        assertThat(
                        YakJdbcMetadataApplier.isRetryableDdlFailure(
                                new SQLTransientException("temporarily unavailable")))
                .isTrue();
        assertThat(
                        YakJdbcMetadataApplier.isRetryableDdlFailure(
                                new SQLException("connection failure", "08006")))
                .isTrue();
        assertThat(
                        YakJdbcMetadataApplier.isRetryableDdlFailure(
                                new SQLException("serialization failure", "40001")))
                .isTrue();
    }

    @Test
    void doesNotRetryPermanentSqlFailures() {
        assertThat(
                        YakJdbcMetadataApplier.isRetryableDdlFailure(
                                new SQLException("syntax error", "42601")))
                .isFalse();
        assertThat(
                        YakJdbcMetadataApplier.isRetryableDdlFailure(
                                new SQLException("duplicate object", "42710")))
                .isFalse();
        assertThat(
                        YakJdbcMetadataApplier.isRetryableDdlFailure(
                                new SQLException("permission denied", "42501")))
                .isFalse();
    }

    @Test
    void inspectsChainedSqlExceptions() {
        SQLException root = new SQLException("wrapper", "HY000");
        root.setNextException(new SQLException("connection lost", "08003"));

        assertThat(YakJdbcMetadataApplier.isRetryableDdlFailure(root)).isTrue();
    }
}
