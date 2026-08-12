package io.yak.flink.cdc.connectors.jdbc.runtime;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcFailureClassifierTest {

    @Test
    void retriesExplicitTransientAndRecoverableFailures() {
        assertThat(JdbcFailureClassifier.isRetryable(new SQLTransientException("busy"))).isTrue();
        assertThat(JdbcFailureClassifier.isRetryable(new SQLRecoverableException("connection lost")))
                .isTrue();
    }

    @Test
    void retriesConnectionAndTransactionRollbackSqlStates() {
        assertThat(JdbcFailureClassifier.isRetryable(new SQLException("connection", "08006")))
                .isTrue();
        assertThat(JdbcFailureClassifier.isRetryable(new SQLException("serialization", "40001")))
                .isTrue();
    }

    @Test
    void failsFastForConstraintSyntaxAndTypeErrors() {
        assertThat(JdbcFailureClassifier.isRetryable(new SQLException("duplicate key", "23505")))
                .isFalse();
        assertThat(JdbcFailureClassifier.isRetryable(new SQLException("syntax", "42601")))
                .isFalse();
        assertThat(JdbcFailureClassifier.isRetryable(new SQLException("invalid cast", "22018")))
                .isFalse();
    }

    @Test
    void inspectsChainedJdbcExceptions() {
        SQLException root = new SQLException("batch failed", "HY000");
        root.setNextException(new SQLRecoverableException("connection reset", "08006"));

        assertThat(JdbcFailureClassifier.isRetryable(root)).isTrue();
    }
}
