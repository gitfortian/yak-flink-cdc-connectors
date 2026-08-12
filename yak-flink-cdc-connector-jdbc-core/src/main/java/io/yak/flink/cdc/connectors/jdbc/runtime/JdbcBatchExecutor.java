package io.yak.flink.cdc.connectors.jdbc.runtime;

import io.yak.flink.cdc.connectors.jdbc.JdbcSinkConfig;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Executes ordered JDBC batch segments inside one transaction per flush. */
public final class JdbcBatchExecutor implements AutoCloseable {
    private final JdbcSinkConfig config;
    private final JdbcConnectionProvider connectionProvider;
    private final Map<String, PreparedStatement> statementCache = new LinkedHashMap<>();

    private Connection connection;

    public JdbcBatchExecutor(JdbcSinkConfig config, JdbcConnectionProvider connectionProvider)
            throws IOException {
        this.config = config;
        this.connectionProvider = connectionProvider;
        try {
            this.connection = openConnection();
        } catch (SQLException failure) {
            throw new IOException("Unable to open JDBC batch connection", failure);
        }
    }

    public void execute(JdbcBatchBuffer buffer) throws IOException {
        if (buffer.isEmpty()) {
            return;
        }

        SQLException last = null;
        for (int attempt = 0; attempt <= config.getMaxRetries(); attempt++) {
            try {
                ensureConnection();
                executeSegments(buffer.getSegments());
                connection.commit();
                return;
            } catch (SQLException failure) {
                last = failure;
                rollbackQuietly();
                invalidateConnection();

                if (attempt == config.getMaxRetries()
                        || !JdbcFailureClassifier.isRetryable(failure)) {
                    break;
                }

                try {
                    JdbcFailureClassifier.sleepBeforeRetry(attempt);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "Interrupted while retrying JDBC batch flush. bufferedRecords="
                                    + buffer.size(),
                            interrupted);
                }
            }
        }

        throw new IOException(
                "JDBC batch flush failed. bufferedRecords="
                        + buffer.size()
                        + ", estimatedBytes="
                        + buffer.estimatedBytes()
                        + failureSummary(last),
                last);
    }

    private void executeSegments(List<JdbcBatchBuffer.BatchSegment> segments) throws SQLException {
        for (JdbcBatchBuffer.BatchSegment segment : segments) {
            PreparedStatement statement = statement(segment.getSql());
            statement.clearBatch();

            for (List<Object> row : segment.getRows()) {
                statement.clearParameters();
                for (int i = 0; i < row.size(); i++) {
                    statement.setObject(i + 1, row.get(i));
                }
                statement.addBatch();
            }

            int[] results = statement.executeBatch();
            validateBatchResult(segment, results);
            statement.clearBatch();
        }
    }

    private static void validateBatchResult(
            JdbcBatchBuffer.BatchSegment segment, int[] results) throws SQLException {
        if (results.length != segment.size()) {
            throw new SQLException(
                    "JDBC driver returned "
                            + results.length
                            + " batch results for "
                            + segment.size()
                            + " statements");
        }
        for (int result : results) {
            if (result == Statement.EXECUTE_FAILED) {
                throw new SQLException("JDBC driver reported EXECUTE_FAILED for batched statement");
            }
        }
    }

    private PreparedStatement statement(String sql) throws SQLException {
        PreparedStatement cached = statementCache.get(sql);
        if (cached == null || cached.isClosed()) {
            cached = connection.prepareStatement(sql);
            statementCache.put(sql, cached);
        }
        return cached;
    }

    private void ensureConnection() throws SQLException {
        if (connection == null || connection.isClosed() || !connection.isValid(2)) {
            invalidateConnection();
            connection = openConnection();
        }
    }

    private Connection openConnection() throws SQLException {
        Connection opened = connectionProvider.open();
        opened.setAutoCommit(false);
        return opened;
    }

    /** Close cached statements after a schema change so the next write prepares against new metadata. */
    public void invalidateStatements() {
        for (PreparedStatement statement : statementCache.values()) {
            try {
                statement.close();
            } catch (SQLException ignored) {
                // best effort
            }
        }
        statementCache.clear();
    }

    private void rollbackQuietly() {
        if (connection == null) {
            return;
        }
        try {
            connection.rollback();
        } catch (SQLException ignored) {
            // The connection may already be broken. A retry recreates the connection and replays
            // the full buffered batch under the connector's at-least-once contract.
        }
    }

    private void invalidateConnection() {
        invalidateStatements();
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ignored) {
                // best effort
            } finally {
                connection = null;
            }
        }
    }

    private static String failureSummary(SQLException failure) {
        if (failure == null) {
            return "";
        }
        return ", sqlState="
                + failure.getSQLState()
                + ", errorCode="
                + failure.getErrorCode();
    }

    @Override
    public void close() {
        invalidateConnection();
    }
}
