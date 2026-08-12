package io.yak.flink.cdc.connectors.jdbc.runtime;

import io.yak.flink.cdc.connectors.jdbc.JdbcSinkConfig;

import org.apache.flink.cdc.common.event.TableId;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Executes ordered JDBC batch segments inside one transaction per flush. */
public final class JdbcBatchExecutor implements AutoCloseable {
    private final JdbcSinkConfig config;
    private final JdbcConnectionProvider connectionProvider;
    private final LinkedHashMap<StatementKey, PreparedStatement> statementCache =
            new LinkedHashMap<>(16, 0.75f, true);

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
            PreparedStatement statement = statement(segment);
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

    private PreparedStatement statement(JdbcBatchBuffer.BatchSegment segment) throws SQLException {
        StatementKey key = new StatementKey(segment.getTableId(), segment.getSql());
        PreparedStatement cached = statementCache.get(key);
        if (cached != null && cached.isClosed()) {
            statementCache.remove(key);
            cached = null;
        }

        if (cached == null) {
            evictIfNecessary();
            cached = connection.prepareStatement(segment.getSql());
            statementCache.put(key, cached);
        }
        return cached;
    }

    private void evictIfNecessary() {
        if (statementCache.size() < config.getStatementCacheSize()) {
            return;
        }
        Iterator<Map.Entry<StatementKey, PreparedStatement>> iterator =
                statementCache.entrySet().iterator();
        if (!iterator.hasNext()) {
            return;
        }
        Map.Entry<StatementKey, PreparedStatement> eldest = iterator.next();
        closeStatementQuietly(eldest.getValue());
        iterator.remove();
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

    /**
     * Invalidates only statements belonging to one table after schema evolution. Other tables keep
     * their hot prepared statements and do not pay a re-prepare penalty.
     */
    public void invalidateStatements(TableId tableId) {
        Iterator<Map.Entry<StatementKey, PreparedStatement>> iterator =
                statementCache.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<StatementKey, PreparedStatement> entry = iterator.next();
            if (Objects.equals(entry.getKey().tableId, tableId)) {
                closeStatementQuietly(entry.getValue());
                iterator.remove();
            }
        }
    }

    /** Close every cached statement, primarily for connection replacement and writer shutdown. */
    public void invalidateStatements() {
        for (PreparedStatement statement : statementCache.values()) {
            closeStatementQuietly(statement);
        }
        statementCache.clear();
    }

    int cachedStatementCount() {
        return statementCache.size();
    }

    private static void closeStatementQuietly(PreparedStatement statement) {
        try {
            statement.close();
        } catch (SQLException ignored) {
            // best effort
        }
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

    private static final class StatementKey {
        private final TableId tableId;
        private final String sql;

        private StatementKey(TableId tableId, String sql) {
            this.tableId = tableId;
            this.sql = sql;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof StatementKey)) {
                return false;
            }
            StatementKey that = (StatementKey) o;
            return Objects.equals(tableId, that.tableId) && sql.equals(that.sql);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableId, sql);
        }
    }
}
