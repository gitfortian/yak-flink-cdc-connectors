package io.yak.flink.cdc.connectors.jdbc.runtime;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;

/** Conservative JDBC failure classification shared by production write paths. */
public final class JdbcFailureClassifier {
    private static final long INITIAL_RETRY_BACKOFF_MILLIS = 100L;
    private static final long MAX_RETRY_BACKOFF_MILLIS = 1_000L;

    private JdbcFailureClassifier() {}

    /**
     * Retry only failures that JDBC or SQLState explicitly identify as transient/recoverable.
     *
     * <p>SQLState class 08 is a connection exception and class 40 is transaction rollback. Syntax,
     * constraint, type and permission errors are deliberately not retried because repeating the
     * same batch cannot make them valid.
     */
    public static boolean isRetryable(SQLException failure) {
        for (SQLException current = failure; current != null; current = current.getNextException()) {
            if (current instanceof SQLTransientException
                    || current instanceof SQLRecoverableException) {
                return true;
            }
            String sqlState = current.getSQLState();
            if (sqlState != null
                    && (sqlState.startsWith("08") || sqlState.startsWith("40"))) {
                return true;
            }
        }
        return false;
    }

    public static void sleepBeforeRetry(int attempt) throws InterruptedException {
        long shift = Math.min(attempt, 4);
        long backoff =
                Math.min(
                        MAX_RETRY_BACKOFF_MILLIS,
                        INITIAL_RETRY_BACKOFF_MILLIS * (1L << shift));
        Thread.sleep(backoff);
    }
}
