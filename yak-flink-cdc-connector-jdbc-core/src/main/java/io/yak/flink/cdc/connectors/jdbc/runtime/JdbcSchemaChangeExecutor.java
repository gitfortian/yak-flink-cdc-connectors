package io.yak.flink.cdc.connectors.jdbc.runtime;

import io.yak.flink.cdc.connectors.jdbc.JdbcSinkConfig;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/** Executes a schema-change plan with event-level preflight and per-step post-condition checks. */
public final class JdbcSchemaChangeExecutor {
    private static final long INITIAL_RETRY_BACKOFF_MILLIS = 100L;
    private static final long MAX_RETRY_BACKOFF_MILLIS = 1_000L;

    private final JdbcSinkConfig config;
    private final JdbcConnectionProvider connectionProvider;

    public JdbcSchemaChangeExecutor(
            JdbcSinkConfig config, JdbcConnectionProvider connectionProvider) {
        this.config = config;
        this.connectionProvider = connectionProvider;
    }

    public void execute(SchemaChangePlan plan) {
        if (plan.isNoOp()) {
            return;
        }

        List<SchemaChangeInspectionResult> initialStates = preflight(plan);
        for (int i = 0; i < plan.getSteps().size(); i++) {
            SchemaChangeStep step = plan.getSteps().get(i);
            SchemaChangeInspectionResult initialState = initialStates.get(i);
            if (step.isStateReconciled() && initialState.isApplied()) {
                continue;
            }
            executeStep(plan, step);
        }
    }

    /**
     * Inspect every independently observable step before mutating the target. This prevents a
     * multi-column event from applying its first DDL and only then discovering that a later step is
     * already in conflict.
     */
    private List<SchemaChangeInspectionResult> preflight(SchemaChangePlan plan) {
        List<SchemaChangeInspectionResult> states = new ArrayList<>(plan.getSteps().size());
        for (SchemaChangeStep step : plan.getSteps()) {
            if (!step.isStateReconciled()) {
                states.add(
                        SchemaChangeInspectionResult.notApplied(
                                "replay-safe step has no metadata precondition"));
                continue;
            }

            SchemaChangeInspectionResult state = inspectWithRetry(plan, step, "preflight");
            if (state.isConflict()) {
                throw schemaConflict(plan, step, state, null);
            }
            states.add(state);
        }
        return states;
    }

    private void executeStep(SchemaChangePlan plan, SchemaChangeStep step) {
        SQLException last = null;
        for (int attempt = 0; attempt <= config.getMaxRetries(); attempt++) {
            try (Connection connection = connectionProvider.open()) {
                if (step.isStateReconciled()) {
                    SchemaChangeInspectionResult before = step.inspect(connection);
                    if (before.isApplied()) {
                        return;
                    }
                    if (before.isConflict()) {
                        throw schemaConflict(plan, step, before, null);
                    }
                }

                try (Statement statement = connection.createStatement()) {
                    statement.executeUpdate(step.getSql());
                }
            } catch (SQLException failure) {
                last = failure;

                if (step.isStateReconciled()) {
                    SchemaChangeInspectionResult after = inspectAfterFailure(step, failure);
                    if (after != null && after.isApplied()) {
                        return;
                    }
                    if (after != null && after.isConflict()) {
                        throw schemaConflict(plan, step, after, failure);
                    }
                }

                if (attempt == config.getMaxRetries() || !isRetryableDdlFailure(failure)) {
                    break;
                }
                sleepBeforeRetry(attempt);
                continue;
            }

            if (!step.isStateReconciled()) {
                return;
            }

            // An acknowledged DDL is not considered complete until a fresh connection observes its
            // target metadata post-condition. DDL is rare; correctness is more important than the
            // extra metadata round trip here.
            SchemaChangeInspectionResult afterSuccess =
                    inspectWithRetry(plan, step, "post-condition verification");
            if (afterSuccess.isApplied()) {
                return;
            }
            if (afterSuccess.isConflict()) {
                throw schemaConflict(plan, step, afterSuccess, null);
            }
            throw new IllegalStateException(
                    "JDBC schema change returned success but target post-condition was not met for "
                            + step.getDescription()
                            + " in "
                            + plan.describe()
                            + ". "
                            + afterSuccess.getDetail());
        }

        throw new IllegalStateException(
                "JDBC schema change failed for "
                        + step.getDescription()
                        + " in "
                        + plan.describe()
                        + ". SQL: "
                        + step.getSql(),
                last);
    }

    private SchemaChangeInspectionResult inspectWithRetry(
            SchemaChangePlan plan, SchemaChangeStep step, String phase) {
        SQLException last = null;
        for (int attempt = 0; attempt <= config.getMaxRetries(); attempt++) {
            try (Connection connection = connectionProvider.open()) {
                return step.inspect(connection);
            } catch (SQLException failure) {
                last = failure;
                if (attempt == config.getMaxRetries() || !isRetryableDdlFailure(failure)) {
                    break;
                }
                sleepBeforeRetry(attempt);
            }
        }
        throw new IllegalStateException(
                "JDBC schema metadata "
                        + phase
                        + " failed for "
                        + step.getDescription()
                        + " in "
                        + plan.describe(),
                last);
    }

    private SchemaChangeInspectionResult inspectAfterFailure(
            SchemaChangeStep step, SQLException originalFailure) {
        try (Connection connection = connectionProvider.open()) {
            return step.inspect(connection);
        } catch (SQLException inspectionFailure) {
            originalFailure.addSuppressed(inspectionFailure);
            return null;
        }
    }

    private static IllegalStateException schemaConflict(
            SchemaChangePlan plan,
            SchemaChangeStep step,
            SchemaChangeInspectionResult result,
            SQLException cause) {
        String message =
                "JDBC schema change conflicts with target metadata during "
                        + step.getDescription()
                        + " in "
                        + plan.describe()
                        + ". "
                        + result.getDetail()
                        + ". SQL: "
                        + step.getSql();
        return cause == null
                ? new IllegalStateException(message)
                : new IllegalStateException(message, cause);
    }

    public static boolean isRetryableDdlFailure(SQLException failure) {
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

    private static void sleepBeforeRetry(int attempt) {
        long shift = Math.min(attempt, 4);
        long backoff =
                Math.min(
                        MAX_RETRY_BACKOFF_MILLIS,
                        INITIAL_RETRY_BACKOFF_MILLIS * (1L << shift));
        try {
            Thread.sleep(backoff);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while waiting to retry JDBC schema change", interrupted);
        }
    }
}
