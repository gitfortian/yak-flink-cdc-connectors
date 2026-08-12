package io.yak.flink.cdc.connectors.e2e;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.flink.FlinkPipelineComposer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Real MySQL CDC -> Yak JDBC -> PostgreSQL production E2E test. */
class MySqlToPostgresPipelineITCase {

    private static final String SOURCE_DATABASE = "app_db";
    private static final String SOURCE_TABLE = "customers";
    private static final String TARGET_DATABASE = "ods";
    private static final String TARGET_SCHEMA = SOURCE_DATABASE;
    private static final String MYSQL_PASSWORD = "root";
    private static final String POSTGRES_USER = "postgres";
    private static final String POSTGRES_PASSWORD = "postgres";
    private static final Duration CONVERGENCE_TIMEOUT = Duration.ofSeconds(120);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(500);

    private static final GenericContainer<?> MYSQL =
            new GenericContainer<>(DockerImageName.parse("mysql:8.0.40"))
                    .withEnv("MYSQL_ROOT_PASSWORD", MYSQL_PASSWORD)
                    .withEnv("MYSQL_ROOT_HOST", "%")
                    .withEnv("MYSQL_DATABASE", SOURCE_DATABASE)
                    .withExposedPorts(3306)
                    .withCommand(
                            "--server-id=223344",
                            "--log-bin=mysql-bin",
                            "--binlog-format=ROW",
                            "--binlog-row-image=FULL")
                    .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));

    private static final GenericContainer<?> POSTGRES =
            new GenericContainer<>(DockerImageName.parse("postgres:16.4"))
                    .withEnv("POSTGRES_DB", TARGET_DATABASE)
                    .withEnv("POSTGRES_USER", POSTGRES_USER)
                    .withEnv("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
                    .withExposedPorts(5432)
                    .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));

    private static final AtomicReference<Throwable> PIPELINE_FAILURE = new AtomicReference<>();
    private static final AtomicBoolean STOPPING = new AtomicBoolean(false);
    private static Thread pipelineThread;

    @BeforeAll
    static void beforeAll() throws Exception {
        MYSQL.start();
        POSTGRES.start();

        awaitJdbc("MySQL startup", MySqlToPostgresPipelineITCase::openMySqlConnection);
        awaitJdbc("PostgreSQL startup", MySqlToPostgresPipelineITCase::openPostgresConnection);

        initializeSource();
        initializeTarget();
        startPipeline();
    }

    @AfterAll
    static void afterAll() throws Exception {
        STOPPING.set(true);
        if (pipelineThread != null) {
            pipelineThread.interrupt();
            pipelineThread.join(5_000L);
        }
        POSTGRES.stop();
        MYSQL.stop();
    }

    @Test
    void snapshotDmlSchemaEvolutionAndConnectionRecovery() throws Exception {
        awaitTargetRows(
                "initial snapshot",
                Arrays.asList("1|Alice", "2|Bob", "3|Carol"),
                false);

        executeMySql(
                "INSERT INTO customers(id, name) VALUES (4, 'Dave')",
                "UPDATE customers SET name = 'Bobby' WHERE id = 2",
                "DELETE FROM customers WHERE id = 3");

        awaitTargetRows(
                "incremental insert/update/delete",
                Arrays.asList("1|Alice", "2|Bobby", "4|Dave"),
                false);

        executeMySql(
                "ALTER TABLE customers ADD COLUMN status VARCHAR(16) NULL",
                "UPDATE customers SET status = 'active'");

        awaitCondition(
                "PostgreSQL ADD COLUMN",
                () -> {
                    try (Connection connection = openPostgresConnection();
                            Statement statement = connection.createStatement();
                            ResultSet rs =
                                    statement.executeQuery(
                                            "SELECT COUNT(*) FROM information_schema.columns "
                                                    + "WHERE table_schema = '"
                                                    + TARGET_SCHEMA
                                                    + "' AND table_name = '"
                                                    + SOURCE_TABLE
                                                    + "' AND column_name = 'status'")) {
                        rs.next();
                        return rs.getInt(1) == 1;
                    }
                });

        awaitTargetRows(
                "schema-evolved updates",
                Arrays.asList("1|Alice|active", "2|Bobby|active", "4|Dave|active"),
                true);

        terminateSinkBackendConnections();
        executeMySql("INSERT INTO customers(id, name, status) VALUES (5, 'Eve', 'active')");

        awaitTargetRows(
                "write after PostgreSQL connection termination",
                Arrays.asList(
                        "1|Alice|active",
                        "2|Bobby|active",
                        "4|Dave|active",
                        "5|Eve|active"),
                true);
    }

    private static void initializeSource() throws SQLException {
        try (Connection connection = openMySqlConnection(); Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS customers");
            statement.execute(
                    "CREATE TABLE customers ("
                            + "id INT NOT NULL PRIMARY KEY, "
                            + "name VARCHAR(64) NOT NULL"
                            + ")");
            statement.execute(
                    "INSERT INTO customers(id, name) VALUES "
                            + "(1, 'Alice'), (2, 'Bob'), (3, 'Carol')");
        }
    }

    private static void initializeTarget() throws SQLException {
        try (Connection connection = openPostgresConnection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA IF NOT EXISTS \"" + TARGET_SCHEMA + "\"");
            statement.execute(
                    "DROP TABLE IF EXISTS \""
                            + TARGET_SCHEMA
                            + "\".\""
                            + SOURCE_TABLE
                            + "\"");
        }
    }

    private static void startPipeline() {
        Map<String, String> sourceOptions = new LinkedHashMap<>();
        sourceOptions.put("hostname", MYSQL.getHost());
        sourceOptions.put("port", String.valueOf(MYSQL.getMappedPort(3306)));
        sourceOptions.put("username", "root");
        sourceOptions.put("password", MYSQL_PASSWORD);
        sourceOptions.put("tables", SOURCE_DATABASE + "." + SOURCE_TABLE);
        sourceOptions.put("server-time-zone", "UTC");
        sourceOptions.put("server-id", "5400-5404");

        SourceDef sourceDef =
                new SourceDef("mysql", "MySQL E2E Source", Configuration.fromMap(sourceOptions));

        Map<String, String> sinkOptions = new LinkedHashMap<>();
        sinkOptions.put("url", postgresJdbcUrl());
        sinkOptions.put("driver", "org.postgresql.Driver");
        sinkOptions.put("username", POSTGRES_USER);
        sinkOptions.put("password", POSTGRES_PASSWORD);
        sinkOptions.put("dialect", "postgres");
        sinkOptions.put("max-retries", "5");

        SinkDef sinkDef =
                new SinkDef("yak-jdbc", "Yak JDBC PostgreSQL Sink", Configuration.fromMap(sinkOptions));

        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_NAME, "yak-jdbc-mysql-postgres-e2e");
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);

        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        PipelineExecution execution = FlinkPipelineComposer.ofMiniCluster().compose(pipelineDef);
        pipelineThread =
                new Thread(
                        () -> {
                            try {
                                execution.execute();
                            } catch (Throwable failure) {
                                if (!STOPPING.get() && !(failure instanceof InterruptedException)) {
                                    PIPELINE_FAILURE.compareAndSet(null, failure);
                                }
                            }
                        },
                        "yak-jdbc-production-e2e-pipeline");
        pipelineThread.setDaemon(true);
        pipelineThread.start();
    }

    private static void executeMySql(String... sqlStatements) throws SQLException {
        try (Connection connection = openMySqlConnection(); Statement statement = connection.createStatement()) {
            for (String sql : sqlStatements) {
                statement.execute(sql);
            }
        }
    }

    private static void terminateSinkBackendConnections() throws SQLException {
        int terminated = 0;
        try (Connection connection = openPostgresConnection();
                Statement statement = connection.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                "SELECT pg_terminate_backend(pid) "
                                        + "FROM pg_stat_activity "
                                        + "WHERE datname = current_database() "
                                        + "AND usename = current_user "
                                        + "AND pid <> pg_backend_pid()")) {
            while (rs.next()) {
                if (rs.getBoolean(1)) {
                    terminated++;
                }
            }
        }
        assertThat(terminated)
                .as("at least one PostgreSQL sink backend should be terminated")
                .isGreaterThan(0);
    }

    private static void awaitTargetRows(
            String phase, List<String> expectedRows, boolean includeStatus) throws Exception {
        awaitCondition(
                phase,
                () -> {
                    List<String> actual = readTargetRows(includeStatus);
                    return actual.equals(expectedRows);
                });

        assertThat(readTargetRows(includeStatus)).containsExactlyElementsOf(expectedRows);
    }

    private static List<String> readTargetRows(boolean includeStatus) throws SQLException {
        String columns = includeStatus ? "id, name, status" : "id, name";
        String sql =
                "SELECT "
                        + columns
                        + " FROM \""
                        + TARGET_SCHEMA
                        + "\".\""
                        + SOURCE_TABLE
                        + "\" ORDER BY id";

        List<String> rows = new ArrayList<>();
        try (Connection connection = openPostgresConnection();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                if (includeStatus) {
                    rows.add(rs.getInt(1) + "|" + rs.getString(2) + "|" + rs.getString(3));
                } else {
                    rows.add(rs.getInt(1) + "|" + rs.getString(2));
                }
            }
        }
        return rows;
    }

    private static void awaitCondition(String phase, CheckedBooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + CONVERGENCE_TIMEOUT.toNanos();
        Throwable lastObservationFailure = null;

        while (System.nanoTime() < deadline) {
            assertPipelineHealthy();
            try {
                if (condition.getAsBoolean()) {
                    return;
                }
                lastObservationFailure = null;
            } catch (SQLException | RuntimeException observationFailure) {
                lastObservationFailure = observationFailure;
            }
            Thread.sleep(POLL_INTERVAL.toMillis());
        }

        assertPipelineHealthy();
        String diagnostics;
        try {
            diagnostics = " targetRows=" + safeReadRowsForDiagnostics();
        } catch (Exception ignored) {
            diagnostics = " targetRows=<unavailable>";
        }
        fail(
                "Timed out waiting for E2E phase '"
                        + phase
                        + "'."
                        + diagnostics,
                lastObservationFailure);
    }

    private static void assertPipelineHealthy() {
        Throwable failure = PIPELINE_FAILURE.get();
        if (failure != null) {
            fail("Flink CDC Pipeline failed before E2E convergence", failure);
        }
    }

    private static List<String> safeReadRowsForDiagnostics() throws SQLException {
        try {
            return readTargetRows(true);
        } catch (SQLException evolvedReadFailure) {
            return readTargetRows(false);
        }
    }

    private static void awaitJdbc(String name, ConnectionFactory factory) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        SQLException last = null;
        while (System.nanoTime() < deadline) {
            try (Connection ignored = factory.open()) {
                return;
            } catch (SQLException e) {
                last = e;
                Thread.sleep(500L);
            }
        }
        throw new SQLException(name + " did not become JDBC-ready", last);
    }

    private static Connection openMySqlConnection() throws SQLException {
        return DriverManager.getConnection(mysqlJdbcUrl(), "root", MYSQL_PASSWORD);
    }

    private static Connection openPostgresConnection() throws SQLException {
        return DriverManager.getConnection(postgresJdbcUrl(), POSTGRES_USER, POSTGRES_PASSWORD);
    }

    private static String mysqlJdbcUrl() {
        return "jdbc:mysql://"
                + MYSQL.getHost()
                + ":"
                + MYSQL.getMappedPort(3306)
                + "/"
                + SOURCE_DATABASE
                + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    }

    private static String postgresJdbcUrl() {
        return "jdbc:postgresql://"
                + POSTGRES.getHost()
                + ":"
                + POSTGRES.getMappedPort(5432)
                + "/"
                + TARGET_DATABASE;
    }

    @FunctionalInterface
    private interface CheckedBooleanSupplier {
        boolean getAsBoolean() throws Exception;
    }

    @FunctionalInterface
    private interface ConnectionFactory {
        Connection open() throws SQLException;
    }
}
