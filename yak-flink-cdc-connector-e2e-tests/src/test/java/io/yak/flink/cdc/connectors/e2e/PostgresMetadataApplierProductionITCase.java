package io.yak.flink.cdc.connectors.e2e;

import io.yak.flink.cdc.connectors.jdbc.JdbcSinkConfig;
import io.yak.flink.cdc.connectors.jdbc.sink.YakJdbcMetadataApplier;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Production invariants for event-level planning and post-condition verification. */
class PostgresMetadataApplierProductionITCase {
    private static final String DATABASE = "metadata_applier_db";
    private static final String SCHEMA_NAME = "metadata_applier";
    private static final String USER = "postgres";
    private static final String PASSWORD = "postgres";
    private static final TableId TABLE_ID = TableId.tableId(SCHEMA_NAME, "customers");

    private static final GenericContainer<?> POSTGRES =
            new GenericContainer<>(DockerImageName.parse("postgres:16.4"))
                    .withEnv("POSTGRES_DB", DATABASE)
                    .withEnv("POSTGRES_USER", USER)
                    .withEnv("POSTGRES_PASSWORD", PASSWORD)
                    .withExposedPorts(5432)
                    .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));

    @BeforeAll
    static void beforeAll() throws Exception {
        POSTGRES.start();
        awaitJdbc();
    }

    @AfterAll
    static void afterAll() {
        POSTGRES.stop();
    }

    @BeforeEach
    void resetSchema() throws SQLException {
        try (Connection connection = openConnection(); Statement statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA IF EXISTS \"" + SCHEMA_NAME + "\" CASCADE");
            statement.execute("CREATE SCHEMA \"" + SCHEMA_NAME + "\"");
        }
    }

    @Test
    void preflightsEveryStepBeforeMutatingAMultiColumnEvent() throws SQLException {
        YakJdbcMetadataApplier applier = standardApplier();
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema()));

        // The second requested column is already present with an incompatible definition. The first
        // requested column is intentionally valid and absent. A sequential executor would add the
        // first column before discovering the second-column conflict.
        try (Connection connection = openConnection(); Statement statement = connection.createStatement()) {
            statement.execute(
                    "ALTER TABLE \""
                            + SCHEMA_NAME
                            + "\".\"customers\" ADD COLUMN conflicting_column INTEGER");
        }

        AddColumnEvent event =
                new AddColumnEvent(
                        TABLE_ID,
                        Arrays.asList(
                                AddColumnEvent.last(
                                        Column.physicalColumn(
                                                "safe_column", DataTypes.VARCHAR(16))),
                                AddColumnEvent.last(
                                        Column.physicalColumn(
                                                "conflicting_column", DataTypes.VARCHAR(16)))));

        assertThatThrownBy(() -> applier.applySchemaChange(event))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("conflicts with target metadata")
                .hasMessageContaining("conflicting_column");

        assertThat(columnExists("safe_column"))
                .as("event-level preflight must prevent partial schema mutation")
                .isFalse();
        assertThat(columnExists("conflicting_column")).isTrue();
    }

    @Test
    void rejectsAcknowledgedDdlWhenFreshMetadataDoesNotMeetThePostCondition() throws SQLException {
        YakJdbcMetadataApplier applier =
                new YakJdbcMetadataApplier(
                        new JdbcSinkConfig(
                                falseAckJdbcUrl(),
                                FalseSuccessPostgresDriver.class.getName(),
                                USER,
                                PASSWORD,
                                "postgres",
                                3));

        FalseSuccessPostgresDriver.acknowledgeWithoutExecutingNextDdl();

        assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new CreateTableEvent(TABLE_ID, initialSchema())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("returned success but target post-condition was not met")
                .hasMessageContaining("create table");

        assertThat(tableExists()).isFalse();
    }

    private static YakJdbcMetadataApplier standardApplier() {
        return new YakJdbcMetadataApplier(
                new JdbcSinkConfig(
                        jdbcUrl(),
                        "org.postgresql.Driver",
                        USER,
                        PASSWORD,
                        "postgres",
                        3));
    }

    private static Schema initialSchema() {
        return Schema.newBuilder()
                .physicalColumn("id", DataTypes.INT().notNull())
                .physicalColumn("name", DataTypes.VARCHAR(64).notNull())
                .primaryKey("id")
                .build();
    }

    private static boolean tableExists() throws SQLException {
        try (Connection connection = openConnection();
                ResultSet rs =
                        connection
                                .getMetaData()
                                .getTables(
                                        null,
                                        SCHEMA_NAME,
                                        TABLE_ID.getTableName(),
                                        new String[] {"TABLE"})) {
            return rs.next();
        }
    }

    private static boolean columnExists(String columnName) throws SQLException {
        try (Connection connection = openConnection();
                ResultSet rs =
                        connection
                                .getMetaData()
                                .getColumns(
                                        null,
                                        SCHEMA_NAME,
                                        TABLE_ID.getTableName(),
                                        columnName)) {
            return rs.next();
        }
    }

    private static void awaitJdbc() throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        SQLException last = null;
        while (System.nanoTime() < deadline) {
            try (Connection ignored = openConnection()) {
                return;
            } catch (SQLException failure) {
                last = failure;
                Thread.sleep(250L);
            }
        }
        throw new SQLException("PostgreSQL did not become JDBC-ready", last);
    }

    private static Connection openConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl(), USER, PASSWORD);
    }

    private static String jdbcUrl() {
        return "jdbc:postgresql://"
                + POSTGRES.getHost()
                + ":"
                + POSTGRES.getMappedPort(5432)
                + "/"
                + DATABASE
                + "?connectTimeout=5&socketTimeout=5";
    }

    private static String falseAckJdbcUrl() {
        return jdbcUrl().replace("jdbc:postgresql:", "jdbc:yak-false-success:postgresql:");
    }

    /**
     * Test-only driver that acknowledges one executeUpdate call without forwarding it to
     * PostgreSQL. It proves that JDBC success alone is not sufficient for MetadataApplier success.
     */
    public static final class FalseSuccessPostgresDriver implements Driver {
        private static final String PREFIX = "jdbc:yak-false-success:";
        private static final AtomicBoolean ACK_WITHOUT_EXECUTING = new AtomicBoolean(false);

        static {
            try {
                DriverManager.registerDriver(new FalseSuccessPostgresDriver());
            } catch (SQLException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        static void acknowledgeWithoutExecutingNextDdl() {
            ACK_WITHOUT_EXECUTING.set(true);
        }

        @Override
        public Connection connect(String url, Properties info) throws SQLException {
            if (!acceptsURL(url)) {
                return null;
            }
            String delegateUrl = "jdbc:" + url.substring(PREFIX.length());
            return wrapConnection(DriverManager.getConnection(delegateUrl, info));
        }

        @Override
        public boolean acceptsURL(String url) {
            return url != null && url.startsWith(PREFIX);
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public Logger getParentLogger() {
            return Logger.getLogger(FalseSuccessPostgresDriver.class.getName());
        }

        private static Connection wrapConnection(Connection delegate) {
            return (Connection)
                    Proxy.newProxyInstance(
                            FalseSuccessPostgresDriver.class.getClassLoader(),
                            new Class<?>[] {Connection.class},
                            (proxy, method, args) -> {
                                Object result = invoke(delegate, method, args);
                                if ("createStatement".equals(method.getName())
                                        && result instanceof Statement) {
                                    return wrapStatement((Statement) result);
                                }
                                return result;
                            });
        }

        private static Statement wrapStatement(Statement delegate) {
            return (Statement)
                    Proxy.newProxyInstance(
                            FalseSuccessPostgresDriver.class.getClassLoader(),
                            new Class<?>[] {Statement.class},
                            (proxy, method, args) -> {
                                if ("executeUpdate".equals(method.getName())
                                        && ACK_WITHOUT_EXECUTING.compareAndSet(true, false)) {
                                    return 0;
                                }
                                return invoke(delegate, method, args);
                            });
        }

        private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
            try {
                return method.invoke(target, args);
            } catch (InvocationTargetException failure) {
                throw failure.getCause();
            }
        }
    }
}
