package io.yak.flink.cdc.connectors.e2e;

import io.yak.flink.cdc.connectors.jdbc.JdbcSinkConfig;
import io.yak.flink.cdc.connectors.jdbc.runtime.JdbcBatchBuffer;
import io.yak.flink.cdc.connectors.jdbc.runtime.JdbcBatchExecutor;
import io.yak.flink.cdc.connectors.jdbc.runtime.JdbcConnectionProvider;
import io.yak.flink.cdc.connectors.jdbc.sink.YakJdbcWriter;

import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real PostgreSQL gate proving production JDBC batch execution semantics. */
class PostgresBatchExecutorITCase {
    private static final String DATABASE = "batch_test_db";
    private static final String SCHEMA = "batch_test";
    private static final String USER = "postgres";
    private static final String PASSWORD = "postgres";

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
    void resetTable() throws SQLException {
        CountingBatchPostgresDriver.reset();
        try (Connection connection = openConnection(); Statement statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA IF EXISTS \"" + SCHEMA + "\" CASCADE");
            statement.execute("CREATE SCHEMA \"" + SCHEMA + "\"");
            statement.execute(
                    "CREATE TABLE \""
                            + SCHEMA
                            + "\".records (id INTEGER PRIMARY KEY, name VARCHAR(8192) NOT NULL)");
        }
    }

    @Test
    void usesExecuteBatchTransactionAndPreparedStatementCacheAcrossFlushes() throws Exception {
        JdbcSinkConfig config =
                new JdbcSinkConfig(
                        countingJdbcUrl(),
                        CountingBatchPostgresDriver.class.getName(),
                        USER,
                        PASSWORD,
                        "postgres",
                        3,
                        1000,
                        2000L);

        String upsert =
                "INSERT INTO \""
                        + SCHEMA
                        + "\".records (id, name) VALUES (?, ?) "
                        + "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name";

        try (JdbcBatchExecutor executor =
                new JdbcBatchExecutor(config, new JdbcConnectionProvider(config))) {
            JdbcBatchBuffer first = new JdbcBatchBuffer();
            for (int i = 1; i <= 500; i++) {
                first.add(upsert, Arrays.asList(i, "name-" + i));
            }
            executor.execute(first);

            assertThat(rowCount()).isEqualTo(500);
            assertThat(CountingBatchPostgresDriver.PREPARES.get()).isEqualTo(1);
            assertThat(CountingBatchPostgresDriver.EXECUTE_BATCHES.get()).isEqualTo(1);
            assertThat(CountingBatchPostgresDriver.EXECUTE_UPDATES.get()).isZero();
            assertThat(CountingBatchPostgresDriver.COMMITS.get()).isEqualTo(1);

            JdbcBatchBuffer second = new JdbcBatchBuffer();
            second.add(upsert, Arrays.asList(1, "updated-1"));
            second.add(upsert, Arrays.asList(501, "name-501"));
            executor.execute(second);

            assertThat(rowCount()).isEqualTo(501);
            assertThat(nameFor(1)).isEqualTo("updated-1");
            assertThat(CountingBatchPostgresDriver.PREPARES.get())
                    .as("the same PreparedStatement should be reused across flushes")
                    .isEqualTo(1);
            assertThat(CountingBatchPostgresDriver.EXECUTE_BATCHES.get()).isEqualTo(2);
            assertThat(CountingBatchPostgresDriver.EXECUTE_UPDATES.get()).isZero();
            assertThat(CountingBatchPostgresDriver.COMMITS.get()).isEqualTo(2);
        }
    }

    @Test
    void writerFlushesOnByteBoundaryBeforeRecordCountBoundary() throws Exception {
        JdbcSinkConfig config =
                new JdbcSinkConfig(
                        countingJdbcUrl(),
                        CountingBatchPostgresDriver.class.getName(),
                        USER,
                        PASSWORD,
                        "postgres",
                        3,
                        1000,
                        0L,
                        3000L);
        TableId tableId = TableId.tableId(SCHEMA, "records");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(8192).notNull())
                        .primaryKey("id")
                        .build();
        String payload = String.join("", java.util.Collections.nCopies(700, "x"));

        try (YakJdbcWriter writer = new YakJdbcWriter(config)) {
            writer.write(new CreateTableEvent(tableId, schema), null);
            for (int id = 1; id <= 3; id++) {
                writer.write(
                        DataChangeEvent.insertEvent(
                                tableId,
                                GenericRecordData.of(
                                        id, BinaryStringData.fromString(payload + id))),
                        null);
            }
        }

        assertThat(rowCount()).isEqualTo(3);
        assertThat(CountingBatchPostgresDriver.EXECUTE_BATCHES.get())
                .as("max-batch-bytes should flush large rows long before batch-size=1000")
                .isEqualTo(3);
        assertThat(CountingBatchPostgresDriver.COMMITS.get()).isEqualTo(3);
        assertThat(CountingBatchPostgresDriver.PREPARES.get())
                .as("byte-triggered flushes should still reuse the prepared statement")
                .isEqualTo(1);
    }

    @Test
    void preservesInterleavedCdcOperationOrderAcrossBatchSegments() throws Exception {
        JdbcSinkConfig config =
                new JdbcSinkConfig(
                        countingJdbcUrl(),
                        CountingBatchPostgresDriver.class.getName(),
                        USER,
                        PASSWORD,
                        "postgres",
                        3,
                        1000,
                        2000L);

        String upsert =
                "INSERT INTO \""
                        + SCHEMA
                        + "\".records (id, name) VALUES (?, ?) "
                        + "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name";
        String delete = "DELETE FROM \"" + SCHEMA + "\".records WHERE id = ?";

        JdbcBatchBuffer buffer = new JdbcBatchBuffer();
        buffer.add(upsert, Arrays.asList(7, "first"));
        buffer.add(delete, Arrays.asList(7));
        buffer.add(upsert, Arrays.asList(7, "final"));

        try (JdbcBatchExecutor executor =
                new JdbcBatchExecutor(config, new JdbcConnectionProvider(config))) {
            executor.execute(buffer);
        }

        assertThat(nameFor(7)).isEqualTo("final");
        assertThat(CountingBatchPostgresDriver.EXECUTE_BATCHES.get()).isEqualTo(3);
        assertThat(CountingBatchPostgresDriver.COMMITS.get()).isEqualTo(1);
    }

    @Test
    void permanentConstraintFailureRollsBackAndDoesNotRetryBatch() throws Exception {
        JdbcSinkConfig config =
                new JdbcSinkConfig(
                        countingJdbcUrl(),
                        CountingBatchPostgresDriver.class.getName(),
                        USER,
                        PASSWORD,
                        "postgres",
                        5,
                        1000,
                        2000L);

        String insert =
                "INSERT INTO \"" + SCHEMA + "\".records (id, name) VALUES (?, ?)";
        JdbcBatchBuffer buffer = new JdbcBatchBuffer();
        buffer.add(insert, Arrays.asList(1, "first"));
        buffer.add(insert, Arrays.asList(1, "duplicate"));

        try (JdbcBatchExecutor executor =
                new JdbcBatchExecutor(config, new JdbcConnectionProvider(config))) {
            assertThatThrownBy(() -> executor.execute(buffer))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("JDBC batch flush failed");
        }

        assertThat(rowCount())
                .as("a permanent batch failure must roll back the whole JDBC transaction")
                .isZero();
        assertThat(CountingBatchPostgresDriver.EXECUTE_BATCHES.get())
                .as("SQLState 23 constraint failures must fail fast instead of consuming retries")
                .isEqualTo(1);
        assertThat(CountingBatchPostgresDriver.COMMITS.get()).isZero();
        assertThat(CountingBatchPostgresDriver.ROLLBACKS.get()).isEqualTo(1);
    }

    private static int rowCount() throws SQLException {
        try (Connection connection = openConnection();
                Statement statement = connection.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                "SELECT COUNT(*) FROM \"" + SCHEMA + "\".records")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private static String nameFor(int id) throws SQLException {
        try (Connection connection = openConnection();
                PreparedStatement statement =
                        connection.prepareStatement(
                                "SELECT name FROM \"" + SCHEMA + "\".records WHERE id = ?")) {
            statement.setInt(1, id);
            try (ResultSet rs = statement.executeQuery()) {
                assertThat(rs.next()).isTrue();
                return rs.getString(1);
            }
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

    private static String countingJdbcUrl() {
        return jdbcUrl().replace("jdbc:postgresql:", "jdbc:yak-batch:postgresql:");
    }

    /** Test-only JDBC wrapper that records which write APIs the production executor actually uses. */
    public static final class CountingBatchPostgresDriver implements Driver {
        private static final String PREFIX = "jdbc:yak-batch:";
        static final AtomicInteger PREPARES = new AtomicInteger();
        static final AtomicInteger EXECUTE_BATCHES = new AtomicInteger();
        static final AtomicInteger EXECUTE_UPDATES = new AtomicInteger();
        static final AtomicInteger COMMITS = new AtomicInteger();
        static final AtomicInteger ROLLBACKS = new AtomicInteger();

        static {
            try {
                DriverManager.registerDriver(new CountingBatchPostgresDriver());
            } catch (SQLException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        static void reset() {
            PREPARES.set(0);
            EXECUTE_BATCHES.set(0);
            EXECUTE_UPDATES.set(0);
            COMMITS.set(0);
            ROLLBACKS.set(0);
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
            return Logger.getLogger(CountingBatchPostgresDriver.class.getName());
        }

        private static Connection wrapConnection(Connection delegate) {
            return (Connection)
                    Proxy.newProxyInstance(
                            CountingBatchPostgresDriver.class.getClassLoader(),
                            new Class<?>[] {Connection.class},
                            (proxy, method, args) -> {
                                if ("prepareStatement".equals(method.getName())) {
                                    Object result = invoke(delegate, method, args);
                                    PREPARES.incrementAndGet();
                                    return wrapPreparedStatement((PreparedStatement) result);
                                }
                                if ("commit".equals(method.getName())) {
                                    Object result = invoke(delegate, method, args);
                                    COMMITS.incrementAndGet();
                                    return result;
                                }
                                if ("rollback".equals(method.getName())) {
                                    Object result = invoke(delegate, method, args);
                                    ROLLBACKS.incrementAndGet();
                                    return result;
                                }
                                return invoke(delegate, method, args);
                            });
        }

        private static PreparedStatement wrapPreparedStatement(PreparedStatement delegate) {
            return (PreparedStatement)
                    Proxy.newProxyInstance(
                            CountingBatchPostgresDriver.class.getClassLoader(),
                            new Class<?>[] {PreparedStatement.class},
                            (proxy, method, args) -> {
                                if ("executeBatch".equals(method.getName())) {
                                    EXECUTE_BATCHES.incrementAndGet();
                                } else if ("executeUpdate".equals(method.getName())) {
                                    EXECUTE_UPDATES.incrementAndGet();
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
