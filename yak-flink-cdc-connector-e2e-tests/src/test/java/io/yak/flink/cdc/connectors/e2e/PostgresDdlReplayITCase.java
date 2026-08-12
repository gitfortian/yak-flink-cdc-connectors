package io.yak.flink.cdc.connectors.e2e;

import io.yak.flink.cdc.connectors.jdbc.JdbcSinkConfig;
import io.yak.flink.cdc.connectors.jdbc.sink.YakJdbcMetadataApplier;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
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
import java.sql.SQLRecoverableException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real PostgreSQL verification for replay-safe and conflict-safe JDBC schema evolution. */
class PostgresDdlReplayITCase {
    private static final String DATABASE = "ddl_test_db";
    private static final String SCHEMA_NAME = "ddl_test";
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

    private static YakJdbcMetadataApplier applier;

    @BeforeAll
    static void beforeAll() throws Exception {
        POSTGRES.start();
        awaitJdbc();
        applier = standardApplier();
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
    void replaysSchemaEventsWithoutTurningAlreadyAppliedDdlIntoFailure() throws Exception {
        Schema initialSchema = initialSchema();
        CreateTableEvent createTable = new CreateTableEvent(TABLE_ID, initialSchema);

        applyTwice(createTable);
        assertThat(columnTypeName("id")).isEqualTo("int4");
        assertThat(columnTypeName("name")).isEqualTo("varchar");

        Column status = Column.physicalColumn("status", DataTypes.VARCHAR(16));
        AddColumnEvent addColumn =
                new AddColumnEvent(TABLE_ID, Collections.singletonList(AddColumnEvent.last(status)));
        applyTwice(addColumn);
        assertThat(columnTypeName("status")).isEqualTo("varchar");
        assertThat(columnSize("status")).isEqualTo(16);

        RenameColumnEvent renameColumn =
                new RenameColumnEvent(TABLE_ID, Collections.singletonMap("status", "state"));
        applyTwice(renameColumn);
        assertThat(columnExists("status")).isFalse();
        assertThat(columnExists("state")).isTrue();

        Map<String, org.apache.flink.cdc.common.types.DataType> typeMapping = new LinkedHashMap<>();
        typeMapping.put("state", DataTypes.VARCHAR(32));
        AlterColumnTypeEvent alterColumnType = new AlterColumnTypeEvent(TABLE_ID, typeMapping);
        applyTwice(alterColumnType);
        assertThat(columnSize("state")).isEqualTo(32);

        DropColumnEvent dropColumn =
                new DropColumnEvent(TABLE_ID, Collections.singletonList("state"));
        applyTwice(dropColumn);
        assertThat(columnExists("state")).isFalse();

        DropTableEvent dropTable = new DropTableEvent(TABLE_ID);
        applyTwice(dropTable);
        assertThat(tableExists()).isFalse();
    }

    @Test
    void reconcilesDdlThatCommittedBeforeJdbcAcknowledgementWasLost() throws SQLException {
        YakJdbcMetadataApplier ambiguousApplier =
                new YakJdbcMetadataApplier(
                        new JdbcSinkConfig(
                                ambiguousJdbcUrl(),
                                AmbiguousCommitPostgresDriver.class.getName(),
                                USER,
                                PASSWORD,
                                "postgres",
                                3));

        AmbiguousCommitPostgresDriver.failAfterNextDdlCommit();
        assertThatCode(
                        () ->
                                ambiguousApplier.applySchemaChange(
                                        new CreateTableEvent(TABLE_ID, initialSchema())))
                .doesNotThrowAnyException();
        assertThat(tableExists()).isTrue();

        AmbiguousCommitPostgresDriver.failAfterNextDdlCommit();
        AddColumnEvent addStatus =
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("status", DataTypes.VARCHAR(16)))));
        assertThatCode(() -> ambiguousApplier.applySchemaChange(addStatus)).doesNotThrowAnyException();
        assertThat(columnExists("status")).isTrue();
        assertThat(columnSize("status")).isEqualTo(16);
    }

    @Test
    void rejectsCreateReplayWhenExistingTableHasDifferentSchema() throws SQLException {
        try (Connection connection = openConnection(); Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE \""
                            + SCHEMA_NAME
                            + "\".\"customers\" (id TEXT NOT NULL PRIMARY KEY, name VARCHAR(64) NOT NULL)");
        }

        assertThatThrownBy(() -> applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("conflicts with target metadata")
                .hasMessageContaining("id");
    }

    @Test
    void rejectsAddReplayWhenSameNamedColumnHasDifferentDefinition() throws SQLException {
        applier.applySchemaChange(new CreateTableEvent(TABLE_ID, initialSchema()));
        try (Connection connection = openConnection(); Statement statement = connection.createStatement()) {
            statement.execute(
                    "ALTER TABLE \""
                            + SCHEMA_NAME
                            + "\".\"customers\" ADD COLUMN status INTEGER");
        }

        AddColumnEvent expectedStatus =
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("status", DataTypes.VARCHAR(16)))));

        assertThatThrownBy(() -> applier.applySchemaChange(expectedStatus))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("conflicts with target metadata")
                .hasMessageContaining("status");
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

    private static void applyTwice(SchemaChangeEvent event) {
        assertThatCode(() -> applier.applySchemaChange(event)).doesNotThrowAnyException();
        assertThatCode(() -> applier.applySchemaChange(event)).doesNotThrowAnyException();
    }

    private static boolean tableExists() throws SQLException {
        try (Connection connection = openConnection();
                ResultSet rs =
                        connection
                                .getMetaData()
                                .getTables(null, SCHEMA_NAME, TABLE_ID.getTableName(), new String[] {"TABLE"})) {
            return rs.next();
        }
    }

    private static boolean columnExists(String columnName) throws SQLException {
        try (Connection connection = openConnection();
                ResultSet rs =
                        connection
                                .getMetaData()
                                .getColumns(null, SCHEMA_NAME, TABLE_ID.getTableName(), columnName)) {
            return rs.next();
        }
    }

    private static String columnTypeName(String columnName) throws SQLException {
        try (Connection connection = openConnection();
                ResultSet rs =
                        connection
                                .getMetaData()
                                .getColumns(null, SCHEMA_NAME, TABLE_ID.getTableName(), columnName)) {
            assertThat(rs.next()).isTrue();
            return rs.getString("TYPE_NAME");
        }
    }

    private static int columnSize(String columnName) throws SQLException {
        try (Connection connection = openConnection();
                ResultSet rs =
                        connection
                                .getMetaData()
                                .getColumns(null, SCHEMA_NAME, TABLE_ID.getTableName(), columnName)) {
            assertThat(rs.next()).isTrue();
            return rs.getInt("COLUMN_SIZE");
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

    private static String ambiguousJdbcUrl() {
        return jdbcUrl().replace("jdbc:postgresql:", "jdbc:yak-ambiguous:postgresql:");
    }

    /**
     * Test-only JDBC wrapper that executes the real PostgreSQL DDL and then loses its successful
     * acknowledgement once. This deterministically models the production ambiguity where the
     * server committed a schema change before the client observed a connection failure.
     */
    public static final class AmbiguousCommitPostgresDriver implements Driver {
        private static final String PREFIX = "jdbc:yak-ambiguous:";
        private static final AtomicBoolean FAIL_AFTER_DDL_COMMIT = new AtomicBoolean(false);

        static {
            try {
                DriverManager.registerDriver(new AmbiguousCommitPostgresDriver());
            } catch (SQLException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        static void failAfterNextDdlCommit() {
            FAIL_AFTER_DDL_COMMIT.set(true);
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
            return Logger.getLogger(AmbiguousCommitPostgresDriver.class.getName());
        }

        private static Connection wrapConnection(Connection delegate) {
            return (Connection)
                    Proxy.newProxyInstance(
                            AmbiguousCommitPostgresDriver.class.getClassLoader(),
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
                            AmbiguousCommitPostgresDriver.class.getClassLoader(),
                            new Class<?>[] {Statement.class},
                            (proxy, method, args) -> {
                                Object result = invoke(delegate, method, args);
                                if ("executeUpdate".equals(method.getName())
                                        && FAIL_AFTER_DDL_COMMIT.compareAndSet(true, false)) {
                                    throw new SQLRecoverableException(
                                            "simulated lost DDL acknowledgement", "08006");
                                }
                                return result;
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
