package io.yak.flink.cdc.connectors.e2e;

import io.yak.flink.cdc.connectors.jdbc.JdbcSinkConfig;
import io.yak.flink.cdc.connectors.jdbc.sink.YakJdbcMetadataApplier;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

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
        applier =
                new YakJdbcMetadataApplier(
                        new JdbcSinkConfig(
                                jdbcUrl(),
                                "org.postgresql.Driver",
                                USER,
                                PASSWORD,
                                "postgres",
                                3));
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

    private static Schema initialSchema() {
        return Schema.newBuilder()
                .physicalColumn("id", DataTypes.INT().notNull())
                .physicalColumn("name", DataTypes.VARCHAR(64).notNull())
                .primaryKey("id")
                .build();
    }

    private static void applyTwice(org.apache.flink.cdc.common.event.SchemaChangeEvent event) {
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
}
