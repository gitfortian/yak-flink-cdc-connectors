package io.yak.flink.cdc.connectors.e2e;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.flink.FlinkPipelineComposer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Real MySQL source -> Flink CDC Pipeline -> Yak JDBC MySQL sink gate. */
class MySqlToMySqlPipelineITCase {
    @Test
    void snapshotIncrementalDmlAndDdlReachMySqlSink() throws Exception {
        try (GenericContainer<?> source = mysql("source_db", "--server-id=4101", "--log-bin=mysql-bin", "--binlog-format=ROW", "--binlog-row-image=FULL");
                GenericContainer<?> target = mysql("source_db")) {
            source.start(); target.start();
            try (Connection c = connect(source, "source_db"); Statement s = c.createStatement()) {
                s.execute("CREATE TABLE customers(id INT PRIMARY KEY, name VARCHAR(64) NOT NULL)");
                s.execute("INSERT INTO customers VALUES (1,'Alice'),(2,'Bob'),(3,'Carol')");
            }

            Map<String,String> sourceOptions = new LinkedHashMap<>();
            sourceOptions.put("hostname", source.getHost());
            sourceOptions.put("port", source.getMappedPort(3306).toString());
            sourceOptions.put("username", "root"); sourceOptions.put("password", "root");
            sourceOptions.put("tables", "source_db.customers");
            sourceOptions.put("server-time-zone", "UTC"); sourceOptions.put("server-id", "4200-4204");
            Map<String,String> sinkOptions = new LinkedHashMap<>();
            sinkOptions.put("url", url(target, "source_db"));
            sinkOptions.put("driver", "com.mysql.cj.jdbc.Driver");
            sinkOptions.put("username", "root"); sinkOptions.put("password", "root");
            sinkOptions.put("dialect", "mysql"); sinkOptions.put("batch-size", "2");
            Configuration pipeline = new Configuration();
            pipeline.set(PipelineOptions.PIPELINE_NAME, "mysql-to-mysql-e2e");
            pipeline.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
            pipeline.set(PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
            PipelineDef definition = new PipelineDef(
                    new SourceDef("mysql", "source", Configuration.fromMap(sourceOptions)),
                    new SinkDef("yak-jdbc", "target", Configuration.fromMap(sinkOptions)),
                    Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), pipeline);
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.enableCheckpointing(500);
            FlinkPipelineComposer.ofApplicationCluster(env).compose(definition);
            JobClient job = env.executeAsync("mysql-to-mysql-e2e");
            try {
                awaitRows(target, Arrays.asList("1|Alice", "2|Bob", "3|Carol"), false);
                try (Connection c = connect(source, "source_db"); Statement s = c.createStatement()) {
                    s.execute("INSERT INTO customers VALUES (4,'Dave')");
                    s.execute("UPDATE customers SET name='Bobby' WHERE id=2");
                    s.execute("DELETE FROM customers WHERE id=3");
                    s.execute("ALTER TABLE customers ADD COLUMN status VARCHAR(16)");
                    s.execute("UPDATE customers SET status='active'");
                }
                awaitRows(target, Arrays.asList("1|Alice|active", "2|Bobby|active", "4|Dave|active"), true);
            } finally {
                job.cancel().get(20, TimeUnit.SECONDS);
            }
        }
    }

    private static GenericContainer<?> mysql(String database, String... command) {
        GenericContainer<?> c = new GenericContainer<>(DockerImageName.parse("mysql:8.0.40"))
                .withEnv("MYSQL_ROOT_PASSWORD", "root").withEnv("MYSQL_ROOT_HOST", "%")
                .withEnv("MYSQL_DATABASE", database).withExposedPorts(3306)
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));
        return command.length == 0 ? c : c.withCommand(command);
    }
    private static Connection connect(GenericContainer<?> c, String db) throws SQLException {
        return DriverManager.getConnection(url(c, db), "root", "root");
    }
    private static String url(GenericContainer<?> c, String db) {
        return "jdbc:mysql://" + c.getHost() + ":" + c.getMappedPort(3306) + "/" + db + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    }
    private static void awaitRows(GenericContainer<?> target, List<String> expected, boolean status) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
        Throwable last = null;
        List<String> lastRows = Collections.emptyList();
        while (System.nanoTime() < deadline) {
            try (Connection c = connect(target, "source_db"); Statement s = c.createStatement();
                    ResultSet r = s.executeQuery("SELECT id,name" + (status ? ",status" : "") + " FROM source_db.customers ORDER BY id")) {
                List<String> actual = new ArrayList<>();
                while (r.next()) actual.add(r.getInt(1) + "|" + r.getString(2) + (status ? "|" + r.getString(3) : ""));
                if (actual.equals(expected)) return;
                lastRows = actual;
            } catch (Throwable t) { last = t; }
            Thread.sleep(500);
        }
        if (last != null) throw new AssertionError("target did not converge to " + expected, last);
        assertThat(lastRows).as("target rows at timeout").isEqualTo(expected);
    }
}
