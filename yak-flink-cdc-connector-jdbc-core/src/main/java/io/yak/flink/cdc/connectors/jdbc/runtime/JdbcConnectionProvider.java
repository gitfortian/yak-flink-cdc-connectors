package io.yak.flink.cdc.connectors.jdbc.runtime;

import io.yak.flink.cdc.connectors.jdbc.JdbcSinkConfig;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public final class JdbcConnectionProvider implements Serializable {
    private static final long serialVersionUID = 1L;

    private final JdbcSinkConfig config;

    public JdbcConnectionProvider(JdbcSinkConfig config) {
        this.config = config;
    }

    public Connection open() throws SQLException {
        try {
            Class.forName(
                    config.getDriver(),
                    true,
                    Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                    "JDBC driver class '"
                            + config.getDriver()
                            + "' was not found. Put the database driver JAR in the Flink CDC lib directory.",
                    e);
        }

        Properties properties = new Properties();
        if (!config.getUsername().isEmpty()) {
            properties.setProperty("user", config.getUsername());
        }
        if (!config.getPassword().isEmpty()) {
            properties.setProperty("password", config.getPassword());
        }
        return DriverManager.getConnection(config.getUrl(), properties);
    }
}
