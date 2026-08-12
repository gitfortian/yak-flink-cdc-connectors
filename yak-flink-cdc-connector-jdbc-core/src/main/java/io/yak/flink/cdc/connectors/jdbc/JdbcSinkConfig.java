package io.yak.flink.cdc.connectors.jdbc;

import org.apache.flink.cdc.common.configuration.Configuration;

import java.io.Serializable;
import java.util.Objects;

public final class JdbcSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String url;
    private final String driver;
    private final String username;
    private final String password;
    private final String dialect;
    private final int maxRetries;

    public JdbcSinkConfig(
            String url,
            String driver,
            String username,
            String password,
            String dialect,
            int maxRetries) {
        this.url = Objects.requireNonNull(url, "url");
        this.driver = Objects.requireNonNull(driver, "driver");
        this.username = username == null ? "" : username;
        this.password = password == null ? "" : password;
        this.dialect = dialect == null ? "auto" : dialect;
        if (maxRetries < 0) {
            throw new IllegalArgumentException("max-retries must be >= 0");
        }
        this.maxRetries = maxRetries;
    }

    public static JdbcSinkConfig from(Configuration configuration) {
        return new JdbcSinkConfig(
                configuration.get(JdbcSinkOptions.URL),
                configuration.get(JdbcSinkOptions.DRIVER),
                configuration.get(JdbcSinkOptions.USERNAME),
                configuration.get(JdbcSinkOptions.PASSWORD),
                configuration.get(JdbcSinkOptions.DIALECT),
                configuration.get(JdbcSinkOptions.MAX_RETRIES));
    }

    public String getUrl() {
        return url;
    }

    public String getDriver() {
        return driver;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDialect() {
        return dialect;
    }

    public int getMaxRetries() {
        return maxRetries;
    }
}
