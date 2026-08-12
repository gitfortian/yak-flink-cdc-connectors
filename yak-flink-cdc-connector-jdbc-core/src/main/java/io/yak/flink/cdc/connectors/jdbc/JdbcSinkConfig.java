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
    private final int batchSize;
    private final long flushIntervalMillis;

    public JdbcSinkConfig(
            String url,
            String driver,
            String username,
            String password,
            String dialect,
            int maxRetries) {
        this(
                url,
                driver,
                username,
                password,
                dialect,
                maxRetries,
                JdbcSinkOptions.DEFAULT_BATCH_SIZE,
                JdbcSinkOptions.DEFAULT_FLUSH_INTERVAL_MILLIS);
    }

    public JdbcSinkConfig(
            String url,
            String driver,
            String username,
            String password,
            String dialect,
            int maxRetries,
            int batchSize,
            long flushIntervalMillis) {
        this.url = Objects.requireNonNull(url, "url");
        this.driver = Objects.requireNonNull(driver, "driver");
        this.username = username == null ? "" : username;
        this.password = password == null ? "" : password;
        this.dialect = dialect == null ? "auto" : dialect;
        if (maxRetries < 0) {
            throw new IllegalArgumentException("max-retries must be >= 0");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batch-size must be > 0");
        }
        if (flushIntervalMillis < 0) {
            throw new IllegalArgumentException("flush-interval-ms must be >= 0");
        }
        this.maxRetries = maxRetries;
        this.batchSize = batchSize;
        this.flushIntervalMillis = flushIntervalMillis;
    }

    public static JdbcSinkConfig from(Configuration configuration) {
        return new JdbcSinkConfig(
                configuration.get(JdbcSinkOptions.URL),
                configuration.get(JdbcSinkOptions.DRIVER),
                configuration.get(JdbcSinkOptions.USERNAME),
                configuration.get(JdbcSinkOptions.PASSWORD),
                configuration.get(JdbcSinkOptions.DIALECT),
                configuration.get(JdbcSinkOptions.MAX_RETRIES),
                configuration.get(JdbcSinkOptions.BATCH_SIZE),
                configuration.get(JdbcSinkOptions.FLUSH_INTERVAL_MILLIS));
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

    public int getBatchSize() {
        return batchSize;
    }

    public long getFlushIntervalMillis() {
        return flushIntervalMillis;
    }
}
