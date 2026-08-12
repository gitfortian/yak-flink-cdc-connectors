package io.yak.flink.cdc.connectors.jdbc;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;

public final class JdbcSinkOptions {

    public static final int DEFAULT_BATCH_SIZE = 1000;
    public static final long DEFAULT_FLUSH_INTERVAL_MILLIS = 2000L;
    public static final long DEFAULT_MAX_BATCH_BYTES = 16L * 1024L * 1024L;

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url").stringType().noDefaultValue();

    public static final ConfigOption<String> DRIVER =
            ConfigOptions.key("driver").stringType().noDefaultValue();

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username").stringType().defaultValue("");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password").stringType().defaultValue("");

    public static final ConfigOption<String> DIALECT =
            ConfigOptions.key("dialect").stringType().defaultValue("auto");

    public static final ConfigOption<Integer> MAX_RETRIES =
            ConfigOptions.key("max-retries").intType().defaultValue(3);

    public static final ConfigOption<Integer> BATCH_SIZE =
            ConfigOptions.key("batch-size").intType().defaultValue(DEFAULT_BATCH_SIZE);

    public static final ConfigOption<Long> FLUSH_INTERVAL_MILLIS =
            ConfigOptions.key("flush-interval-ms")
                    .longType()
                    .defaultValue(DEFAULT_FLUSH_INTERVAL_MILLIS);

    public static final ConfigOption<Long> MAX_BATCH_BYTES =
            ConfigOptions.key("max-batch-bytes")
                    .longType()
                    .defaultValue(DEFAULT_MAX_BATCH_BYTES);

    private JdbcSinkOptions() {}
}
