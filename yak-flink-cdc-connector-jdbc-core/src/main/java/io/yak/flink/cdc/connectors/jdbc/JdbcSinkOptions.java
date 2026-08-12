package io.yak.flink.cdc.connectors.jdbc;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;

public final class JdbcSinkOptions {

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

    private JdbcSinkOptions() {}
}
