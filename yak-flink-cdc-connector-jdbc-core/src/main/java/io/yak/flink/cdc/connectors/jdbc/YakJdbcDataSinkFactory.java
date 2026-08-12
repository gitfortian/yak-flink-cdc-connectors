package io.yak.flink.cdc.connectors.jdbc;

import io.yak.flink.cdc.connectors.jdbc.dialect.JdbcDialect;
import io.yak.flink.cdc.connectors.jdbc.dialect.JdbcDialectRegistry;
import io.yak.flink.cdc.connectors.jdbc.sink.YakJdbcDataSink;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;

import java.util.LinkedHashSet;
import java.util.Set;

public final class YakJdbcDataSinkFactory implements DataSinkFactory {

    @Override
    public DataSink createDataSink(Context context) {
        FactoryHelper.createFactoryHelper(this, context).validate();

        JdbcSinkConfig config = JdbcSinkConfig.from(context.getFactoryConfiguration());
        JdbcDialect dialect =
                JdbcDialectRegistry.discover(
                        config.getDialect(), config.getUrl(), context.getClassLoader());

        return new YakJdbcDataSink(config, dialect);
    }

    @Override
    public String identifier() {
        return "yak-jdbc";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new LinkedHashSet<>();
        options.add(JdbcSinkOptions.URL);
        options.add(JdbcSinkOptions.DRIVER);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new LinkedHashSet<>();
        options.add(JdbcSinkOptions.USERNAME);
        options.add(JdbcSinkOptions.PASSWORD);
        options.add(JdbcSinkOptions.DIALECT);
        options.add(JdbcSinkOptions.MAX_RETRIES);
        return options;
    }
}
