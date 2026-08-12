package io.yak.flink.cdc.connectors.jdbc;

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

        // Validate eagerly while the Factory context classloader is available, but deliberately do
        // not retain the concrete dialect instance. Flink serializes sink objects across runtime
        // classloader boundaries; the dialect is resolved again lazily on the runtime side.
        JdbcDialectRegistry.discover(config.getDialect(), config.getUrl(), context.getClassLoader());

        return new YakJdbcDataSink(config);
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
        options.add(JdbcSinkOptions.BATCH_SIZE);
        options.add(JdbcSinkOptions.FLUSH_INTERVAL_MILLIS);
        options.add(JdbcSinkOptions.MAX_BATCH_BYTES);
        options.add(JdbcSinkOptions.STATEMENT_CACHE_SIZE);
        options.add(JdbcSinkOptions.REPLAY_SAFETY);
        return options;
    }
}
