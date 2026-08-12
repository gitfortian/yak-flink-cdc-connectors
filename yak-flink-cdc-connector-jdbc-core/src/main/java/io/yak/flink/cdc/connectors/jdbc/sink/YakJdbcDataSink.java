package io.yak.flink.cdc.connectors.jdbc.sink;

import io.yak.flink.cdc.connectors.jdbc.JdbcSinkConfig;

import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;

import java.io.Serializable;

public final class YakJdbcDataSink implements DataSink, Serializable {
    private static final long serialVersionUID = 1L;

    private final JdbcSinkConfig config;

    public YakJdbcDataSink(JdbcSinkConfig config) {
        this.config = config;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        return FlinkSinkProvider.of(new YakJdbcSink(config));
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return new YakJdbcMetadataApplier(config);
    }
}
