package io.yak.flink.cdc.connectors.jdbc.sink;

import io.yak.flink.cdc.connectors.jdbc.JdbcSinkConfig;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.cdc.common.event.Event;

import java.io.IOException;

public final class YakJdbcSink implements Sink<Event> {
    private static final long serialVersionUID = 1L;

    private final JdbcSinkConfig config;

    public YakJdbcSink(JdbcSinkConfig config) {
        this.config = config;
    }

    @SuppressWarnings("deprecation")
    @Override
    public SinkWriter<Event> createWriter(Sink.InitContext context) throws IOException {
        return new YakJdbcWriter(config);
    }

    @Override
    public SinkWriter<Event> createWriter(WriterInitContext context) throws IOException {
        return new YakJdbcWriter(config);
    }
}
