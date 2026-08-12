package io.yak.flink.cdc.connectors.jdbc.sink;

import io.yak.flink.cdc.connectors.jdbc.JdbcSinkConfig;
import io.yak.flink.cdc.connectors.jdbc.state.YakJdbcWriterState;
import io.yak.flink.cdc.connectors.jdbc.state.YakJdbcWriterStateSerializer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public final class YakJdbcSink
        implements Sink<Event>, SupportsWriterState<Event, YakJdbcWriterState> {
    private static final long serialVersionUID = 1L;

    private final JdbcSinkConfig config;

    public YakJdbcSink(JdbcSinkConfig config) {
        this.config = config;
    }

    @SuppressWarnings("deprecation")
    @Override
    public SinkWriter<Event> createWriter(Sink.InitContext context) throws IOException {
        return new YakJdbcWriter(
                config, Collections.emptyMap(), context.getProcessingTimeService());
    }

    @Override
    public StatefulSinkWriter<Event, YakJdbcWriterState> createWriter(WriterInitContext context)
            throws IOException {
        return new YakJdbcWriter(
                config, Collections.emptyMap(), context.getProcessingTimeService());
    }

    @Override
    public StatefulSinkWriter<Event, YakJdbcWriterState> restoreWriter(
            WriterInitContext context, Collection<YakJdbcWriterState> recoveredState)
            throws IOException {
        return new YakJdbcWriter(
                config,
                YakJdbcWriterState.merge(recoveredState),
                context.getProcessingTimeService());
    }

    @Override
    public SimpleVersionedSerializer<YakJdbcWriterState> getWriterStateSerializer() {
        return YakJdbcWriterStateSerializer.INSTANCE;
    }
}
