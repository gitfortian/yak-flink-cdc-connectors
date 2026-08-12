package io.yak.flink.cdc.connectors.jdbc.state;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.util.LinkedHashMap;
import java.util.Map;

/** Versioned serializer for the JDBC writer schema cache stored in Flink checkpoints/savepoints. */
public final class YakJdbcWriterStateSerializer
        implements SimpleVersionedSerializer<YakJdbcWriterState> {

    public static final YakJdbcWriterStateSerializer INSTANCE = new YakJdbcWriterStateSerializer();

    private static final int VERSION = 1;

    private YakJdbcWriterStateSerializer() {}

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(YakJdbcWriterState state) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            // Persist the schema graph only. The outer state class is reconstructed explicitly so
            // its implementation can evolve independently from the checkpoint payload envelope.
            output.writeObject(new LinkedHashMap<>(state.getSchemas()));
        }
        return bytes.toByteArray();
    }

    @Override
    public YakJdbcWriterState deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException(
                    "Unsupported Yak JDBC writer state version "
                            + version
                            + "; supported version is "
                            + VERSION);
        }

        try (ObjectInputStream input =
                new ContextClassLoaderObjectInputStream(new ByteArrayInputStream(serialized))) {
            Object value = input.readObject();
            if (!(value instanceof Map)) {
                throw new IOException(
                        "Corrupt Yak JDBC writer state: expected schema map but found "
                                + value.getClass().getName());
            }

            Map<TableId, Schema> schemas = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                if (!(entry.getKey() instanceof TableId) || !(entry.getValue() instanceof Schema)) {
                    throw new IOException(
                            "Corrupt Yak JDBC writer state: schema map contains unexpected types");
                }
                schemas.put((TableId) entry.getKey(), (Schema) entry.getValue());
            }
            return new YakJdbcWriterState(schemas);
        } catch (ClassNotFoundException e) {
            throw new IOException("Unable to load classes while restoring Yak JDBC writer state", e);
        }
    }

    /** Uses the task context classloader first so connector/Flink CDC classes restore safely. */
    private static final class ContextClassLoaderObjectInputStream extends ObjectInputStream {
        private ContextClassLoaderObjectInputStream(ByteArrayInputStream input) throws IOException {
            super(input);
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass descriptor)
                throws IOException, ClassNotFoundException {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            if (contextClassLoader != null) {
                try {
                    return Class.forName(descriptor.getName(), false, contextClassLoader);
                } catch (ClassNotFoundException ignored) {
                    // Fall back to ObjectInputStream's default resolution below.
                }
            }
            return super.resolveClass(descriptor);
        }
    }
}
