package io.yak.flink.cdc.connectors.jdbc.state;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class YakJdbcWriterStateTest {

    private static final TableId TABLE = TableId.tableId("app_db", "customers");

    @Test
    void serializerRoundTripPreservesEvolvedSchemaCache() throws Exception {
        Schema schema = evolvedSchema();
        YakJdbcWriterState state = new YakJdbcWriterState(singleSchema(TABLE, schema));

        YakJdbcWriterStateSerializer serializer = YakJdbcWriterStateSerializer.INSTANCE;
        byte[] serialized = serializer.serialize(state);
        YakJdbcWriterState restored = serializer.deserialize(serializer.getVersion(), serialized);

        assertThat(restored.getSchemas()).containsOnlyKeys(TABLE);
        assertThat(restored.getSchemas().get(TABLE)).isEqualTo(schema);
        assertThat(restored.getSchemas().get(TABLE).getColumnNames())
                .containsExactly("id", "name", "status");
        assertThat(restored.getSchemas().get(TABLE).primaryKeys()).containsExactly("id");
    }

    @Test
    void mergeDeduplicatesIdenticalSchemaCopiesFromRescaledWriters() throws Exception {
        Schema schema = evolvedSchema();
        YakJdbcWriterState first = new YakJdbcWriterState(singleSchema(TABLE, schema));
        YakJdbcWriterState second = new YakJdbcWriterState(singleSchema(TABLE, schema));

        Map<TableId, Schema> merged = YakJdbcWriterState.merge(Arrays.asList(first, second));

        assertThat(merged).containsOnlyKeys(TABLE);
        assertThat(merged.get(TABLE)).isEqualTo(schema);
    }

    @Test
    void mergeRejectsConflictingRecoveredSchemas() {
        YakJdbcWriterState oldState =
                new YakJdbcWriterState(singleSchema(TABLE, baseSchema()));
        YakJdbcWriterState evolvedState =
                new YakJdbcWriterState(singleSchema(TABLE, evolvedSchema()));

        assertThatThrownBy(() -> YakJdbcWriterState.merge(Arrays.asList(oldState, evolvedState)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Conflicting recovered schema state")
                .hasMessageContaining(TABLE.identifier());
    }

    @Test
    void serializerRejectsUnknownStateVersion() throws Exception {
        YakJdbcWriterStateSerializer serializer = YakJdbcWriterStateSerializer.INSTANCE;
        byte[] serialized =
                serializer.serialize(
                        new YakJdbcWriterState(singleSchema(TABLE, evolvedSchema())));

        assertThatThrownBy(() -> serializer.deserialize(999, serialized))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported Yak JDBC writer state version");
    }

    @Test
    void emptyRecoveryProducesEmptySchemaCache() throws Exception {
        assertThat(YakJdbcWriterState.merge(Collections.emptyList())).isEmpty();
    }

    private static Map<TableId, Schema> singleSchema(TableId tableId, Schema schema) {
        Map<TableId, Schema> schemas = new LinkedHashMap<>();
        schemas.put(tableId, schema);
        return schemas;
    }

    private static Schema baseSchema() {
        return Schema.newBuilder()
                .physicalColumn("id", DataTypes.INT().notNull())
                .physicalColumn("name", DataTypes.VARCHAR(64).notNull())
                .primaryKey("id")
                .build();
    }

    private static Schema evolvedSchema() {
        return Schema.newBuilder()
                .physicalColumn("id", DataTypes.INT().notNull())
                .physicalColumn("name", DataTypes.VARCHAR(64).notNull())
                .physicalColumn("status", DataTypes.VARCHAR(16))
                .primaryKey("id")
                .build();
    }
}
