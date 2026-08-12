package io.yak.flink.cdc.connectors.jdbc.runtime;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcBatchBufferTest {

    @Test
    void batchesOnlyAdjacentMatchingSqlAndPreservesCdcOrder() {
        JdbcBatchBuffer buffer = new JdbcBatchBuffer();

        buffer.add("UPSERT", Arrays.asList(1, "first"));
        buffer.add("UPSERT", Arrays.asList(2, "second"));
        buffer.add("DELETE", Collections.singletonList(1));
        buffer.add("UPSERT", Arrays.asList(1, "third"));

        assertThat(buffer.size()).isEqualTo(4);
        assertThat(buffer.getSegments()).hasSize(3);
        assertThat(buffer.getSegments().get(0).getSql()).isEqualTo("UPSERT");
        assertThat(buffer.getSegments().get(0).size()).isEqualTo(2);
        assertThat(buffer.getSegments().get(1).getSql()).isEqualTo("DELETE");
        assertThat(buffer.getSegments().get(1).size()).isEqualTo(1);
        assertThat(buffer.getSegments().get(2).getSql()).isEqualTo("UPSERT");
        assertThat(buffer.getSegments().get(2).size()).isEqualTo(1);
    }

    @Test
    void clearReleasesBufferedRecords() {
        JdbcBatchBuffer buffer = new JdbcBatchBuffer();
        buffer.add("UPSERT", Arrays.asList(1, "value"));

        buffer.clear();

        assertThat(buffer).matches(JdbcBatchBuffer::isEmpty);
        assertThat(buffer.getSegments()).isEmpty();
    }
}
