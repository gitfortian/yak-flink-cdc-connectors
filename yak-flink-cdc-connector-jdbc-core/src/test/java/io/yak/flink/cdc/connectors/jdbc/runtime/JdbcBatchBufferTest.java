package io.yak.flink.cdc.connectors.jdbc.runtime;

import org.apache.flink.cdc.common.event.TableId;
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
    void sameSqlOnDifferentTablesRemainsSeparateForStatementScoping() {
        JdbcBatchBuffer buffer = new JdbcBatchBuffer();
        TableId first = TableId.tableId("app", "first_table");
        TableId second = TableId.tableId("app", "second_table");

        buffer.add(first, "UPSERT", Collections.singletonList(1));
        buffer.add(second, "UPSERT", Collections.singletonList(2));

        assertThat(buffer.getSegments()).hasSize(2);
        assertThat(buffer.getSegments().get(0).getTableId()).isEqualTo(first);
        assertThat(buffer.getSegments().get(1).getTableId()).isEqualTo(second);
    }

    @Test
    void estimatesRetainedPayloadAndDetectsByteBoundaryBeforeAdd() {
        JdbcBatchBuffer buffer = new JdbcBatchBuffer();
        buffer.add("UPSERT", Arrays.asList(1, "small"));

        long current = buffer.estimatedBytes();
        assertThat(current).isPositive();

        String largeValue = String.join("", Collections.nCopies(1024, "x"));
        assertThat(buffer.wouldExceed(current + 1024L, "UPSERT", Arrays.asList(2, largeValue)))
                .isTrue();
        assertThat(buffer.wouldExceed(current + 16_384L, "UPSERT", Arrays.asList(2, largeValue)))
                .isFalse();
    }

    @Test
    void countsBinaryPayloadAgainstMemoryBoundary() {
        JdbcBatchBuffer buffer = new JdbcBatchBuffer();
        byte[] payload = new byte[4096];

        assertThat(buffer.wouldExceed(2048L, "INSERT", Collections.singletonList(payload)))
                .isTrue();
        buffer.add("INSERT", Collections.singletonList(payload));
        assertThat(buffer.estimatedBytes()).isGreaterThan(4096L);
    }

    @Test
    void clearReleasesBufferedRecordsAndByteAccounting() {
        JdbcBatchBuffer buffer = new JdbcBatchBuffer();
        buffer.add("UPSERT", Arrays.asList(1, "value"));

        buffer.clear();

        assertThat(buffer).matches(JdbcBatchBuffer::isEmpty);
        assertThat(buffer.getSegments()).isEmpty();
        assertThat(buffer.estimatedBytes()).isZero();
    }
}
