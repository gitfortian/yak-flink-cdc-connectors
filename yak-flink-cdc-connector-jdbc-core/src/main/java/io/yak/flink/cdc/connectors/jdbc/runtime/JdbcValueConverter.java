package io.yak.flink.cdc.connectors.jdbc.runtime;

import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.StringData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public final class JdbcValueConverter {

    private JdbcValueConverter() {}

    public static List<Object> toJdbcValues(RecordData recordData, Schema schema) {
        if (recordData == null) {
            throw new IllegalArgumentException("recordData must not be null");
        }
        if (recordData.getArity() != schema.getColumnCount()) {
            throw new IllegalArgumentException(
                    "Record arity "
                            + recordData.getArity()
                            + " does not match schema column count "
                            + schema.getColumnCount());
        }

        List<RecordData.FieldGetter> getters = SchemaUtils.createFieldGetters(schema);
        List<Object> result = new ArrayList<>(getters.size());
        for (RecordData.FieldGetter getter : getters) {
            result.add(normalize(getter.getFieldOrNull(recordData)));
        }
        return result;
    }

    private static Object normalize(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof StringData) {
            return value.toString();
        }
        if (value instanceof DecimalData) {
            return ((DecimalData) value).toBigDecimal();
        }
        if (value instanceof DateData) {
            return Date.valueOf(((DateData) value).toLocalDate());
        }
        if (value instanceof TimeData) {
            return Time.valueOf(((TimeData) value).toLocalTime());
        }
        if (value instanceof TimestampData) {
            return ((TimestampData) value).toTimestamp();
        }
        if (value instanceof LocalZonedTimestampData) {
            return Timestamp.from(((LocalZonedTimestampData) value).toInstant());
        }
        if (value instanceof ZonedTimestampData) {
            return ((ZonedTimestampData) value).getZonedDateTime().toOffsetDateTime();
        }
        if (value instanceof byte[]
                || value instanceof Boolean
                || value instanceof Byte
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long
                || value instanceof Float
                || value instanceof Double) {
            return value;
        }
        throw new UnsupportedOperationException(
                "Unsupported JDBC value type in MVP: " + value.getClass().getName());
    }
}
