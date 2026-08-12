package io.yak.flink.cdc.connectors.jdbc.dialect;

public final class JdbcColumnMetadata {
    private final int jdbcType;
    private final String typeName;
    private final int columnSize;
    private final int decimalDigits;
    private final boolean nullable;

    public JdbcColumnMetadata(
            int jdbcType, String typeName, int columnSize, int decimalDigits, boolean nullable) {
        this.jdbcType = jdbcType;
        this.typeName = typeName == null ? "" : typeName;
        this.columnSize = columnSize;
        this.decimalDigits = decimalDigits;
        this.nullable = nullable;
    }

    public int getJdbcType() {
        return jdbcType;
    }

    public String getTypeName() {
        return typeName;
    }

    public int getColumnSize() {
        return columnSize;
    }

    public int getDecimalDigits() {
        return decimalDigits;
    }

    public boolean isNullable() {
        return nullable;
    }

    @Override
    public String toString() {
        return "JdbcColumnMetadata{"
                + "jdbcType="
                + jdbcType
                + ", typeName='"
                + typeName
                + '\''
                + ", columnSize="
                + columnSize
                + ", decimalDigits="
                + decimalDigits
                + ", nullable="
                + nullable
                + '}';
    }
}
