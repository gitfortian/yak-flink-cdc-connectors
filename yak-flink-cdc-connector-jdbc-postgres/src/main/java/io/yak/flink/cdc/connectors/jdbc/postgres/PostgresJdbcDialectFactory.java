package io.yak.flink.cdc.connectors.jdbc.postgres;

import io.yak.flink.cdc.connectors.jdbc.dialect.JdbcDialect;
import io.yak.flink.cdc.connectors.jdbc.dialect.JdbcDialectFactory;

import java.util.Locale;

public final class PostgresJdbcDialectFactory implements JdbcDialectFactory {

    @Override
    public String identifier() {
        return "postgres";
    }

    @Override
    public boolean acceptsUrl(String jdbcUrl) {
        if (jdbcUrl == null) {
            return false;
        }
        String normalized = jdbcUrl.toLowerCase(Locale.ROOT);
        return normalized.startsWith("jdbc:postgresql:")
                || normalized.startsWith("jdbc:postgres:");
    }

    @Override
    public JdbcDialect create() {
        return new PostgresJdbcDialect();
    }
}
