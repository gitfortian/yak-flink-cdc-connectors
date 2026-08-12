package io.yak.flink.cdc.connectors.jdbc.mysql;

import io.yak.flink.cdc.connectors.jdbc.dialect.JdbcDialect;
import io.yak.flink.cdc.connectors.jdbc.dialect.JdbcDialectFactory;

import java.util.Locale;

public final class MySqlJdbcDialectFactory implements JdbcDialectFactory {

    @Override
    public String identifier() {
        return "mysql";
    }

    @Override
    public boolean acceptsUrl(String jdbcUrl) {
        return jdbcUrl != null
                && jdbcUrl.toLowerCase(Locale.ROOT).startsWith("jdbc:mysql:");
    }

    @Override
    public JdbcDialect create() {
        return new MySqlJdbcDialect();
    }
}
