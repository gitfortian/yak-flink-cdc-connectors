package io.yak.flink.cdc.connectors.jdbc.dialect;

public interface JdbcDialectFactory {

    String identifier();

    boolean acceptsUrl(String jdbcUrl);

    JdbcDialect create();
}
