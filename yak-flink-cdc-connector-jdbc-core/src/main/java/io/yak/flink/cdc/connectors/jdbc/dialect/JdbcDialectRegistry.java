package io.yak.flink.cdc.connectors.jdbc.dialect;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

public final class JdbcDialectRegistry {

    private JdbcDialectRegistry() {}

    public static JdbcDialect discover(
            String requestedDialect, String jdbcUrl, ClassLoader classLoader) {
        List<JdbcDialectFactory> factories = new ArrayList<>();
        ServiceLoader.load(JdbcDialectFactory.class, classLoader).forEach(factories::add);

        String requested =
                requestedDialect == null ? "auto" : requestedDialect.trim().toLowerCase(Locale.ROOT);

        if (!"auto".equals(requested)) {
            return factories.stream()
                    .filter(factory -> factory.identifier().equalsIgnoreCase(requested))
                    .findFirst()
                    .map(JdbcDialectFactory::create)
                    .orElseThrow(
                            () ->
                                    new IllegalArgumentException(
                                            "Unknown JDBC dialect '"
                                                    + requestedDialect
                                                    + "'. Available dialects: "
                                                    + available(factories)));
        }

        return factories.stream()
                .filter(factory -> factory.acceptsUrl(jdbcUrl))
                .findFirst()
                .map(JdbcDialectFactory::create)
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "Unable to infer JDBC dialect from URL '"
                                                + jdbcUrl
                                                + "'. Set sink option 'dialect'. Available dialects: "
                                                + available(factories)));
    }

    /**
     * Resolves a dialect inside the runtime classloader boundary.
     *
     * <p>Flink serializes sink/coordinator objects between the client, JobMaster and TaskManagers.
     * Concrete dialect instances must not cross that boundary because the connector can be loaded
     * by different user-code classloaders. Loading the SPI with the same classloader that defined
     * {@link JdbcDialect} guarantees that factories and the interface use one type identity.
     */
    public static JdbcDialect discoverRuntime(String requestedDialect, String jdbcUrl) {
        return discover(requestedDialect, jdbcUrl, JdbcDialect.class.getClassLoader());
    }

    private static String available(List<JdbcDialectFactory> factories) {
        return factories.stream()
                .map(JdbcDialectFactory::identifier)
                .sorted()
                .collect(Collectors.joining(", "));
    }
}
