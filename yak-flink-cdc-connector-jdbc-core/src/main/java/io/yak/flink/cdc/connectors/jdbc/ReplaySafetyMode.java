package io.yak.flink.cdc.connectors.jdbc;

import java.util.Locale;

/** Defines which at-least-once DML shapes the JDBC sink is allowed to execute. */
public enum ReplaySafetyMode {
    /** Require primary-key based idempotency for every DML event. */
    STRICT("strict"),

    /**
     * Explicitly allow INSERT-only tables without a primary key.
     *
     * <p>Such rows may duplicate when a committed batch is replayed after an ambiguous commit or
     * task recovery. UPDATE, REPLACE and DELETE without a primary key remain unsupported.
     */
    ALLOW_APPEND_ONLY("allow-append-only");

    private final String optionValue;

    ReplaySafetyMode(String optionValue) {
        this.optionValue = optionValue;
    }

    public String optionValue() {
        return optionValue;
    }

    public static ReplaySafetyMode fromOption(String value) {
        String normalized = value == null ? STRICT.optionValue : value.trim().toLowerCase(Locale.ROOT);
        for (ReplaySafetyMode mode : values()) {
            if (mode.optionValue.equals(normalized)) {
                return mode;
            }
        }
        throw new IllegalArgumentException(
                "Unsupported replay-safety '"
                        + value
                        + "'. Expected one of: strict, allow-append-only");
    }
}
