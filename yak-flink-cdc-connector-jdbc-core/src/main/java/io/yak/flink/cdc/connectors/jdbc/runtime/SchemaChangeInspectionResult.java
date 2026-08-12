package io.yak.flink.cdc.connectors.jdbc.runtime;

public final class SchemaChangeInspectionResult {

    public enum Status {
        APPLIED,
        NOT_APPLIED,
        CONFLICT
    }

    private final Status status;
    private final String detail;

    private SchemaChangeInspectionResult(Status status, String detail) {
        this.status = status;
        this.detail = detail == null ? "" : detail;
    }

    public static SchemaChangeInspectionResult applied(String detail) {
        return new SchemaChangeInspectionResult(Status.APPLIED, detail);
    }

    public static SchemaChangeInspectionResult notApplied(String detail) {
        return new SchemaChangeInspectionResult(Status.NOT_APPLIED, detail);
    }

    public static SchemaChangeInspectionResult conflict(String detail) {
        return new SchemaChangeInspectionResult(Status.CONFLICT, detail);
    }

    public Status getStatus() {
        return status;
    }

    public String getDetail() {
        return detail;
    }

    public boolean isApplied() {
        return status == Status.APPLIED;
    }

    public boolean isConflict() {
        return status == Status.CONFLICT;
    }

    @Override
    public String toString() {
        return status + (detail.isEmpty() ? "" : ": " + detail);
    }
}
