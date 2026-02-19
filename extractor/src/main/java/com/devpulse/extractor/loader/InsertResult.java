package com.devpulse.extractor.loader;

import java.util.List;

/**
 * Result of a BigQuery streaming insert operation. Reports total rows attempted,
 * successful rows inserted, and any per-row errors for partial failure reporting.
 */
public record InsertResult(
        int totalRows,
        int successfulRows,
        List<RowError> errors
) {

    public record RowError(long rowIndex, String message) {}

    public boolean hasErrors() {
        return errors != null && !errors.isEmpty();
    }
}
