package com.devpulse.extractor.orchestrator;

/**
 * Holds the result of extracting a single entity type for a repository (or globally).
 * Tracks record counts, errors, and duration for summary reporting.
 */
public record ExtractionResult(
        String entityType,
        String repoFullName,
        int recordsExtracted,
        int recordsLoaded,
        boolean success,
        String errorMessage,
        long durationMs
) {

    public static ExtractionResult success(String entityType, String repoFullName,
                                           int extracted, int loaded, long durationMs) {
        return new ExtractionResult(entityType, repoFullName, extracted, loaded, true, null, durationMs);
    }

    public static ExtractionResult failure(String entityType, String repoFullName,
                                           String error, long durationMs) {
        return new ExtractionResult(entityType, repoFullName, 0, 0, false, error, durationMs);
    }
}
