package com.devpulse.extractor.orchestrator;

import java.util.List;

/**
 * Aggregated summary of an extraction run. Provides convenience methods
 * for querying results by entity type and computing overall success/failure counts.
 */
public record ExtractionSummary(
        List<ExtractionResult> results,
        long totalDurationMs
) {

    public int successCount() {
        return (int) results.stream().filter(ExtractionResult::success).count();
    }

    public int failureCount() {
        return (int) results.stream().filter(r -> !r.success()).count();
    }

    public boolean hasFailures() {
        return results.stream().anyMatch(r -> !r.success());
    }

    public int totalExtractedForEntity(String entityType) {
        return results.stream()
                .filter(r -> r.entityType().equals(entityType) && r.success())
                .mapToInt(ExtractionResult::recordsExtracted)
                .sum();
    }

    public int totalLoadedForEntity(String entityType) {
        return results.stream()
                .filter(r -> r.entityType().equals(entityType) && r.success())
                .mapToInt(ExtractionResult::recordsLoaded)
                .sum();
    }
}
