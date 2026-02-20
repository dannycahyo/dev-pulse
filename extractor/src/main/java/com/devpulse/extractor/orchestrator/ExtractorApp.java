package com.devpulse.extractor.orchestrator;

import com.devpulse.extractor.client.GitHubApiClient;
import com.devpulse.extractor.config.AppConfig;
import com.devpulse.extractor.loader.BigQueryLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for the DevPulse GitHub data extractor.
 * Parses CLI arguments, initializes components, runs the extraction pipeline,
 * and exits with appropriate status codes.
 *
 * <p>Usage:
 * <pre>
 *   java -jar extractor.jar --incremental   # default: incremental extraction
 *   java -jar extractor.jar --full          # full extraction (ignore timestamps)
 * </pre>
 */
public class ExtractorApp {

    private static final Logger logger = LoggerFactory.getLogger(ExtractorApp.class);

    public static void main(String[] args) {
        boolean fullMode = parseFullMode(args);
        logger.info("Starting DevPulse Extractor (mode: {})", fullMode ? "FULL" : "INCREMENTAL");

        try {
            AppConfig config = new AppConfig();
            GitHubApiClient client = new GitHubApiClient(
                    config.getGithubToken(), config.getGithubUsername());
            BigQueryLoader loader = new BigQueryLoader(config.getGcpProjectId());

            ExtractionOrchestrator orchestrator = new ExtractionOrchestrator(client, loader);
            ExtractionSummary summary = orchestrator.run(fullMode);

            printSummary(summary);

            if (summary.hasFailures()) {
                logger.warn("Extraction completed with failures");
                System.exit(1);
            }

            logger.info("DevPulse Extractor finished successfully.");
            System.exit(0);

        } catch (Exception e) {
            logger.error("Fatal error during extraction", e);
            System.exit(1);
        }
    }

    static boolean parseFullMode(String[] args) {
        for (String arg : args) {
            if ("--full".equals(arg)) {
                return true;
            }
        }
        return false;
    }

    private static void printSummary(ExtractionSummary summary) {
        System.out.println();
        System.out.println("=== DevPulse Extraction Summary ===");
        System.out.println("Duration: " + summary.totalDurationMs() + "ms");
        System.out.println("Results:  " + summary.successCount() + " successful, "
                + summary.failureCount() + " failed");

        System.out.println();
        System.out.println("Entity breakdown:");
        printEntityLine(summary, "repositories");
        printEntityLine(summary, "commits");
        printEntityLine(summary, "pull_requests");
        printEntityLine(summary, "reviews");
        printEntityLine(summary, "languages");

        if (summary.hasFailures()) {
            System.out.println();
            System.out.println("Failures:");
            summary.results().stream()
                    .filter(r -> !r.success())
                    .forEach(r -> System.out.println("  - " + r.entityType()
                            + " [" + r.repoFullName() + "]: " + r.errorMessage()));
        }
        System.out.println();
    }

    private static void printEntityLine(ExtractionSummary summary, String entityType) {
        int extracted = summary.totalExtractedForEntity(entityType);
        int loaded = summary.totalLoadedForEntity(entityType);
        long failures = summary.results().stream()
                .filter(r -> r.entityType().equals(entityType) && !r.success())
                .count();
        System.out.printf("  %-16s extracted=%-6d loaded=%-6d failures=%d%n",
                entityType, extracted, loaded, failures);
    }
}
