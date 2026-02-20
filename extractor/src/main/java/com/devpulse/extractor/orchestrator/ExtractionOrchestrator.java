package com.devpulse.extractor.orchestrator;

import com.devpulse.extractor.client.GitHubApiClient;
import com.devpulse.extractor.loader.BigQueryLoader;
import com.devpulse.extractor.model.PullRequest;
import com.devpulse.extractor.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Coordinates the full extraction pipeline: repos -> commits -> PRs -> reviews -> languages.
 * Reads last extraction timestamps from _extraction_metadata for incremental mode.
 * Writes extracted data to BigQuery and updates metadata on success.
 */
public class ExtractionOrchestrator {

    private static final Logger logger = LoggerFactory.getLogger(ExtractionOrchestrator.class);

    static final String ENTITY_REPOSITORIES = "repositories";
    static final String ENTITY_COMMITS = "commits";
    static final String ENTITY_PULL_REQUESTS = "pull_requests";
    static final String ENTITY_REVIEWS = "reviews";
    static final String ENTITY_LANGUAGES = "languages";

    private final GitHubApiClient client;
    private final BigQueryLoader loader;
    private final RepositoryExtractor repoExtractor;
    private final CommitExtractor commitExtractor;
    private final PullRequestExtractor prExtractor;
    private final ReviewExtractor reviewExtractor;
    private final LanguageExtractor languageExtractor;

    public ExtractionOrchestrator(GitHubApiClient client, BigQueryLoader loader) {
        this.client = client;
        this.loader = loader;
        this.repoExtractor = new RepositoryExtractor(client, loader);
        this.commitExtractor = new CommitExtractor(client, loader);
        this.prExtractor = new PullRequestExtractor(client, loader);
        this.reviewExtractor = new ReviewExtractor(client, loader);
        this.languageExtractor = new LanguageExtractor(client, loader);
    }

    // Visible for testing
    ExtractionOrchestrator(GitHubApiClient client, BigQueryLoader loader,
                           RepositoryExtractor repoExtractor, CommitExtractor commitExtractor,
                           PullRequestExtractor prExtractor, ReviewExtractor reviewExtractor,
                           LanguageExtractor languageExtractor) {
        this.client = client;
        this.loader = loader;
        this.repoExtractor = repoExtractor;
        this.commitExtractor = commitExtractor;
        this.prExtractor = prExtractor;
        this.reviewExtractor = reviewExtractor;
        this.languageExtractor = languageExtractor;
    }

    /**
     * Runs the full extraction pipeline.
     *
     * @param fullMode if true, ignores last extraction timestamps (full extraction);
     *                 if false, uses incremental mode
     * @return summary of all extraction results
     */
    public ExtractionSummary run(boolean fullMode) {
        Instant runStart = Instant.now();
        logger.info("Starting {} extraction", fullMode ? "FULL" : "INCREMENTAL");

        List<ExtractionResult> results = new ArrayList<>();

        // Ensure BigQuery infrastructure exists
        try {
            loader.ensureDatasetsExist();
            loader.ensureTablesExist();
        } catch (Exception e) {
            logger.error("Failed to initialize BigQuery infrastructure", e);
            results.add(ExtractionResult.failure("infrastructure", "N/A",
                    e.getMessage(), elapsed(runStart)));
            return new ExtractionSummary(results, elapsed(runStart));
        }

        // Read last extraction timestamps for incremental mode
        Instant commitsSince = null;
        Instant prsSince = null;
        if (!fullMode) {
            commitsSince = loader.getLastExtractionTimestamp(ENTITY_COMMITS);
            prsSince = loader.getLastExtractionTimestamp(ENTITY_PULL_REQUESTS);
            logger.info("Incremental mode — commits since: {}, PRs since: {}",
                    commitsSince != null ? commitsSince : "none (full)",
                    prsSince != null ? prsSince : "none (full)");
        }

        Instant extractionTimestamp = Instant.now();

        // Step 1: Extract repositories
        List<Repository> repositories = List.of();
        long stepStart = System.currentTimeMillis();
        try {
            repositories = repoExtractor.extractAndLoad();
            results.add(ExtractionResult.success(ENTITY_REPOSITORIES, "all",
                    repositories.size(), repositories.size(),
                    System.currentTimeMillis() - stepStart));
        } catch (Exception e) {
            logger.error("Failed to extract repositories", e);
            results.add(ExtractionResult.failure(ENTITY_REPOSITORIES, "all",
                    e.getMessage(), System.currentTimeMillis() - stepStart));
        }

        // Step 2-4: For each repository, extract commits, PRs, reviews, languages
        for (Repository repo : repositories) {
            String repoName = repo.fullName();

            // Commits
            stepStart = System.currentTimeMillis();
            try {
                int count = commitExtractor.extractAndLoad(repoName, commitsSince);
                results.add(ExtractionResult.success(ENTITY_COMMITS, repoName,
                        count, count, System.currentTimeMillis() - stepStart));
            } catch (Exception e) {
                logger.error("Failed to extract commits for {}", repoName, e);
                results.add(ExtractionResult.failure(ENTITY_COMMITS, repoName,
                        e.getMessage(), System.currentTimeMillis() - stepStart));
            }

            // Pull Requests
            List<PullRequest> pullRequests = List.of();
            stepStart = System.currentTimeMillis();
            try {
                pullRequests = prExtractor.extractAndLoad(repoName, prsSince);
                results.add(ExtractionResult.success(ENTITY_PULL_REQUESTS, repoName,
                        pullRequests.size(), pullRequests.size(),
                        System.currentTimeMillis() - stepStart));
            } catch (Exception e) {
                logger.error("Failed to extract pull requests for {}", repoName, e);
                results.add(ExtractionResult.failure(ENTITY_PULL_REQUESTS, repoName,
                        e.getMessage(), System.currentTimeMillis() - stepStart));
            }

            // Reviews (for each PR)
            stepStart = System.currentTimeMillis();
            try {
                int count = reviewExtractor.extractAndLoad(repoName, pullRequests);
                results.add(ExtractionResult.success(ENTITY_REVIEWS, repoName,
                        count, count, System.currentTimeMillis() - stepStart));
            } catch (Exception e) {
                logger.error("Failed to extract reviews for {}", repoName, e);
                results.add(ExtractionResult.failure(ENTITY_REVIEWS, repoName,
                        e.getMessage(), System.currentTimeMillis() - stepStart));
            }

            // Languages
            stepStart = System.currentTimeMillis();
            try {
                int count = languageExtractor.extractAndLoad(List.of(repo));
                results.add(ExtractionResult.success(ENTITY_LANGUAGES, repoName,
                        count, count, System.currentTimeMillis() - stepStart));
            } catch (Exception e) {
                logger.error("Failed to extract languages for {}", repoName, e);
                results.add(ExtractionResult.failure(ENTITY_LANGUAGES, repoName,
                        e.getMessage(), System.currentTimeMillis() - stepStart));
            }
        }

        // Update extraction metadata for successful entity types
        updateMetadataIfSuccessful(results, ENTITY_COMMITS, extractionTimestamp);
        updateMetadataIfSuccessful(results, ENTITY_PULL_REQUESTS, extractionTimestamp);
        updateMetadataIfSuccessful(results, ENTITY_REVIEWS, extractionTimestamp);
        updateMetadataIfSuccessful(results, ENTITY_REPOSITORIES, extractionTimestamp);
        updateMetadataIfSuccessful(results, ENTITY_LANGUAGES, extractionTimestamp);

        ExtractionSummary summary = new ExtractionSummary(results, elapsed(runStart));
        logSummary(summary);
        return summary;
    }

    private void updateMetadataIfSuccessful(List<ExtractionResult> results, String entityType,
                                            Instant timestamp) {
        boolean anySuccess = results.stream()
                .anyMatch(r -> r.entityType().equals(entityType) && r.success());
        boolean anyFailure = results.stream()
                .anyMatch(r -> r.entityType().equals(entityType) && !r.success());

        if (anySuccess && !anyFailure) {
            try {
                loader.updateLastExtractionTimestamp(entityType, timestamp);
            } catch (Exception e) {
                logger.error("Failed to update extraction metadata for {}", entityType, e);
            }
        }
    }

    private void logSummary(ExtractionSummary summary) {
        logger.info("=== Extraction Summary ===");
        logger.info("Total duration: {}ms", summary.totalDurationMs());
        logger.info("Total results: {} ({} successful, {} failed)",
                summary.results().size(), summary.successCount(), summary.failureCount());

        for (String entityType : List.of(ENTITY_REPOSITORIES, ENTITY_COMMITS,
                ENTITY_PULL_REQUESTS, ENTITY_REVIEWS, ENTITY_LANGUAGES)) {
            int extracted = summary.totalExtractedForEntity(entityType);
            int loaded = summary.totalLoadedForEntity(entityType);
            long failures = summary.results().stream()
                    .filter(r -> r.entityType().equals(entityType) && !r.success())
                    .count();
            logger.info("  {}: extracted={}, loaded={}, failures={}",
                    entityType, extracted, loaded, failures);
        }

        if (summary.hasFailures()) {
            logger.warn("Extraction completed with {} failures", summary.failureCount());
            summary.results().stream()
                    .filter(r -> !r.success())
                    .forEach(r -> logger.warn("  FAILED: {} [{}] — {}",
                            r.entityType(), r.repoFullName(), r.errorMessage()));
        }
    }

    private long elapsed(Instant start) {
        return Instant.now().toEpochMilli() - start.toEpochMilli();
    }
}
