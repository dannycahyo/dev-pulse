package com.devpulse.extractor.orchestrator;

import com.devpulse.extractor.client.GitHubApiClient;
import com.devpulse.extractor.loader.BigQueryLoader;
import com.devpulse.extractor.loader.InsertResult;
import com.devpulse.extractor.model.PullRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;

/**
 * Extracts pull requests from GitHub for a given repository and loads them into BigQuery.
 * Supports incremental extraction by filtering PRs updated since last extraction.
 */
public class PullRequestExtractor {

    private static final Logger logger = LoggerFactory.getLogger(PullRequestExtractor.class);

    private final GitHubApiClient client;
    private final BigQueryLoader loader;

    public PullRequestExtractor(GitHubApiClient client, BigQueryLoader loader) {
        this.client = client;
        this.loader = loader;
    }

    /**
     * Extracts pull requests for a repository and loads them into BigQuery.
     *
     * @param repoFullName the full repository name (owner/repo)
     * @param since        if non-null, only PRs updated after this timestamp are kept
     * @return the list of pull requests (for use by ReviewExtractor)
     */
    public List<PullRequest> extractAndLoad(String repoFullName, Instant since) throws Exception {
        logger.info("Extracting pull requests for {} (since: {})", repoFullName,
                since != null ? since : "full");

        List<PullRequest> pullRequests = client.getPullRequests(repoFullName);

        if (since != null) {
            pullRequests = pullRequests.stream()
                    .filter(pr -> {
                        if (pr.updatedAt() == null) {
                            return true;
                        }
                        try {
                            Instant updatedAt = Instant.parse(pr.updatedAt());
                            return updatedAt.isAfter(since);
                        } catch (Exception e) {
                            return true;
                        }
                    })
                    .toList();
        }

        logger.info("Fetched {} pull requests for {}", pullRequests.size(), repoFullName);

        if (!pullRequests.isEmpty()) {
            InsertResult result = loader.loadPullRequests(repoFullName, pullRequests);
            if (result.hasErrors()) {
                logger.warn("PR load for {} had {} errors out of {} rows",
                        repoFullName, result.errors().size(), result.totalRows());
            }
        }

        return pullRequests;
    }
}
