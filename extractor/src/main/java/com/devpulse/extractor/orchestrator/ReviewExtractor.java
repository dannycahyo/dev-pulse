package com.devpulse.extractor.orchestrator;

import com.devpulse.extractor.client.GitHubApiClient;
import com.devpulse.extractor.loader.BigQueryLoader;
import com.devpulse.extractor.loader.InsertResult;
import com.devpulse.extractor.model.PullRequest;
import com.devpulse.extractor.model.Review;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Extracts reviews for each pull request and loads them into BigQuery.
 */
public class ReviewExtractor {

    private static final Logger logger = LoggerFactory.getLogger(ReviewExtractor.class);

    private final GitHubApiClient client;
    private final BigQueryLoader loader;

    public ReviewExtractor(GitHubApiClient client, BigQueryLoader loader) {
        this.client = client;
        this.loader = loader;
    }

    /**
     * Extracts reviews for all given pull requests in a repository and loads into BigQuery.
     *
     * @param repoFullName the full repository name (owner/repo)
     * @param pullRequests the pull requests to fetch reviews for
     * @return total number of reviews loaded
     */
    public int extractAndLoad(String repoFullName, List<PullRequest> pullRequests) throws Exception {
        int totalLoaded = 0;

        for (PullRequest pr : pullRequests) {
            logger.debug("Extracting reviews for PR #{} in {}", pr.number(), repoFullName);

            List<Review> reviews = client.getReviews(repoFullName, pr.number());

            if (!reviews.isEmpty()) {
                InsertResult result = loader.loadReviews(repoFullName, pr.number(), reviews);
                if (result.hasErrors()) {
                    logger.warn("Review load for {} PR#{} had {} errors",
                            repoFullName, pr.number(), result.errors().size());
                }
                totalLoaded += result.successfulRows();
            }
        }

        logger.info("Loaded {} reviews for {} across {} PRs",
                totalLoaded, repoFullName, pullRequests.size());
        return totalLoaded;
    }
}
