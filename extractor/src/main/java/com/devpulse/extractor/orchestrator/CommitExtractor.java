package com.devpulse.extractor.orchestrator;

import com.devpulse.extractor.client.GitHubApiClient;
import com.devpulse.extractor.loader.BigQueryLoader;
import com.devpulse.extractor.loader.InsertResult;
import com.devpulse.extractor.model.Commit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;

/**
 * Extracts commits from GitHub for a given repository and loads them into BigQuery.
 * Supports incremental extraction by filtering commits since last extraction.
 */
public class CommitExtractor {

    private static final Logger logger = LoggerFactory.getLogger(CommitExtractor.class);

    private final GitHubApiClient client;
    private final BigQueryLoader loader;

    public CommitExtractor(GitHubApiClient client, BigQueryLoader loader) {
        this.client = client;
        this.loader = loader;
    }

    /**
     * Extracts commits for a repository and loads them into BigQuery.
     *
     * @param repoFullName the full repository name (owner/repo)
     * @param since        if non-null, only commits after this timestamp are kept
     * @return the number of commits loaded
     */
    public int extractAndLoad(String repoFullName, Instant since) throws Exception {
        logger.info("Extracting commits for {} (since: {})", repoFullName,
                since != null ? since : "full");

        List<Commit> commits = client.getCommits(repoFullName);

        if (since != null) {
            commits = commits.stream()
                    .filter(c -> {
                        if (c.commit() == null || c.commit().author() == null
                                || c.commit().author().date() == null) {
                            return true; // include commits with missing dates
                        }
                        try {
                            Instant commitDate = Instant.parse(c.commit().author().date());
                            return commitDate.isAfter(since);
                        } catch (Exception e) {
                            return true; // include if date can't be parsed
                        }
                    })
                    .toList();
        }

        logger.info("Fetched {} commits for {}", commits.size(), repoFullName);

        if (commits.isEmpty()) {
            return 0;
        }

        InsertResult result = loader.loadCommits(repoFullName, commits);
        if (result.hasErrors()) {
            logger.warn("Commit load for {} had {} errors out of {} rows",
                    repoFullName, result.errors().size(), result.totalRows());
        }
        return result.successfulRows();
    }
}
