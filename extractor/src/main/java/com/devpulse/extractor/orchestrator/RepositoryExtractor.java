package com.devpulse.extractor.orchestrator;

import com.devpulse.extractor.client.GitHubApiClient;
import com.devpulse.extractor.loader.BigQueryLoader;
import com.devpulse.extractor.loader.InsertResult;
import com.devpulse.extractor.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Extracts repository metadata from GitHub and loads into BigQuery.
 */
public class RepositoryExtractor {

    private static final Logger logger = LoggerFactory.getLogger(RepositoryExtractor.class);

    private final GitHubApiClient client;
    private final BigQueryLoader loader;

    public RepositoryExtractor(GitHubApiClient client, BigQueryLoader loader) {
        this.client = client;
        this.loader = loader;
    }

    /**
     * Fetches all user repositories and loads them into BigQuery.
     *
     * @return the list of repositories (for use by downstream extractors)
     */
    public List<Repository> extractAndLoad() throws Exception {
        logger.info("Extracting repositories...");
        List<Repository> repositories = client.getRepositories();
        logger.info("Fetched {} repositories from GitHub", repositories.size());

        if (!repositories.isEmpty()) {
            InsertResult result = loader.loadRepositories(repositories);
            if (result.hasErrors()) {
                logger.warn("Repository load had {} errors out of {} rows",
                        result.errors().size(), result.totalRows());
            }
        }

        return repositories;
    }
}
