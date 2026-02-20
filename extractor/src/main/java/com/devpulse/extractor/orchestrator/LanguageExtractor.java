package com.devpulse.extractor.orchestrator;

import com.devpulse.extractor.client.GitHubApiClient;
import com.devpulse.extractor.loader.BigQueryLoader;
import com.devpulse.extractor.loader.InsertResult;
import com.devpulse.extractor.model.Language;
import com.devpulse.extractor.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Extracts language statistics for each repository and loads them into BigQuery.
 * Each repo's language map is flattened into individual rows by BigQueryLoader.
 */
public class LanguageExtractor {

    private static final Logger logger = LoggerFactory.getLogger(LanguageExtractor.class);

    private final GitHubApiClient client;
    private final BigQueryLoader loader;

    public LanguageExtractor(GitHubApiClient client, BigQueryLoader loader) {
        this.client = client;
        this.loader = loader;
    }

    /**
     * Extracts language stats for all given repositories and loads into BigQuery.
     *
     * @param repositories the repositories to fetch languages for
     * @return the number of language rows loaded
     */
    public int extractAndLoad(List<Repository> repositories) throws Exception {
        List<Language> allLanguages = new ArrayList<>();

        for (Repository repo : repositories) {
            logger.debug("Extracting languages for {}", repo.fullName());
            Language language = client.getLanguages(repo.fullName());
            if (language.languages() != null && !language.languages().isEmpty()) {
                allLanguages.add(language);
            }
        }

        logger.info("Fetched language data for {} repositories ({} with languages)",
                repositories.size(), allLanguages.size());

        if (allLanguages.isEmpty()) {
            return 0;
        }

        InsertResult result = loader.loadLanguages(allLanguages);
        if (result.hasErrors()) {
            logger.warn("Language load had {} errors out of {} rows",
                    result.errors().size(), result.totalRows());
        }
        return result.successfulRows();
    }
}
