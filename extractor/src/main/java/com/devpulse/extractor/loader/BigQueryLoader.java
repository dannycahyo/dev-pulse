package com.devpulse.extractor.loader;

import com.devpulse.extractor.model.*;
import com.google.cloud.bigquery.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

/**
 * BigQuery writer component responsible for loading extracted GitHub data
 * into the raw dataset tables. Handles dataset/table creation, streaming
 * inserts, and extraction metadata tracking for incremental extraction.
 */
public class BigQueryLoader {

    private static final Logger logger = LoggerFactory.getLogger(BigQueryLoader.class);

    static final String RAW_DATASET = "devpulse_raw";
    static final String STAGING_DATASET = "devpulse_staging";
    static final String MART_DATASET = "devpulse_mart";

    private final BigQuery bigQuery;
    private final String projectId;

    /**
     * Production constructor. Initializes BigQuery client using service account
     * credentials from GOOGLE_APPLICATION_CREDENTIALS environment variable.
     */
    public BigQueryLoader(String projectId) {
        this.projectId = projectId;
        this.bigQuery = BigQueryOptions.newBuilder()
                .setProjectId(projectId)
                .build()
                .getService();
        logger.info("BigQueryLoader initialized for project: {}", projectId);
    }

    /**
     * Test constructor. Accepts an injected BigQuery client for mocking.
     */
    BigQueryLoader(BigQuery bigQuery, String projectId) {
        this.bigQuery = bigQuery;
        this.projectId = projectId;
    }

    // =========================================================================
    // Dataset management
    // =========================================================================

    /**
     * Creates the devpulse_raw, devpulse_staging, and devpulse_mart datasets
     * if they don't already exist.
     */
    public void ensureDatasetsExist() {
        for (String datasetName : List.of(RAW_DATASET, STAGING_DATASET, MART_DATASET)) {
            DatasetId datasetId = DatasetId.of(projectId, datasetName);
            if (bigQuery.getDataset(datasetId) == null) {
                DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetId)
                        .setLocation("US")
                        .build();
                bigQuery.create(datasetInfo);
                logger.info("Created dataset: {}", datasetName);
            } else {
                logger.debug("Dataset already exists: {}", datasetName);
            }
        }
    }

    // =========================================================================
    // Table management
    // =========================================================================

    /**
     * Creates all raw layer tables and the extraction metadata table with
     * proper schemas, partitioning (by ingestion_timestamp DAY), and clustering
     * as defined in TRD Section 2.2.2.
     */
    public void ensureTablesExist() {
        createTableIfNotExists(RAW_DATASET, BigQuerySchemas.TABLE_RAW_COMMITS,
                BigQuerySchemas.rawCommitsDefinition());
        createTableIfNotExists(RAW_DATASET, BigQuerySchemas.TABLE_RAW_PULL_REQUESTS,
                BigQuerySchemas.rawPullRequestsDefinition());
        createTableIfNotExists(RAW_DATASET, BigQuerySchemas.TABLE_RAW_REVIEWS,
                BigQuerySchemas.rawReviewsDefinition());
        createTableIfNotExists(RAW_DATASET, BigQuerySchemas.TABLE_RAW_REPOSITORIES,
                BigQuerySchemas.rawRepositoriesDefinition());
        createTableIfNotExists(RAW_DATASET, BigQuerySchemas.TABLE_RAW_LANGUAGES,
                BigQuerySchemas.rawLanguagesDefinition());
        createTableIfNotExists(RAW_DATASET, BigQuerySchemas.TABLE_EXTRACTION_METADATA,
                BigQuerySchemas.extractionMetadataDefinition());
    }

    private void createTableIfNotExists(String dataset, String tableName, TableDefinition definition) {
        TableId tableId = TableId.of(projectId, dataset, tableName);
        if (bigQuery.getTable(tableId) == null) {
            TableInfo tableInfo = TableInfo.newBuilder(tableId, definition).build();
            bigQuery.create(tableInfo);
            logger.info("Created table: {}.{}", dataset, tableName);
        } else {
            logger.debug("Table already exists: {}.{}", dataset, tableName);
        }
    }

    // =========================================================================
    // Generic row insertion
    // =========================================================================

    /**
     * Inserts rows into a BigQuery table using streaming inserts (InsertAllRequest).
     * Automatically adds ingestion_timestamp to every row. Handles insert errors
     * with logging and partial failure reporting.
     *
     * @param datasetName target dataset
     * @param tableName   target table
     * @param rows        list of row data as maps
     * @return InsertResult with success/failure details
     */
    public InsertResult insertRows(String datasetName, String tableName,
                                   List<Map<String, Object>> rows) {
        if (rows == null || rows.isEmpty()) {
            logger.debug("No rows to insert into {}.{}", datasetName, tableName);
            return new InsertResult(0, 0, List.of());
        }

        TableId tableId = TableId.of(projectId, datasetName, tableName);
        String now = Instant.now().toString();

        InsertAllRequest.Builder requestBuilder = InsertAllRequest.newBuilder(tableId);
        for (Map<String, Object> row : rows) {
            Map<String, Object> rowWithTimestamp = new HashMap<>(row);
            rowWithTimestamp.put("ingestion_timestamp", now);
            requestBuilder.addRow(rowWithTimestamp);
        }

        InsertAllResponse response = bigQuery.insertAll(requestBuilder.build());

        List<InsertResult.RowError> errors = new ArrayList<>();
        if (response.hasErrors()) {
            for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                long rowIndex = entry.getKey();
                for (BigQueryError error : entry.getValue()) {
                    errors.add(new InsertResult.RowError(rowIndex, error.getMessage()));
                    logger.error("Insert error in {}.{} row {}: {} (reason: {})",
                            datasetName, tableName, rowIndex,
                            error.getMessage(), error.getReason());
                }
            }
        }

        int successfulRows = rows.size() - response.getInsertErrors().size();
        logger.info("Inserted {}/{} rows into {}.{} ({} errors)",
                successfulRows, rows.size(), datasetName, tableName, errors.size());

        return new InsertResult(rows.size(), successfulRows, errors);
    }

    // =========================================================================
    // Entity-specific loaders
    // =========================================================================

    /**
     * Loads commit data into raw_commits table.
     */
    public InsertResult loadCommits(String repoFullName, List<Commit> commits) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Commit commit : commits) {
            Map<String, Object> row = new HashMap<>();
            row.put("sha", commit.sha());
            row.put("repo_full_name", repoFullName);
            if (commit.commit() != null && commit.commit().author() != null) {
                row.put("author_name", commit.commit().author().name());
                row.put("author_email", commit.commit().author().email());
                row.put("author_date", commit.commit().author().date());
            }
            if (commit.commit() != null) {
                row.put("message", commit.commit().message());
            }
            if (commit.stats() != null) {
                row.put("additions", commit.stats().additions());
                row.put("deletions", commit.stats().deletions());
                row.put("changed_files", commit.stats().total());
            }
            rows.add(row);
        }
        return insertRows(RAW_DATASET, BigQuerySchemas.TABLE_RAW_COMMITS, rows);
    }

    /**
     * Loads pull request data into raw_pull_requests table.
     */
    public InsertResult loadPullRequests(String repoFullName, List<PullRequest> pullRequests) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (PullRequest pr : pullRequests) {
            Map<String, Object> row = new HashMap<>();
            row.put("number", pr.number());
            row.put("repo_full_name", repoFullName);
            row.put("title", pr.title());
            row.put("state", pr.state());
            row.put("user_login", pr.user() != null ? pr.user().login() : null);
            row.put("created_at", pr.createdAt());
            row.put("updated_at", pr.updatedAt());
            row.put("merged_at", pr.mergedAt());
            row.put("merge_commit_sha", pr.mergeCommitSha());
            rows.add(row);
        }
        return insertRows(RAW_DATASET, BigQuerySchemas.TABLE_RAW_PULL_REQUESTS, rows);
    }

    /**
     * Loads review data into raw_reviews table.
     */
    public InsertResult loadReviews(String repoFullName, int prNumber, List<Review> reviews) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Review review : reviews) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", review.id());
            row.put("repo_full_name", repoFullName);
            row.put("pr_number", prNumber);
            row.put("user_login", review.user() != null ? review.user().login() : null);
            row.put("state", review.state());
            row.put("submitted_at", review.submittedAt());
            row.put("body", review.body());
            rows.add(row);
        }
        return insertRows(RAW_DATASET, BigQuerySchemas.TABLE_RAW_REVIEWS, rows);
    }

    /**
     * Loads repository metadata into raw_repositories table.
     */
    public InsertResult loadRepositories(List<Repository> repositories) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Repository repo : repositories) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", repo.id());
            row.put("name", repo.name());
            row.put("full_name", repo.fullName());
            row.put("owner_login", repo.owner() != null ? repo.owner().login() : null);
            row.put("language", repo.language());
            row.put("created_at", repo.createdAt());
            row.put("updated_at", repo.updatedAt());
            row.put("visibility", repo.visibility());
            row.put("fork", repo.fork());
            row.put("stargazers_count", repo.stargazersCount());
            rows.add(row);
        }
        return insertRows(RAW_DATASET, BigQuerySchemas.TABLE_RAW_REPOSITORIES, rows);
    }

    /**
     * Loads language data into raw_languages table. Each Language record contains
     * a map of language_name to byte_count, which is flattened into individual rows.
     */
    public InsertResult loadLanguages(List<Language> languages) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Language lang : languages) {
            if (lang.languages() == null) continue;
            for (Map.Entry<String, Long> entry : lang.languages().entrySet()) {
                Map<String, Object> row = new HashMap<>();
                row.put("repo_full_name", lang.repoFullName());
                row.put("language_name", entry.getKey());
                row.put("byte_count", entry.getValue());
                rows.add(row);
            }
        }
        return insertRows(RAW_DATASET, BigQuerySchemas.TABLE_RAW_LANGUAGES, rows);
    }

    // =========================================================================
    // Extraction metadata (incremental extraction support)
    // =========================================================================

    /**
     * Gets the last successful extraction timestamp for a given entity type.
     * Returns null if no previous extraction exists.
     *
     * @param entityType the entity type (e.g., "commits", "pull_requests")
     * @return the last extraction timestamp, or null if not found
     */
    public Instant getLastExtractionTimestamp(String entityType) {
        String query = String.format(
                "SELECT last_extracted_at FROM `%s.%s.%s` WHERE entity_type = @entityType",
                projectId, RAW_DATASET, BigQuerySchemas.TABLE_EXTRACTION_METADATA);

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query)
                .addNamedParameter("entityType", QueryParameterValue.string(entityType))
                .setUseLegacySql(false)
                .build();

        try {
            TableResult result = bigQuery.query(queryConfig);
            for (FieldValueList row : result.iterateAll()) {
                if (!row.get("last_extracted_at").isNull()) {
                    long micros = row.get("last_extracted_at").getTimestampValue();
                    return Instant.ofEpochSecond(
                            micros / 1_000_000,
                            (micros % 1_000_000) * 1_000);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while querying extraction metadata for: {}", entityType, e);
        } catch (BigQueryException e) {
            logger.error("Failed to query extraction metadata for: {}", entityType, e);
        }

        return null;
    }

    /**
     * Updates (or inserts) the last successful extraction timestamp for a given
     * entity type using a MERGE statement for upsert semantics.
     *
     * @param entityType the entity type (e.g., "commits", "pull_requests")
     * @param timestamp  the extraction timestamp to record
     */
    public void updateLastExtractionTimestamp(String entityType, Instant timestamp) {
        String query = String.format(
                "MERGE `%s.%s.%s` T "
                        + "USING (SELECT @entityType AS entity_type, @timestamp AS last_extracted_at, "
                        + "CURRENT_TIMESTAMP() AS updated_at) S "
                        + "ON T.entity_type = S.entity_type "
                        + "WHEN MATCHED THEN UPDATE SET "
                        + "last_extracted_at = S.last_extracted_at, updated_at = S.updated_at "
                        + "WHEN NOT MATCHED THEN INSERT (entity_type, last_extracted_at, updated_at) "
                        + "VALUES (S.entity_type, S.last_extracted_at, S.updated_at)",
                projectId, RAW_DATASET, BigQuerySchemas.TABLE_EXTRACTION_METADATA);

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query)
                .addNamedParameter("entityType", QueryParameterValue.string(entityType))
                .addNamedParameter("timestamp", QueryParameterValue.timestamp(
                        timestamp.getEpochSecond() * 1_000_000 + timestamp.getNano() / 1_000))
                .setUseLegacySql(false)
                .build();

        try {
            bigQuery.query(queryConfig);
            logger.info("Updated extraction metadata for {}: {}", entityType, timestamp);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while updating extraction metadata for: {}", entityType, e);
        } catch (BigQueryException e) {
            logger.error("Failed to update extraction metadata for: {}", entityType, e);
            throw e;
        }
    }
}
