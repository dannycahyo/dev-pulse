package com.devpulse.extractor.loader;

import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TimePartitioning;

import java.util.List;

/**
 * Defines BigQuery table schemas for all raw layer tables per TRD Section 2.2.2.
 * Each table is partitioned by ingestion_timestamp (DAY) and clustered as specified.
 */
public final class BigQuerySchemas {

    private BigQuerySchemas() {}

    // =========================================================================
    // Raw table names
    // =========================================================================

    public static final String TABLE_RAW_COMMITS = "raw_commits";
    public static final String TABLE_RAW_PULL_REQUESTS = "raw_pull_requests";
    public static final String TABLE_RAW_REVIEWS = "raw_reviews";
    public static final String TABLE_RAW_REPOSITORIES = "raw_repositories";
    public static final String TABLE_RAW_LANGUAGES = "raw_languages";
    public static final String TABLE_EXTRACTION_METADATA = "_extraction_metadata";

    // =========================================================================
    // Schema definitions
    // =========================================================================

    public static Schema rawCommitsSchema() {
        return Schema.of(
                Field.newBuilder("sha", StandardSQLTypeName.STRING)
                        .setMode(Field.Mode.REQUIRED).build(),
                Field.of("repo_full_name", StandardSQLTypeName.STRING),
                Field.of("author_name", StandardSQLTypeName.STRING),
                Field.of("author_email", StandardSQLTypeName.STRING),
                Field.of("author_date", StandardSQLTypeName.TIMESTAMP),
                Field.of("message", StandardSQLTypeName.STRING),
                Field.of("additions", StandardSQLTypeName.INT64),
                Field.of("deletions", StandardSQLTypeName.INT64),
                Field.of("changed_files", StandardSQLTypeName.INT64),
                Field.newBuilder("ingestion_timestamp", StandardSQLTypeName.TIMESTAMP)
                        .setMode(Field.Mode.REQUIRED).build()
        );
    }

    public static Schema rawPullRequestsSchema() {
        return Schema.of(
                Field.newBuilder("number", StandardSQLTypeName.INT64)
                        .setMode(Field.Mode.REQUIRED).build(),
                Field.of("repo_full_name", StandardSQLTypeName.STRING),
                Field.of("title", StandardSQLTypeName.STRING),
                Field.of("state", StandardSQLTypeName.STRING),
                Field.of("user_login", StandardSQLTypeName.STRING),
                Field.of("created_at", StandardSQLTypeName.TIMESTAMP),
                Field.of("updated_at", StandardSQLTypeName.TIMESTAMP),
                Field.of("merged_at", StandardSQLTypeName.TIMESTAMP),
                Field.of("merge_commit_sha", StandardSQLTypeName.STRING),
                Field.newBuilder("ingestion_timestamp", StandardSQLTypeName.TIMESTAMP)
                        .setMode(Field.Mode.REQUIRED).build()
        );
    }

    public static Schema rawReviewsSchema() {
        return Schema.of(
                Field.newBuilder("id", StandardSQLTypeName.INT64)
                        .setMode(Field.Mode.REQUIRED).build(),
                Field.of("repo_full_name", StandardSQLTypeName.STRING),
                Field.of("pr_number", StandardSQLTypeName.INT64),
                Field.of("user_login", StandardSQLTypeName.STRING),
                Field.of("state", StandardSQLTypeName.STRING),
                Field.of("submitted_at", StandardSQLTypeName.TIMESTAMP),
                Field.of("body", StandardSQLTypeName.STRING),
                Field.newBuilder("ingestion_timestamp", StandardSQLTypeName.TIMESTAMP)
                        .setMode(Field.Mode.REQUIRED).build()
        );
    }

    public static Schema rawRepositoriesSchema() {
        return Schema.of(
                Field.newBuilder("id", StandardSQLTypeName.INT64)
                        .setMode(Field.Mode.REQUIRED).build(),
                Field.of("name", StandardSQLTypeName.STRING),
                Field.of("full_name", StandardSQLTypeName.STRING),
                Field.of("owner_login", StandardSQLTypeName.STRING),
                Field.of("language", StandardSQLTypeName.STRING),
                Field.of("created_at", StandardSQLTypeName.TIMESTAMP),
                Field.of("updated_at", StandardSQLTypeName.TIMESTAMP),
                Field.of("visibility", StandardSQLTypeName.STRING),
                Field.of("fork", StandardSQLTypeName.BOOL),
                Field.of("stargazers_count", StandardSQLTypeName.INT64),
                Field.newBuilder("ingestion_timestamp", StandardSQLTypeName.TIMESTAMP)
                        .setMode(Field.Mode.REQUIRED).build()
        );
    }

    public static Schema rawLanguagesSchema() {
        return Schema.of(
                Field.of("repo_full_name", StandardSQLTypeName.STRING),
                Field.of("language_name", StandardSQLTypeName.STRING),
                Field.of("byte_count", StandardSQLTypeName.INT64),
                Field.newBuilder("ingestion_timestamp", StandardSQLTypeName.TIMESTAMP)
                        .setMode(Field.Mode.REQUIRED).build()
        );
    }

    public static Schema extractionMetadataSchema() {
        return Schema.of(
                Field.newBuilder("entity_type", StandardSQLTypeName.STRING)
                        .setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("last_extracted_at", StandardSQLTypeName.TIMESTAMP)
                        .setMode(Field.Mode.REQUIRED).build(),
                Field.of("updated_at", StandardSQLTypeName.TIMESTAMP)
        );
    }

    // =========================================================================
    // Table definition builders (with partitioning and clustering)
    // =========================================================================

    public static TableDefinition rawCommitsDefinition() {
        return buildPartitionedTable(rawCommitsSchema(), List.of("repo_full_name"));
    }

    public static TableDefinition rawPullRequestsDefinition() {
        return buildPartitionedTable(rawPullRequestsSchema(), List.of("repo_full_name", "state"));
    }

    public static TableDefinition rawReviewsDefinition() {
        return buildPartitionedTable(rawReviewsSchema(), List.of("repo_full_name"));
    }

    public static TableDefinition rawRepositoriesDefinition() {
        return buildPartitionedTable(rawRepositoriesSchema(), List.of("language"));
    }

    public static TableDefinition rawLanguagesDefinition() {
        return buildPartitionedTable(rawLanguagesSchema(), List.of("repo_full_name"));
    }

    public static TableDefinition extractionMetadataDefinition() {
        return StandardTableDefinition.newBuilder()
                .setSchema(extractionMetadataSchema())
                .build();
    }

    private static TableDefinition buildPartitionedTable(Schema schema, List<String> clusterFields) {
        TimePartitioning partitioning = TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
                .setField("ingestion_timestamp")
                .build();

        Clustering clustering = Clustering.newBuilder()
                .setFields(clusterFields)
                .build();

        return StandardTableDefinition.newBuilder()
                .setSchema(schema)
                .setTimePartitioning(partitioning)
                .setClustering(clustering)
                .build();
    }
}
