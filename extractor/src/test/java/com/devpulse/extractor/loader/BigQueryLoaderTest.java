package com.devpulse.extractor.loader;

import com.devpulse.extractor.model.*;
import com.google.cloud.bigquery.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Integration tests for {@link BigQueryLoader} using mocked BigQuery client.
 * Covers row insertion, schema creation, metadata tracking, and error handling.
 */
@ExtendWith(MockitoExtension.class)
class BigQueryLoaderTest {

    private static final String PROJECT_ID = "test-project";

    @Mock
    private BigQuery bigQuery;

    private BigQueryLoader loader;

    @BeforeEach
    void setUp() {
        loader = new BigQueryLoader(bigQuery, PROJECT_ID);
    }

    // =========================================================================
    // Dataset creation tests
    // =========================================================================

    @Test
    @DisplayName("ensureDatasetsExist creates all three datasets when none exist")
    void ensureDatasetsExist_createsAllDatasets() {
        when(bigQuery.getDataset(any(DatasetId.class))).thenReturn(null);
        when(bigQuery.create(any(DatasetInfo.class))).thenReturn(mock(Dataset.class));

        loader.ensureDatasetsExist();

        ArgumentCaptor<DatasetInfo> captor = ArgumentCaptor.forClass(DatasetInfo.class);
        verify(bigQuery, times(3)).create(captor.capture());

        List<String> createdDatasets = captor.getAllValues().stream()
                .map(info -> info.getDatasetId().getDataset())
                .toList();

        assertTrue(createdDatasets.contains("devpulse_raw"));
        assertTrue(createdDatasets.contains("devpulse_staging"));
        assertTrue(createdDatasets.contains("devpulse_mart"));
    }

    @Test
    @DisplayName("ensureDatasetsExist skips existing datasets")
    void ensureDatasetsExist_skipsExisting() {
        when(bigQuery.getDataset(any(DatasetId.class))).thenReturn(mock(Dataset.class));

        loader.ensureDatasetsExist();

        verify(bigQuery, never()).create(any(DatasetInfo.class));
    }

    // =========================================================================
    // Table creation tests
    // =========================================================================

    @Test
    @DisplayName("ensureTablesExist creates all six tables when none exist")
    void ensureTablesExist_createsAllTables() {
        when(bigQuery.getTable(any(TableId.class))).thenReturn(null);
        when(bigQuery.create(any(TableInfo.class))).thenReturn(mock(Table.class));

        loader.ensureTablesExist();

        ArgumentCaptor<TableInfo> captor = ArgumentCaptor.forClass(TableInfo.class);
        verify(bigQuery, times(6)).create(captor.capture());

        List<String> createdTables = captor.getAllValues().stream()
                .map(info -> info.getTableId().getTable())
                .toList();

        assertTrue(createdTables.contains("raw_commits"));
        assertTrue(createdTables.contains("raw_pull_requests"));
        assertTrue(createdTables.contains("raw_reviews"));
        assertTrue(createdTables.contains("raw_repositories"));
        assertTrue(createdTables.contains("raw_languages"));
        assertTrue(createdTables.contains("_extraction_metadata"));
    }

    @Test
    @DisplayName("ensureTablesExist skips existing tables")
    void ensureTablesExist_skipsExisting() {
        when(bigQuery.getTable(any(TableId.class))).thenReturn(mock(Table.class));

        loader.ensureTablesExist();

        verify(bigQuery, never()).create(any(TableInfo.class));
    }

    @Test
    @DisplayName("raw_commits table has DAY partitioning on ingestion_timestamp and clustering on repo_full_name")
    void rawCommitsTable_hasCorrectPartitioningAndClustering() {
        when(bigQuery.getTable(any(TableId.class))).thenReturn(null);
        when(bigQuery.create(any(TableInfo.class))).thenReturn(mock(Table.class));

        loader.ensureTablesExist();

        ArgumentCaptor<TableInfo> captor = ArgumentCaptor.forClass(TableInfo.class);
        verify(bigQuery, atLeastOnce()).create(captor.capture());

        TableInfo commitsTable = captor.getAllValues().stream()
                .filter(info -> "raw_commits".equals(info.getTableId().getTable()))
                .findFirst()
                .orElseThrow();

        StandardTableDefinition definition = (StandardTableDefinition) commitsTable.getDefinition();
        assertNotNull(definition.getTimePartitioning());
        assertEquals(TimePartitioning.Type.DAY, definition.getTimePartitioning().getType());
        assertEquals("ingestion_timestamp", definition.getTimePartitioning().getField());
        assertNotNull(definition.getClustering());
        assertEquals(List.of("repo_full_name"), definition.getClustering().getFields());
    }

    @Test
    @DisplayName("raw_pull_requests table clusters on repo_full_name and state")
    void rawPullRequestsTable_hasTwoClusterFields() {
        when(bigQuery.getTable(any(TableId.class))).thenReturn(null);
        when(bigQuery.create(any(TableInfo.class))).thenReturn(mock(Table.class));

        loader.ensureTablesExist();

        ArgumentCaptor<TableInfo> captor = ArgumentCaptor.forClass(TableInfo.class);
        verify(bigQuery, atLeastOnce()).create(captor.capture());

        TableInfo prTable = captor.getAllValues().stream()
                .filter(info -> "raw_pull_requests".equals(info.getTableId().getTable()))
                .findFirst()
                .orElseThrow();

        StandardTableDefinition definition = (StandardTableDefinition) prTable.getDefinition();
        assertEquals(List.of("repo_full_name", "state"), definition.getClustering().getFields());
    }

    @Test
    @DisplayName("raw_repositories table clusters on language")
    void rawRepositoriesTable_clustersOnLanguage() {
        when(bigQuery.getTable(any(TableId.class))).thenReturn(null);
        when(bigQuery.create(any(TableInfo.class))).thenReturn(mock(Table.class));

        loader.ensureTablesExist();

        ArgumentCaptor<TableInfo> captor = ArgumentCaptor.forClass(TableInfo.class);
        verify(bigQuery, atLeastOnce()).create(captor.capture());

        TableInfo repoTable = captor.getAllValues().stream()
                .filter(info -> "raw_repositories".equals(info.getTableId().getTable()))
                .findFirst()
                .orElseThrow();

        StandardTableDefinition definition = (StandardTableDefinition) repoTable.getDefinition();
        assertEquals(List.of("language"), definition.getClustering().getFields());
    }

    @Test
    @DisplayName("_extraction_metadata table has no partitioning or clustering")
    void extractionMetadataTable_hasNoPartitioning() {
        when(bigQuery.getTable(any(TableId.class))).thenReturn(null);
        when(bigQuery.create(any(TableInfo.class))).thenReturn(mock(Table.class));

        loader.ensureTablesExist();

        ArgumentCaptor<TableInfo> captor = ArgumentCaptor.forClass(TableInfo.class);
        verify(bigQuery, atLeastOnce()).create(captor.capture());

        TableInfo metaTable = captor.getAllValues().stream()
                .filter(info -> "_extraction_metadata".equals(info.getTableId().getTable()))
                .findFirst()
                .orElseThrow();

        StandardTableDefinition definition = (StandardTableDefinition) metaTable.getDefinition();
        assertNull(definition.getTimePartitioning());
        assertNull(definition.getClustering());
    }

    // =========================================================================
    // Schema field verification tests
    // =========================================================================

    @Test
    @DisplayName("raw_commits schema has all required fields")
    void rawCommitsSchema_hasAllFields() {
        Schema schema = BigQuerySchemas.rawCommitsSchema();
        List<String> fieldNames = schema.getFields().stream()
                .map(Field::getName)
                .toList();

        assertEquals(List.of(
                "sha", "repo_full_name", "author_name", "author_email",
                "author_date", "message", "additions", "deletions",
                "changed_files", "ingestion_timestamp"), fieldNames);
    }

    @Test
    @DisplayName("raw_pull_requests schema has all required fields")
    void rawPullRequestsSchema_hasAllFields() {
        Schema schema = BigQuerySchemas.rawPullRequestsSchema();
        List<String> fieldNames = schema.getFields().stream()
                .map(Field::getName)
                .toList();

        assertEquals(List.of(
                "number", "repo_full_name", "title", "state", "user_login",
                "created_at", "updated_at", "merged_at", "merge_commit_sha",
                "ingestion_timestamp"), fieldNames);
    }

    @Test
    @DisplayName("raw_reviews schema has all required fields")
    void rawReviewsSchema_hasAllFields() {
        Schema schema = BigQuerySchemas.rawReviewsSchema();
        List<String> fieldNames = schema.getFields().stream()
                .map(Field::getName)
                .toList();

        assertEquals(List.of(
                "id", "repo_full_name", "pr_number", "user_login",
                "state", "submitted_at", "body", "ingestion_timestamp"), fieldNames);
    }

    @Test
    @DisplayName("raw_repositories schema has all required fields")
    void rawRepositoriesSchema_hasAllFields() {
        Schema schema = BigQuerySchemas.rawRepositoriesSchema();
        List<String> fieldNames = schema.getFields().stream()
                .map(Field::getName)
                .toList();

        assertEquals(List.of(
                "id", "name", "full_name", "owner_login", "language",
                "created_at", "updated_at", "visibility", "fork",
                "stargazers_count", "ingestion_timestamp"), fieldNames);
    }

    @Test
    @DisplayName("raw_languages schema has all required fields")
    void rawLanguagesSchema_hasAllFields() {
        Schema schema = BigQuerySchemas.rawLanguagesSchema();
        List<String> fieldNames = schema.getFields().stream()
                .map(Field::getName)
                .toList();

        assertEquals(List.of(
                "repo_full_name", "language_name", "byte_count",
                "ingestion_timestamp"), fieldNames);
    }

    @Test
    @DisplayName("_extraction_metadata schema has all required fields")
    void extractionMetadataSchema_hasAllFields() {
        Schema schema = BigQuerySchemas.extractionMetadataSchema();
        List<String> fieldNames = schema.getFields().stream()
                .map(Field::getName)
                .toList();

        assertEquals(List.of("entity_type", "last_extracted_at", "updated_at"), fieldNames);
    }

    // =========================================================================
    // Row insertion tests
    // =========================================================================

    @Test
    @DisplayName("insertRows sends correct InsertAllRequest with ingestion_timestamp")
    void insertRows_addsIngestionTimestamp() {
        InsertAllResponse response = mock(InsertAllResponse.class);
        when(response.hasErrors()).thenReturn(false);
        when(response.getInsertErrors()).thenReturn(Map.of());
        when(bigQuery.insertAll(any(InsertAllRequest.class))).thenReturn(response);

        List<Map<String, Object>> rows = List.of(
                Map.of("sha", "abc123", "repo_full_name", "user/repo")
        );

        InsertResult result = loader.insertRows("devpulse_raw", "raw_commits", rows);

        assertEquals(1, result.totalRows());
        assertEquals(1, result.successfulRows());
        assertFalse(result.hasErrors());

        ArgumentCaptor<InsertAllRequest> captor = ArgumentCaptor.forClass(InsertAllRequest.class);
        verify(bigQuery).insertAll(captor.capture());

        InsertAllRequest request = captor.getValue();
        assertEquals(1, request.getRows().size());
        Map<String, Object> insertedRow = request.getRows().get(0).getContent();
        assertTrue(insertedRow.containsKey("ingestion_timestamp"));
        assertNotNull(insertedRow.get("ingestion_timestamp"));
        assertEquals("abc123", insertedRow.get("sha"));
    }

    @Test
    @DisplayName("insertRows returns empty result for null rows")
    void insertRows_nullRows_returnsEmpty() {
        InsertResult result = loader.insertRows("devpulse_raw", "raw_commits", null);

        assertEquals(0, result.totalRows());
        assertEquals(0, result.successfulRows());
        assertFalse(result.hasErrors());
        verify(bigQuery, never()).insertAll(any(InsertAllRequest.class));
    }

    @Test
    @DisplayName("insertRows returns empty result for empty list")
    void insertRows_emptyList_returnsEmpty() {
        InsertResult result = loader.insertRows("devpulse_raw", "raw_commits", List.of());

        assertEquals(0, result.totalRows());
        verify(bigQuery, never()).insertAll(any(InsertAllRequest.class));
    }

    @Test
    @DisplayName("insertRows reports partial failures correctly")
    void insertRows_partialFailure_reportsErrors() {
        BigQueryError bqError = new BigQueryError("invalid", "field1", "Invalid value");
        InsertAllResponse response = mock(InsertAllResponse.class);
        when(response.hasErrors()).thenReturn(true);
        when(response.getInsertErrors()).thenReturn(Map.of(1L, List.of(bqError)));
        when(bigQuery.insertAll(any(InsertAllRequest.class))).thenReturn(response);

        List<Map<String, Object>> rows = List.of(
                Map.of("sha", "abc123"),
                Map.of("sha", "def456"),
                Map.of("sha", "ghi789")
        );

        InsertResult result = loader.insertRows("devpulse_raw", "raw_commits", rows);

        assertEquals(3, result.totalRows());
        assertEquals(2, result.successfulRows());
        assertTrue(result.hasErrors());
        assertEquals(1, result.errors().size());
        assertEquals(1L, result.errors().get(0).rowIndex());
        assertEquals("Invalid value", result.errors().get(0).message());
    }

    @Test
    @DisplayName("insertRows handles multiple errors on multiple rows")
    void insertRows_multipleErrors_reportsAll() {
        BigQueryError error1 = new BigQueryError("invalid", "field1", "Error on row 0");
        BigQueryError error2 = new BigQueryError("invalid", "field2", "Error on row 2");
        InsertAllResponse response = mock(InsertAllResponse.class);
        when(response.hasErrors()).thenReturn(true);
        when(response.getInsertErrors()).thenReturn(Map.of(
                0L, List.of(error1),
                2L, List.of(error2)
        ));
        when(bigQuery.insertAll(any(InsertAllRequest.class))).thenReturn(response);

        List<Map<String, Object>> rows = List.of(
                Map.of("sha", "abc"),
                Map.of("sha", "def"),
                Map.of("sha", "ghi")
        );

        InsertResult result = loader.insertRows("devpulse_raw", "raw_commits", rows);

        assertEquals(3, result.totalRows());
        assertEquals(1, result.successfulRows());
        assertEquals(2, result.errors().size());
    }

    // =========================================================================
    // Entity-specific loader tests
    // =========================================================================

    @Test
    @DisplayName("loadCommits maps Commit DTOs to correct BigQuery rows")
    void loadCommits_mapsFieldsCorrectly() {
        InsertAllResponse response = mock(InsertAllResponse.class);
        when(response.hasErrors()).thenReturn(false);
        when(response.getInsertErrors()).thenReturn(Map.of());
        when(bigQuery.insertAll(any(InsertAllRequest.class))).thenReturn(response);

        Commit commit = new Commit(
                "sha123",
                new Commit.CommitDetail("Initial commit",
                        new Commit.CommitAuthor("John", "john@test.com", "2024-06-15T10:00:00Z")),
                new Commit.GitHubUser("johndoe", 1),
                new Commit.CommitStats(100, 20, 120)
        );

        InsertResult result = loader.loadCommits("user/repo", List.of(commit));

        assertEquals(1, result.totalRows());

        ArgumentCaptor<InsertAllRequest> captor = ArgumentCaptor.forClass(InsertAllRequest.class);
        verify(bigQuery).insertAll(captor.capture());

        Map<String, Object> row = captor.getValue().getRows().get(0).getContent();
        assertEquals("sha123", row.get("sha"));
        assertEquals("user/repo", row.get("repo_full_name"));
        assertEquals("John", row.get("author_name"));
        assertEquals("john@test.com", row.get("author_email"));
        assertEquals("2024-06-15T10:00:00Z", row.get("author_date"));
        assertEquals("Initial commit", row.get("message"));
        assertEquals(100, row.get("additions"));
        assertEquals(20, row.get("deletions"));
        assertEquals(120, row.get("changed_files"));
    }

    @Test
    @DisplayName("loadCommits handles null stats gracefully")
    void loadCommits_nullStats_handledGracefully() {
        InsertAllResponse response = mock(InsertAllResponse.class);
        when(response.hasErrors()).thenReturn(false);
        when(response.getInsertErrors()).thenReturn(Map.of());
        when(bigQuery.insertAll(any(InsertAllRequest.class))).thenReturn(response);

        Commit commit = new Commit("sha123", null, null, null);

        InsertResult result = loader.loadCommits("user/repo", List.of(commit));

        assertEquals(1, result.totalRows());

        ArgumentCaptor<InsertAllRequest> captor = ArgumentCaptor.forClass(InsertAllRequest.class);
        verify(bigQuery).insertAll(captor.capture());

        Map<String, Object> row = captor.getValue().getRows().get(0).getContent();
        assertEquals("sha123", row.get("sha"));
        assertEquals("user/repo", row.get("repo_full_name"));
        assertFalse(row.containsKey("author_name"));
        assertFalse(row.containsKey("additions"));
    }

    @Test
    @DisplayName("loadPullRequests maps PullRequest DTOs to correct BigQuery rows")
    void loadPullRequests_mapsFieldsCorrectly() {
        InsertAllResponse response = mock(InsertAllResponse.class);
        when(response.hasErrors()).thenReturn(false);
        when(response.getInsertErrors()).thenReturn(Map.of());
        when(bigQuery.insertAll(any(InsertAllRequest.class))).thenReturn(response);

        PullRequest pr = new PullRequest(
                42, "Add feature", "closed",
                "2024-06-01T09:00:00Z", "2024-06-02T10:00:00Z",
                "2024-06-02T15:00:00Z", "merge123",
                new PullRequest.User("testuser", 999)
        );

        InsertResult result = loader.loadPullRequests("user/repo", List.of(pr));

        ArgumentCaptor<InsertAllRequest> captor = ArgumentCaptor.forClass(InsertAllRequest.class);
        verify(bigQuery).insertAll(captor.capture());

        Map<String, Object> row = captor.getValue().getRows().get(0).getContent();
        assertEquals(42, row.get("number"));
        assertEquals("user/repo", row.get("repo_full_name"));
        assertEquals("Add feature", row.get("title"));
        assertEquals("closed", row.get("state"));
        assertEquals("testuser", row.get("user_login"));
        assertEquals("2024-06-01T09:00:00Z", row.get("created_at"));
        assertEquals("2024-06-02T10:00:00Z", row.get("updated_at"));
        assertEquals("2024-06-02T15:00:00Z", row.get("merged_at"));
        assertEquals("merge123", row.get("merge_commit_sha"));
    }

    @Test
    @DisplayName("loadReviews maps Review DTOs with pr_number to correct BigQuery rows")
    void loadReviews_mapsFieldsCorrectly() {
        InsertAllResponse response = mock(InsertAllResponse.class);
        when(response.hasErrors()).thenReturn(false);
        when(response.getInsertErrors()).thenReturn(Map.of());
        when(bigQuery.insertAll(any(InsertAllRequest.class))).thenReturn(response);

        Review review = new Review(
                7890, "APPROVED", "2024-06-02T12:00:00Z",
                "Looks good!", new Review.User("reviewer1", 555)
        );

        InsertResult result = loader.loadReviews("user/repo", 42, List.of(review));

        ArgumentCaptor<InsertAllRequest> captor = ArgumentCaptor.forClass(InsertAllRequest.class);
        verify(bigQuery).insertAll(captor.capture());

        Map<String, Object> row = captor.getValue().getRows().get(0).getContent();
        assertEquals(7890L, row.get("id"));
        assertEquals("user/repo", row.get("repo_full_name"));
        assertEquals(42, row.get("pr_number"));
        assertEquals("reviewer1", row.get("user_login"));
        assertEquals("APPROVED", row.get("state"));
        assertEquals("2024-06-02T12:00:00Z", row.get("submitted_at"));
        assertEquals("Looks good!", row.get("body"));
    }

    @Test
    @DisplayName("loadRepositories maps Repository DTOs with owner to correct BigQuery rows")
    void loadRepositories_mapsFieldsCorrectly() {
        InsertAllResponse response = mock(InsertAllResponse.class);
        when(response.hasErrors()).thenReturn(false);
        when(response.getInsertErrors()).thenReturn(Map.of());
        when(bigQuery.insertAll(any(InsertAllRequest.class))).thenReturn(response);

        Repository repo = new Repository(
                12345, "dev-pulse", "testuser/dev-pulse",
                new Repository.Owner("testuser"),
                "Java", "2024-01-15T10:30:00Z", "2024-06-01T08:00:00Z",
                "public", false, 42
        );

        InsertResult result = loader.loadRepositories(List.of(repo));

        ArgumentCaptor<InsertAllRequest> captor = ArgumentCaptor.forClass(InsertAllRequest.class);
        verify(bigQuery).insertAll(captor.capture());

        Map<String, Object> row = captor.getValue().getRows().get(0).getContent();
        assertEquals(12345L, row.get("id"));
        assertEquals("dev-pulse", row.get("name"));
        assertEquals("testuser/dev-pulse", row.get("full_name"));
        assertEquals("testuser", row.get("owner_login"));
        assertEquals("Java", row.get("language"));
        assertEquals("2024-01-15T10:30:00Z", row.get("created_at"));
        assertEquals("2024-06-01T08:00:00Z", row.get("updated_at"));
        assertEquals("public", row.get("visibility"));
        assertEquals(false, row.get("fork"));
        assertEquals(42L, row.get("stargazers_count"));
    }

    @Test
    @DisplayName("loadLanguages flattens language map into individual rows")
    void loadLanguages_flattensLanguageMap() {
        InsertAllResponse response = mock(InsertAllResponse.class);
        when(response.hasErrors()).thenReturn(false);
        when(response.getInsertErrors()).thenReturn(Map.of());
        when(bigQuery.insertAll(any(InsertAllRequest.class))).thenReturn(response);

        Language lang = new Language("user/repo", Map.of(
                "Java", 125000L,
                "Python", 45000L
        ));

        InsertResult result = loader.loadLanguages(List.of(lang));

        ArgumentCaptor<InsertAllRequest> captor = ArgumentCaptor.forClass(InsertAllRequest.class);
        verify(bigQuery).insertAll(captor.capture());

        List<InsertAllRequest.RowToInsert> rows = captor.getValue().getRows();
        assertEquals(2, rows.size());

        Set<String> languageNames = new HashSet<>();
        for (InsertAllRequest.RowToInsert row : rows) {
            assertEquals("user/repo", row.getContent().get("repo_full_name"));
            languageNames.add((String) row.getContent().get("language_name"));
        }
        assertTrue(languageNames.contains("Java"));
        assertTrue(languageNames.contains("Python"));
    }

    @Test
    @DisplayName("loadLanguages handles null language map gracefully")
    void loadLanguages_nullMap_insertsNothing() {
        Language lang = new Language("user/repo", null);

        InsertResult result = loader.loadLanguages(List.of(lang));

        assertEquals(0, result.totalRows());
        verify(bigQuery, never()).insertAll(any(InsertAllRequest.class));
    }

    // =========================================================================
    // Extraction metadata tests
    // =========================================================================

    @Test
    @DisplayName("getLastExtractionTimestamp returns timestamp when metadata exists")
    void getLastExtractionTimestamp_returnsTimestamp() throws Exception {
        Instant expectedTimestamp = Instant.parse("2024-06-15T10:00:00Z");
        long expectedMicros = expectedTimestamp.getEpochSecond() * 1_000_000
                + expectedTimestamp.getNano() / 1_000;

        FieldValue fieldValue = mock(FieldValue.class);
        when(fieldValue.isNull()).thenReturn(false);
        when(fieldValue.getTimestampValue()).thenReturn(expectedMicros);

        FieldValueList row = mock(FieldValueList.class);
        when(row.get("last_extracted_at")).thenReturn(fieldValue);

        TableResult tableResult = mock(TableResult.class);
        when(tableResult.iterateAll()).thenReturn(List.of(row));
        when(bigQuery.query(any(QueryJobConfiguration.class))).thenReturn(tableResult);

        Instant result = loader.getLastExtractionTimestamp("commits");

        assertNotNull(result);
        assertEquals(expectedTimestamp, result);
    }

    @Test
    @DisplayName("getLastExtractionTimestamp returns null when no metadata exists")
    void getLastExtractionTimestamp_returnsNullWhenEmpty() throws Exception {
        TableResult tableResult = mock(TableResult.class);
        when(tableResult.iterateAll()).thenReturn(List.of());
        when(bigQuery.query(any(QueryJobConfiguration.class))).thenReturn(tableResult);

        Instant result = loader.getLastExtractionTimestamp("commits");

        assertNull(result);
    }

    @Test
    @DisplayName("getLastExtractionTimestamp returns null on BigQueryException")
    void getLastExtractionTimestamp_returnsNullOnError() throws Exception {
        when(bigQuery.query(any(QueryJobConfiguration.class)))
                .thenThrow(new BigQueryException(404, "Table not found"));

        Instant result = loader.getLastExtractionTimestamp("commits");

        assertNull(result);
    }

    @Test
    @DisplayName("updateLastExtractionTimestamp executes MERGE query with correct parameters")
    void updateLastExtractionTimestamp_executesMergeQuery() throws Exception {
        TableResult tableResult = mock(TableResult.class);
        when(bigQuery.query(any(QueryJobConfiguration.class))).thenReturn(tableResult);

        Instant timestamp = Instant.parse("2024-06-15T10:00:00Z");
        loader.updateLastExtractionTimestamp("commits", timestamp);

        ArgumentCaptor<QueryJobConfiguration> captor = ArgumentCaptor.forClass(QueryJobConfiguration.class);
        verify(bigQuery).query(captor.capture());

        QueryJobConfiguration config = captor.getValue();
        String query = config.getQuery();
        assertTrue(query.contains("MERGE"));
        assertTrue(query.contains("_extraction_metadata"));
        assertTrue(query.contains("entity_type"));
        assertFalse(config.useLegacySql());
    }

    @Test
    @DisplayName("updateLastExtractionTimestamp throws BigQueryException on failure")
    void updateLastExtractionTimestamp_throwsOnError() throws Exception {
        when(bigQuery.query(any(QueryJobConfiguration.class)))
                .thenThrow(new BigQueryException(500, "Internal error"));

        assertThrows(BigQueryException.class, () ->
                loader.updateLastExtractionTimestamp("commits", Instant.now()));
    }

    // =========================================================================
    // InsertResult record tests
    // =========================================================================

    @Test
    @DisplayName("InsertResult.hasErrors returns false when no errors")
    void insertResult_hasErrors_false() {
        InsertResult result = new InsertResult(5, 5, List.of());
        assertFalse(result.hasErrors());
    }

    @Test
    @DisplayName("InsertResult.hasErrors returns true when errors exist")
    void insertResult_hasErrors_true() {
        InsertResult result = new InsertResult(5, 3,
                List.of(new InsertResult.RowError(1, "Error")));
        assertTrue(result.hasErrors());
    }

    @Test
    @DisplayName("InsertResult.hasErrors handles null errors list")
    void insertResult_hasErrors_null() {
        InsertResult result = new InsertResult(0, 0, null);
        assertFalse(result.hasErrors());
    }
}
