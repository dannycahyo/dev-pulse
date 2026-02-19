package com.devpulse.extractor.orchestrator;

import com.devpulse.extractor.client.GitHubApiClient;
import com.devpulse.extractor.loader.BigQueryLoader;
import com.devpulse.extractor.loader.InsertResult;
import com.devpulse.extractor.model.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * End-to-end test for the extraction orchestrator. Mocks the GitHub API client
 * responses and verifies that the correct BigQuery loader methods are called
 * with the expected data.
 */
@ExtendWith(MockitoExtension.class)
class ExtractionOrchestratorTest {

    @Mock
    private GitHubApiClient gitHubClient;

    @Mock
    private BigQueryLoader loader;

    private static final InsertResult SUCCESS_RESULT =
            new InsertResult(1, 1, List.of());

    // =========================================================================
    // End-to-end full extraction test
    // =========================================================================

    @Test
    @DisplayName("Full extraction fetches repos, commits, PRs, reviews, languages and calls loader correctly")
    void fullExtraction_endToEnd() throws Exception {
        // Set up mock GitHub responses
        Repository repo1 = new Repository(1L, "test-repo", "testuser/test-repo",
                new Repository.Owner("testuser"), "Java",
                "2024-01-01T00:00:00Z", "2024-06-01T00:00:00Z",
                "public", false, 42);
        Repository repo2 = new Repository(2L, "other-repo", "testuser/other-repo",
                new Repository.Owner("testuser"), "Python",
                "2024-03-01T00:00:00Z", "2024-07-01T00:00:00Z",
                "private", false, 10);
        when(gitHubClient.getRepositories()).thenReturn(List.of(repo1, repo2));

        // Commits for repo1
        Commit commit1 = new Commit("abc123",
                new Commit.CommitDetail("Initial commit",
                        new Commit.CommitAuthor("Test User", "test@example.com",
                                "2024-05-15T10:30:00Z")),
                new Commit.GitHubUser("testuser", 1),
                new Commit.CommitStats(100, 20, 5));
        Commit commit2 = new Commit("def456",
                new Commit.CommitDetail("Add feature",
                        new Commit.CommitAuthor("Test User", "test@example.com",
                                "2024-05-16T14:00:00Z")),
                new Commit.GitHubUser("testuser", 1),
                new Commit.CommitStats(50, 10, 3));
        when(gitHubClient.getCommits("testuser/test-repo")).thenReturn(List.of(commit1, commit2));
        when(gitHubClient.getCommits("testuser/other-repo")).thenReturn(List.of());

        // PRs for repo1
        PullRequest pr1 = new PullRequest(1, "Add feature X", "closed",
                "2024-05-10T08:00:00Z", "2024-05-12T16:00:00Z",
                "2024-05-12T16:00:00Z", "merge123",
                new PullRequest.User("testuser", 1));
        when(gitHubClient.getPullRequests("testuser/test-repo")).thenReturn(List.of(pr1));
        when(gitHubClient.getPullRequests("testuser/other-repo")).thenReturn(List.of());

        // Reviews for PR#1
        Review review1 = new Review(100L, "APPROVED", "2024-05-11T12:00:00Z",
                "Looks good!", new Review.User("reviewer1", 2));
        when(gitHubClient.getReviews("testuser/test-repo", 1)).thenReturn(List.of(review1));

        // Languages
        Language lang1 = new Language("testuser/test-repo",
                Map.of("Java", 50000L, "Kotlin", 10000L));
        Language lang2 = new Language("testuser/other-repo",
                Map.of("Python", 30000L));
        when(gitHubClient.getLanguages("testuser/test-repo")).thenReturn(lang1);
        when(gitHubClient.getLanguages("testuser/other-repo")).thenReturn(lang2);

        // Mock loader responses
        when(loader.loadRepositories(anyList()))
                .thenReturn(new InsertResult(2, 2, List.of()));
        when(loader.loadCommits(eq("testuser/test-repo"), anyList()))
                .thenReturn(new InsertResult(2, 2, List.of()));
        when(loader.loadPullRequests(eq("testuser/test-repo"), anyList()))
                .thenReturn(new InsertResult(1, 1, List.of()));
        when(loader.loadReviews(eq("testuser/test-repo"), eq(1), anyList()))
                .thenReturn(new InsertResult(1, 1, List.of()));
        when(loader.loadLanguages(anyList()))
                .thenReturn(new InsertResult(3, 3, List.of()))  // repo1: Java, Kotlin
                .thenReturn(new InsertResult(1, 1, List.of())); // repo2: Python

        // Run orchestrator
        ExtractionOrchestrator orchestrator = new ExtractionOrchestrator(gitHubClient, loader);
        ExtractionSummary summary = orchestrator.run(true);

        // Verify no failures
        assertFalse(summary.hasFailures(),
                "Expected no failures but got: " + summary.results().stream()
                        .filter(r -> !r.success())
                        .map(r -> r.entityType() + "[" + r.repoFullName() + "]: " + r.errorMessage())
                        .toList());

        // Verify infrastructure setup
        verify(loader).ensureDatasetsExist();
        verify(loader).ensureTablesExist();

        // Verify repositories loaded
        ArgumentCaptor<List<Repository>> repoCaptor = ArgumentCaptor.forClass(List.class);
        verify(loader).loadRepositories(repoCaptor.capture());
        assertEquals(2, repoCaptor.getValue().size());

        // Verify commits loaded for repo1 with both commits
        ArgumentCaptor<List<Commit>> commitCaptor = ArgumentCaptor.forClass(List.class);
        verify(loader).loadCommits(eq("testuser/test-repo"), commitCaptor.capture());
        assertEquals(2, commitCaptor.getValue().size());
        assertEquals("abc123", commitCaptor.getValue().get(0).sha());
        assertEquals("def456", commitCaptor.getValue().get(1).sha());

        // Verify PRs loaded for repo1
        ArgumentCaptor<List<PullRequest>> prCaptor = ArgumentCaptor.forClass(List.class);
        verify(loader).loadPullRequests(eq("testuser/test-repo"), prCaptor.capture());
        assertEquals(1, prCaptor.getValue().size());
        assertEquals("Add feature X", prCaptor.getValue().get(0).title());

        // Verify reviews loaded
        ArgumentCaptor<List<Review>> reviewCaptor = ArgumentCaptor.forClass(List.class);
        verify(loader).loadReviews(eq("testuser/test-repo"), eq(1), reviewCaptor.capture());
        assertEquals(1, reviewCaptor.getValue().size());
        assertEquals("APPROVED", reviewCaptor.getValue().get(0).state());

        // Verify languages loaded for both repos
        verify(loader, times(2)).loadLanguages(anyList());

        // Verify metadata was updated for all entity types (full mode, all succeed)
        verify(loader).updateLastExtractionTimestamp(eq("repositories"), any(Instant.class));
        verify(loader).updateLastExtractionTimestamp(eq("commits"), any(Instant.class));
        verify(loader).updateLastExtractionTimestamp(eq("pull_requests"), any(Instant.class));
        verify(loader).updateLastExtractionTimestamp(eq("reviews"), any(Instant.class));
        verify(loader).updateLastExtractionTimestamp(eq("languages"), any(Instant.class));

        // Verify summary counts
        assertEquals(2, summary.totalExtractedForEntity("repositories"));
        assertEquals(2, summary.totalExtractedForEntity("commits"));
        assertEquals(1, summary.totalExtractedForEntity("pull_requests"));
        assertEquals(1, summary.totalExtractedForEntity("reviews"));
    }

    // =========================================================================
    // Incremental extraction test
    // =========================================================================

    @Test
    @DisplayName("Incremental extraction reads metadata and filters old records")
    void incrementalExtraction_filtersOldRecords() throws Exception {
        // Mock metadata — return a timestamp for commits and PRs
        Instant lastRun = Instant.parse("2024-05-15T12:00:00Z");
        when(loader.getLastExtractionTimestamp("commits")).thenReturn(lastRun);
        when(loader.getLastExtractionTimestamp("pull_requests")).thenReturn(lastRun);

        // Set up single repo
        Repository repo = new Repository(1L, "test-repo", "testuser/test-repo",
                new Repository.Owner("testuser"), "Java",
                "2024-01-01T00:00:00Z", "2024-06-01T00:00:00Z",
                "public", false, 5);
        when(gitHubClient.getRepositories()).thenReturn(List.of(repo));

        // Commits: one old (before lastRun), one new (after lastRun)
        Commit oldCommit = new Commit("old111",
                new Commit.CommitDetail("Old commit",
                        new Commit.CommitAuthor("Test", "t@t.com", "2024-05-14T10:00:00Z")),
                null, null);
        Commit newCommit = new Commit("new222",
                new Commit.CommitDetail("New commit",
                        new Commit.CommitAuthor("Test", "t@t.com", "2024-05-16T10:00:00Z")),
                null, null);
        when(gitHubClient.getCommits("testuser/test-repo")).thenReturn(List.of(oldCommit, newCommit));

        // PRs: one old, one new
        PullRequest oldPr = new PullRequest(1, "Old PR", "closed",
                "2024-05-10T00:00:00Z", "2024-05-14T00:00:00Z",
                "2024-05-14T00:00:00Z", "m1", new PullRequest.User("testuser", 1));
        PullRequest newPr = new PullRequest(2, "New PR", "open",
                "2024-05-16T00:00:00Z", "2024-05-16T00:00:00Z",
                null, null, new PullRequest.User("testuser", 1));
        when(gitHubClient.getPullRequests("testuser/test-repo")).thenReturn(List.of(oldPr, newPr));

        // Reviews for the new PR only (old PR is filtered out)
        when(gitHubClient.getReviews("testuser/test-repo", 2)).thenReturn(List.of());

        // Languages
        when(gitHubClient.getLanguages("testuser/test-repo"))
                .thenReturn(new Language("testuser/test-repo", Map.of("Java", 1000L)));

        // Mock loader responses
        when(loader.loadRepositories(anyList())).thenReturn(SUCCESS_RESULT);
        when(loader.loadCommits(anyString(), anyList())).thenReturn(SUCCESS_RESULT);
        when(loader.loadPullRequests(anyString(), anyList())).thenReturn(SUCCESS_RESULT);
        when(loader.loadLanguages(anyList())).thenReturn(SUCCESS_RESULT);

        ExtractionOrchestrator orchestrator = new ExtractionOrchestrator(gitHubClient, loader);
        ExtractionSummary summary = orchestrator.run(false); // incremental

        assertFalse(summary.hasFailures());

        // Verify commit load received only the new commit (old one filtered out)
        ArgumentCaptor<List<Commit>> commitCaptor = ArgumentCaptor.forClass(List.class);
        verify(loader).loadCommits(eq("testuser/test-repo"), commitCaptor.capture());
        assertEquals(1, commitCaptor.getValue().size());
        assertEquals("new222", commitCaptor.getValue().get(0).sha());

        // Verify PR load received only the new PR (old one filtered out)
        ArgumentCaptor<List<PullRequest>> prCaptor = ArgumentCaptor.forClass(List.class);
        verify(loader).loadPullRequests(eq("testuser/test-repo"), prCaptor.capture());
        assertEquals(1, prCaptor.getValue().size());
        assertEquals(2, prCaptor.getValue().get(0).number());

        // Reviews should only be fetched for the new PR (which was the only one that passed the filter)
        verify(gitHubClient).getReviews("testuser/test-repo", 2);
        verify(gitHubClient, never()).getReviews("testuser/test-repo", 1);
    }

    // =========================================================================
    // Error handling test
    // =========================================================================

    @Test
    @DisplayName("Orchestrator continues on partial failure and reports in summary")
    void partialFailure_continuesAndReportsErrors() throws Exception {
        Repository repo = new Repository(1L, "test-repo", "testuser/test-repo",
                new Repository.Owner("testuser"), "Java",
                "2024-01-01T00:00:00Z", "2024-06-01T00:00:00Z",
                "public", false, 5);
        when(gitHubClient.getRepositories()).thenReturn(List.of(repo));

        // Commits succeed (empty list)
        when(gitHubClient.getCommits("testuser/test-repo")).thenReturn(List.of());

        // PRs throw an exception
        when(gitHubClient.getPullRequests("testuser/test-repo"))
                .thenThrow(new IOException("API rate limit exceeded"));

        // Languages succeed
        when(gitHubClient.getLanguages("testuser/test-repo"))
                .thenReturn(new Language("testuser/test-repo", Map.of("Java", 5000L)));

        // Mock loader responses
        when(loader.loadRepositories(anyList())).thenReturn(SUCCESS_RESULT);
        when(loader.loadLanguages(anyList())).thenReturn(SUCCESS_RESULT);

        ExtractionOrchestrator orchestrator = new ExtractionOrchestrator(gitHubClient, loader);
        ExtractionSummary summary = orchestrator.run(true);

        // Should have failures
        assertTrue(summary.hasFailures());

        // PR extraction should have failed
        long prFailures = summary.results().stream()
                .filter(r -> r.entityType().equals("pull_requests") && !r.success())
                .count();
        assertEquals(1, prFailures);

        // Verify error message captured
        ExtractionResult prFailure = summary.results().stream()
                .filter(r -> r.entityType().equals("pull_requests") && !r.success())
                .findFirst().orElseThrow();
        assertTrue(prFailure.errorMessage().contains("API rate limit exceeded"));

        // Repositories should have succeeded
        long repoSuccesses = summary.results().stream()
                .filter(r -> r.entityType().equals("repositories") && r.success())
                .count();
        assertEquals(1, repoSuccesses);

        // Languages should have succeeded (continues past PR failure)
        long langSuccesses = summary.results().stream()
                .filter(r -> r.entityType().equals("languages") && r.success())
                .count();
        assertEquals(1, langSuccesses);

        // Reviews should still get a result (with empty PR list since PR extraction failed)
        long reviewResults = summary.results().stream()
                .filter(r -> r.entityType().equals("reviews"))
                .count();
        assertEquals(1, reviewResults);

        // Metadata should NOT be updated for pull_requests (had failure)
        verify(loader, never()).updateLastExtractionTimestamp(eq("pull_requests"), any());
    }

    // =========================================================================
    // Infrastructure failure test
    // =========================================================================

    @Test
    @DisplayName("Orchestrator handles infrastructure setup failure gracefully")
    void infrastructureFailure_returnsErrorSummary() {
        doThrow(new RuntimeException("Service unavailable"))
                .when(loader).ensureDatasetsExist();

        ExtractionOrchestrator orchestrator = new ExtractionOrchestrator(gitHubClient, loader);
        ExtractionSummary summary = orchestrator.run(true);

        assertTrue(summary.hasFailures());
        assertEquals(1, summary.failureCount());
        assertEquals("infrastructure", summary.results().get(0).entityType());

        // No GitHub API calls or loader calls should be made
        verifyNoInteractions(gitHubClient);
        verify(loader, never()).loadRepositories(anyList());
    }

    // =========================================================================
    // Empty repository list test
    // =========================================================================

    @Test
    @DisplayName("Orchestrator handles empty repository list gracefully")
    void emptyRepositories_noExtractions() throws Exception {
        when(gitHubClient.getRepositories()).thenReturn(List.of());

        ExtractionOrchestrator orchestrator = new ExtractionOrchestrator(gitHubClient, loader);
        ExtractionSummary summary = orchestrator.run(true);

        assertFalse(summary.hasFailures());

        // Only repositories result should exist — no per-repo extractions
        assertEquals(1, summary.results().size());
        assertEquals("repositories", summary.results().get(0).entityType());

        // No commits, PRs, reviews, or languages should be fetched
        verify(gitHubClient, never()).getCommits(anyString());
        verify(gitHubClient, never()).getPullRequests(anyString());
        verify(gitHubClient, never()).getReviews(anyString(), anyInt());
        verify(gitHubClient, never()).getLanguages(anyString());
    }

    // =========================================================================
    // Multiple repos with mixed success/failure test
    // =========================================================================

    @Test
    @DisplayName("Mixed success across repos: commits fail for repo1, succeed for repo2")
    void mixedRepoResults_partialSuccess() throws Exception {
        Repository repo1 = new Repository(1L, "repo1", "user/repo1",
                new Repository.Owner("user"), "Java",
                "2024-01-01T00:00:00Z", "2024-06-01T00:00:00Z",
                "public", false, 0);
        Repository repo2 = new Repository(2L, "repo2", "user/repo2",
                new Repository.Owner("user"), "Python",
                "2024-01-01T00:00:00Z", "2024-06-01T00:00:00Z",
                "public", false, 0);
        when(gitHubClient.getRepositories()).thenReturn(List.of(repo1, repo2));

        // repo1 commits fail
        when(gitHubClient.getCommits("user/repo1"))
                .thenThrow(new IOException("404 Not Found"));
        // repo2 commits succeed
        Commit c = new Commit("sha1",
                new Commit.CommitDetail("msg",
                        new Commit.CommitAuthor("U", "u@u.com", "2024-06-01T00:00:00Z")),
                null, null);
        when(gitHubClient.getCommits("user/repo2")).thenReturn(List.of(c));

        when(gitHubClient.getPullRequests(anyString())).thenReturn(List.of());
        when(gitHubClient.getLanguages(anyString()))
                .thenReturn(new Language("n/a", Map.of()));

        when(loader.loadRepositories(anyList())).thenReturn(new InsertResult(2, 2, List.of()));
        when(loader.loadCommits(anyString(), anyList())).thenReturn(SUCCESS_RESULT);

        ExtractionOrchestrator orchestrator = new ExtractionOrchestrator(gitHubClient, loader);
        ExtractionSummary summary = orchestrator.run(true);

        assertTrue(summary.hasFailures());

        // repo1 commits failed
        long commitFailures = summary.results().stream()
                .filter(r -> r.entityType().equals("commits") && !r.success()
                        && r.repoFullName().equals("user/repo1"))
                .count();
        assertEquals(1, commitFailures);

        // repo2 commits succeeded
        long commitSuccesses = summary.results().stream()
                .filter(r -> r.entityType().equals("commits") && r.success()
                        && r.repoFullName().equals("user/repo2"))
                .count();
        assertEquals(1, commitSuccesses);

        // Commits metadata should NOT be updated (one repo failed)
        verify(loader, never()).updateLastExtractionTimestamp(eq("commits"), any());
    }

    // =========================================================================
    // ExtractorApp CLI argument parsing tests
    // =========================================================================

    @Test
    @DisplayName("parseFullMode returns true for --full flag")
    void parseFullMode_withFullFlag() {
        assertTrue(ExtractorApp.parseFullMode(new String[]{"--full"}));
    }

    @Test
    @DisplayName("parseFullMode returns false for --incremental flag")
    void parseFullMode_withIncrementalFlag() {
        assertFalse(ExtractorApp.parseFullMode(new String[]{"--incremental"}));
    }

    @Test
    @DisplayName("parseFullMode returns false when no args provided")
    void parseFullMode_noArgs() {
        assertFalse(ExtractorApp.parseFullMode(new String[]{}));
    }

    @Test
    @DisplayName("parseFullMode detects --full among multiple args")
    void parseFullMode_mixedArgs() {
        assertTrue(ExtractorApp.parseFullMode(new String[]{"--verbose", "--full", "--dry-run"}));
    }

    // =========================================================================
    // ExtractionSummary tests
    // =========================================================================

    @Test
    @DisplayName("ExtractionSummary correctly computes aggregate counts")
    void extractionSummary_aggregates() {
        List<ExtractionResult> results = List.of(
                ExtractionResult.success("commits", "repo1", 10, 10, 100),
                ExtractionResult.success("commits", "repo2", 5, 5, 50),
                ExtractionResult.failure("pull_requests", "repo1", "error", 20),
                ExtractionResult.success("pull_requests", "repo2", 3, 3, 30)
        );

        ExtractionSummary summary = new ExtractionSummary(results, 500);

        assertEquals(3, summary.successCount());
        assertEquals(1, summary.failureCount());
        assertTrue(summary.hasFailures());
        assertEquals(15, summary.totalExtractedForEntity("commits"));
        assertEquals(15, summary.totalLoadedForEntity("commits"));
        assertEquals(3, summary.totalExtractedForEntity("pull_requests"));
        assertEquals(0, summary.totalExtractedForEntity("nonexistent"));
    }

    @Test
    @DisplayName("ExtractionSummary with no failures")
    void extractionSummary_noFailures() {
        List<ExtractionResult> results = List.of(
                ExtractionResult.success("commits", "repo1", 5, 5, 50)
        );
        ExtractionSummary summary = new ExtractionSummary(results, 100);

        assertFalse(summary.hasFailures());
        assertEquals(0, summary.failureCount());
        assertEquals(1, summary.successCount());
    }

    // =========================================================================
    // ExtractionResult tests
    // =========================================================================

    @Test
    @DisplayName("ExtractionResult.success creates successful result")
    void extractionResult_success() {
        ExtractionResult result = ExtractionResult.success("commits", "repo1", 10, 8, 100);
        assertTrue(result.success());
        assertNull(result.errorMessage());
        assertEquals(10, result.recordsExtracted());
        assertEquals(8, result.recordsLoaded());
        assertEquals(100, result.durationMs());
    }

    @Test
    @DisplayName("ExtractionResult.failure creates failed result")
    void extractionResult_failure() {
        ExtractionResult result = ExtractionResult.failure("commits", "repo1", "timeout", 50);
        assertFalse(result.success());
        assertEquals("timeout", result.errorMessage());
        assertEquals(0, result.recordsExtracted());
        assertEquals(0, result.recordsLoaded());
    }
}
