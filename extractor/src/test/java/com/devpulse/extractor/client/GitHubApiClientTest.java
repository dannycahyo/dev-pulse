package com.devpulse.extractor.client;

import com.devpulse.extractor.model.*;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link GitHubApiClient} covering pagination parsing,
 * rate limit detection, retry logic, and DTO deserialization.
 */
class GitHubApiClientTest {

    private MockWebServer server;
    private GitHubApiClient client;

    @BeforeEach
    void setUp() throws IOException {
        server = new MockWebServer();
        server.start();

        OkHttpClient httpClient = new OkHttpClient.Builder()
                .connectTimeout(5, TimeUnit.SECONDS)
                .readTimeout(5, TimeUnit.SECONDS)
                .build();

        client = new GitHubApiClient("test-token", "testuser", httpClient);
    }

    @AfterEach
    void tearDown() throws IOException {
        server.shutdown();
    }

    // =========================================================================
    // Pagination parsing tests
    // =========================================================================

    @Test
    @DisplayName("parseNextPageUrl extracts next URL from Link header")
    void parseNextPageUrl_withNextAndLast() {
        String linkHeader = "<https://api.github.com/user/repos?page=2&per_page=100>; rel=\"next\", "
                + "<https://api.github.com/user/repos?page=5&per_page=100>; rel=\"last\"";

        String nextUrl = GitHubApiClient.parseNextPageUrl(linkHeader);

        assertEquals("https://api.github.com/user/repos?page=2&per_page=100", nextUrl);
    }

    @Test
    @DisplayName("parseNextPageUrl returns null when no next link present")
    void parseNextPageUrl_noNext() {
        String linkHeader = "<https://api.github.com/user/repos?page=1&per_page=100>; rel=\"prev\", "
                + "<https://api.github.com/user/repos?page=5&per_page=100>; rel=\"last\"";

        String nextUrl = GitHubApiClient.parseNextPageUrl(linkHeader);

        assertNull(nextUrl);
    }

    @Test
    @DisplayName("parseNextPageUrl returns null for null header")
    void parseNextPageUrl_null() {
        assertNull(GitHubApiClient.parseNextPageUrl(null));
    }

    @Test
    @DisplayName("parseNextPageUrl returns null for empty header")
    void parseNextPageUrl_empty() {
        assertNull(GitHubApiClient.parseNextPageUrl(""));
    }

    @Test
    @DisplayName("parseNextPageUrl handles next link only (no last)")
    void parseNextPageUrl_nextOnly() {
        String linkHeader = "<https://api.github.com/repos/user/repo/commits?page=3>; rel=\"next\"";

        String nextUrl = GitHubApiClient.parseNextPageUrl(linkHeader);

        assertEquals("https://api.github.com/repos/user/repo/commits?page=3", nextUrl);
    }

    // =========================================================================
    // Rate limit detection tests
    // =========================================================================

    @Test
    @DisplayName("handleRateLimitPause does nothing when remaining is above threshold")
    void handleRateLimitPause_aboveThreshold() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("X-RateLimit-Remaining", "4500")
                .setHeader("X-RateLimit-Reset", String.valueOf(System.currentTimeMillis() / 1000 + 3600))
                .setBody("[]"));

        var request = client.buildRequest(server.url("/test").toString(), null, null);
        var response = new OkHttpClient().newCall(request).execute();

        long start = System.currentTimeMillis();
        client.handleRateLimitPause(response);
        long elapsed = System.currentTimeMillis() - start;

        // Should not have paused — elapsed should be well under 1 second
        assertTrue(elapsed < 1000, "Should not pause when rate limit remaining is above threshold");
        response.close();
    }

    @Test
    @DisplayName("handleRateLimitPause skips when headers are missing")
    void handleRateLimitPause_missingHeaders() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setBody("[]"));

        var request = client.buildRequest(server.url("/test").toString(), null, null);
        var response = new OkHttpClient().newCall(request).execute();

        // Should not throw or block
        assertDoesNotThrow(() -> client.handleRateLimitPause(response));
        response.close();
    }

    // =========================================================================
    // Retry logic tests
    // =========================================================================

    @Test
    @DisplayName("Retries on 429 with exponential backoff and succeeds")
    void retryOn429_thenSuccess() throws Exception {
        // First request returns 429
        server.enqueue(new MockResponse()
                .setResponseCode(429)
                .setHeader("X-RateLimit-Remaining", "0")
                .setHeader("X-RateLimit-Reset", String.valueOf(System.currentTimeMillis() / 1000 + 60))
                .setBody("rate limited"));

        // Second request succeeds
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("X-RateLimit-Remaining", "4999")
                .setBody("[{\"id\": 1, \"name\": \"repo1\", \"full_name\": \"user/repo1\", "
                        + "\"language\": \"Java\", \"created_at\": \"2024-01-01T00:00:00Z\", "
                        + "\"visibility\": \"public\"}]"));

        var request = client.buildRequest(server.url("/user/repos").toString(), null, null);
        var result = client.executePageWithRetry(request);

        assertNotNull(result.body());
        assertEquals(2, server.getRequestCount());
    }

    @Test
    @DisplayName("Retries on 503 and succeeds on second attempt")
    void retryOn503_thenSuccess() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(503).setBody("unavailable"));
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("X-RateLimit-Remaining", "4000")
                .setBody("[]"));

        var request = client.buildRequest(server.url("/test").toString(), null, null);
        var result = client.executePageWithRetry(request);

        assertNotNull(result.body());
        assertEquals("[]", result.body());
        assertEquals(2, server.getRequestCount());
    }

    @Test
    @DisplayName("Uses Retry-After header when present on 429")
    void retryAfterHeader_usedForWaitTime() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(429)
                .setHeader("Retry-After", "1")
                .setBody("rate limited"));
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("X-RateLimit-Remaining", "4999")
                .setBody("[]"));

        var request = client.buildRequest(server.url("/test").toString(), null, null);
        var result = client.executePageWithRetry(request);

        assertNotNull(result.body());
        assertEquals(2, server.getRequestCount());
    }

    @Test
    @DisplayName("Throws IOException after exhausting retries on persistent 429")
    void exhaustedRetries_throwsIOException() {
        // Enqueue enough 429s to exhaust all retries
        for (int i = 0; i <= 7; i++) {
            server.enqueue(new MockResponse()
                    .setResponseCode(429)
                    .setHeader("Retry-After", "0")
                    .setBody("rate limited"));
        }

        var request = client.buildRequest(server.url("/test").toString(), null, null);

        assertThrows(IOException.class, () -> client.executePageWithRetry(request));
    }

    @Test
    @DisplayName("Returns null body on 304 Not Modified")
    void notModified_returnsNullBody() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(304)
                .setHeader("X-RateLimit-Remaining", "4000"));

        var request = client.buildRequest(server.url("/test").toString(), "etag-value", null);
        var result = client.executePageWithRetry(request);

        assertNull(result.body());
        assertNull(result.nextUrl());
    }

    @Test
    @DisplayName("Throws IOException on non-retryable error status")
    void nonRetryableError_throwsIOException() {
        server.enqueue(new MockResponse()
                .setResponseCode(404)
                .setHeader("X-RateLimit-Remaining", "4000")
                .setBody("not found"));

        var request = client.buildRequest(server.url("/test").toString(), null, null);

        assertThrows(IOException.class, () -> client.executePageWithRetry(request));
    }

    // =========================================================================
    // Request building tests
    // =========================================================================

    @Test
    @DisplayName("Builds request with Bearer token authentication")
    void buildRequest_hasBearerToken() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("X-RateLimit-Remaining", "5000")
                .setBody("[]"));

        var request = client.buildRequest(server.url("/test").toString(), null, null);
        new OkHttpClient().newCall(request).execute().close();

        RecordedRequest recorded = server.takeRequest();
        assertEquals("Bearer test-token", recorded.getHeader("Authorization"));
        assertEquals("application/vnd.github+json", recorded.getHeader("Accept"));
        assertEquals("2022-11-28", recorded.getHeader("X-GitHub-Api-Version"));
    }

    @Test
    @DisplayName("Builds request with ETag conditional header")
    void buildRequest_withEtag() {
        var request = client.buildRequest("https://api.github.com/test", "W/\"abc123\"", null);

        assertEquals("W/\"abc123\"", request.header("If-None-Match"));
        assertNull(request.header("If-Modified-Since"));
    }

    @Test
    @DisplayName("Builds request with If-Modified-Since conditional header")
    void buildRequest_withIfModifiedSince() {
        var request = client.buildRequest(
                "https://api.github.com/test", null, "Mon, 01 Jan 2024 00:00:00 GMT");

        assertNull(request.header("If-None-Match"));
        assertEquals("Mon, 01 Jan 2024 00:00:00 GMT", request.header("If-Modified-Since"));
    }

    // =========================================================================
    // DTO deserialization tests
    // =========================================================================

    @Test
    @DisplayName("Deserializes Repository JSON correctly")
    void deserializeRepository() throws Exception {
        String json = """
                [{
                    "id": 12345,
                    "name": "dev-pulse",
                    "full_name": "testuser/dev-pulse",
                    "language": "Java",
                    "created_at": "2024-01-15T10:30:00Z",
                    "visibility": "public",
                    "extra_field": "ignored"
                }]""";

        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("X-RateLimit-Remaining", "4999")
                .setBody(json));

        var request = client.buildRequest(server.url("/user/repos").toString(), null, null);
        var result = client.executePageWithRetry(request);
        List<Repository> repos = new com.fasterxml.jackson.databind.ObjectMapper()
                .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .readValue(result.body(), new com.fasterxml.jackson.core.type.TypeReference<List<Repository>>() {});

        assertEquals(1, repos.size());
        Repository repo = repos.get(0);
        assertEquals(12345, repo.id());
        assertEquals("dev-pulse", repo.name());
        assertEquals("testuser/dev-pulse", repo.fullName());
        assertEquals("Java", repo.language());
        assertEquals("2024-01-15T10:30:00Z", repo.createdAt());
        assertEquals("public", repo.visibility());
    }

    @Test
    @DisplayName("Deserializes Commit JSON with nested objects")
    void deserializeCommit() throws Exception {
        String json = """
                [{
                    "sha": "abc123def456",
                    "commit": {
                        "message": "feat: add extraction layer",
                        "author": {
                            "name": "Test User",
                            "email": "test@example.com",
                            "date": "2024-06-15T14:30:00Z"
                        }
                    },
                    "author": {
                        "login": "testuser",
                        "id": 999
                    },
                    "stats": {
                        "additions": 150,
                        "deletions": 30,
                        "total": 180
                    }
                }]""";

        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("X-RateLimit-Remaining", "4998")
                .setBody(json));

        var request = client.buildRequest(server.url("/repos/user/repo/commits").toString(), null, null);
        var result = client.executePageWithRetry(request);
        List<Commit> commits = new com.fasterxml.jackson.databind.ObjectMapper()
                .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .readValue(result.body(), new com.fasterxml.jackson.core.type.TypeReference<List<Commit>>() {});

        assertEquals(1, commits.size());
        Commit commit = commits.get(0);
        assertEquals("abc123def456", commit.sha());
        assertEquals("feat: add extraction layer", commit.commit().message());
        assertEquals("Test User", commit.commit().author().name());
        assertEquals("test@example.com", commit.commit().author().email());
        assertEquals("2024-06-15T14:30:00Z", commit.commit().author().date());
        assertEquals("testuser", commit.author().login());
        assertEquals(999, commit.author().id());
        assertEquals(150, commit.stats().additions());
        assertEquals(30, commit.stats().deletions());
        assertEquals(180, commit.stats().total());
    }

    @Test
    @DisplayName("Deserializes PullRequest JSON correctly")
    void deserializePullRequest() throws Exception {
        String json = """
                [{
                    "number": 42,
                    "title": "Add GitHub API client",
                    "state": "closed",
                    "created_at": "2024-06-01T09:00:00Z",
                    "merged_at": "2024-06-02T15:00:00Z",
                    "user": {
                        "login": "testuser",
                        "id": 999
                    }
                }]""";

        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("X-RateLimit-Remaining", "4997")
                .setBody(json));

        var request = client.buildRequest(server.url("/repos/user/repo/pulls").toString(), null, null);
        var result = client.executePageWithRetry(request);
        List<PullRequest> prs = new com.fasterxml.jackson.databind.ObjectMapper()
                .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .readValue(result.body(), new com.fasterxml.jackson.core.type.TypeReference<List<PullRequest>>() {});

        assertEquals(1, prs.size());
        PullRequest pr = prs.get(0);
        assertEquals(42, pr.number());
        assertEquals("Add GitHub API client", pr.title());
        assertEquals("closed", pr.state());
        assertEquals("2024-06-01T09:00:00Z", pr.createdAt());
        assertEquals("2024-06-02T15:00:00Z", pr.mergedAt());
        assertEquals("testuser", pr.user().login());
    }

    @Test
    @DisplayName("Deserializes Review JSON correctly")
    void deserializeReview() throws Exception {
        String json = """
                [{
                    "id": 7890,
                    "state": "APPROVED",
                    "submitted_at": "2024-06-02T12:00:00Z",
                    "user": {
                        "login": "reviewer1",
                        "id": 555
                    }
                }]""";

        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("X-RateLimit-Remaining", "4996")
                .setBody(json));

        var request = client.buildRequest(server.url("/repos/user/repo/pulls/42/reviews").toString(), null, null);
        var result = client.executePageWithRetry(request);
        List<Review> reviews = new com.fasterxml.jackson.databind.ObjectMapper()
                .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .readValue(result.body(), new com.fasterxml.jackson.core.type.TypeReference<List<Review>>() {});

        assertEquals(1, reviews.size());
        Review review = reviews.get(0);
        assertEquals(7890, review.id());
        assertEquals("APPROVED", review.state());
        assertEquals("2024-06-02T12:00:00Z", review.submittedAt());
        assertEquals("reviewer1", review.user().login());
    }

    @Test
    @DisplayName("Deserializes Language map correctly")
    void deserializeLanguage() throws Exception {
        String json = """
                {"Java": 125000, "Python": 45000, "Shell": 3200}""";

        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("X-RateLimit-Remaining", "4995")
                .setBody(json));

        var request = client.buildRequest(server.url("/repos/user/repo/languages").toString(), null, null);
        var result = client.executePageWithRetry(request);
        java.util.Map<String, Long> languages = new com.fasterxml.jackson.databind.ObjectMapper()
                .readValue(result.body(),
                        new com.fasterxml.jackson.core.type.TypeReference<java.util.Map<String, Long>>() {});

        assertEquals(3, languages.size());
        assertEquals(125000L, languages.get("Java"));
        assertEquals(45000L, languages.get("Python"));
        assertEquals(3200L, languages.get("Shell"));
    }

    @Test
    @DisplayName("Handles PullRequest with null merged_at")
    void deserializePullRequest_nullMergedAt() throws Exception {
        String json = """
                [{
                    "number": 10,
                    "title": "WIP: new feature",
                    "state": "open",
                    "created_at": "2024-06-10T08:00:00Z",
                    "merged_at": null,
                    "user": {
                        "login": "testuser",
                        "id": 999
                    }
                }]""";

        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("X-RateLimit-Remaining", "4994")
                .setBody(json));

        var request = client.buildRequest(server.url("/test").toString(), null, null);
        var result = client.executePageWithRetry(request);
        List<PullRequest> prs = new com.fasterxml.jackson.databind.ObjectMapper()
                .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .readValue(result.body(), new com.fasterxml.jackson.core.type.TypeReference<List<PullRequest>>() {});

        assertEquals(1, prs.size());
        assertEquals("open", prs.get(0).state());
        assertNull(prs.get(0).mergedAt());
    }

    // =========================================================================
    // Pagination integration test (multi-page fetch)
    // =========================================================================

    @Test
    @DisplayName("fetchAllPages follows pagination Link headers across multiple pages")
    void fetchAllPages_multiplePages() throws Exception {
        String page1Url = server.url("/user/repos?per_page=100").toString();
        String page2Url = server.url("/user/repos?page=2&per_page=100").toString();

        // Page 1 — has a next link
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("X-RateLimit-Remaining", "4998")
                .setHeader("Link", "<" + page2Url + ">; rel=\"next\"")
                .setBody("""
                        [{
                            "id": 1, "name": "repo1", "full_name": "user/repo1",
                            "language": "Java", "created_at": "2024-01-01T00:00:00Z",
                            "visibility": "public"
                        }]"""));

        // Page 2 — no next link (last page)
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("X-RateLimit-Remaining", "4997")
                .setBody("""
                        [{
                            "id": 2, "name": "repo2", "full_name": "user/repo2",
                            "language": "Python", "created_at": "2024-02-01T00:00:00Z",
                            "visibility": "private"
                        }]"""));

        List<Repository> repos = client.fetchAllPages(page1Url,
                new com.fasterxml.jackson.core.type.TypeReference<>() {});

        assertEquals(2, repos.size());
        assertEquals("repo1", repos.get(0).name());
        assertEquals("repo2", repos.get(1).name());
        assertEquals(2, server.getRequestCount());
    }

    // =========================================================================
    // getRetryWaitMs tests
    // =========================================================================

    @Test
    @DisplayName("getRetryWaitMs uses Retry-After header when present")
    void getRetryWaitMs_usesRetryAfterHeader() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(429)
                .setHeader("Retry-After", "5")
                .setBody("rate limited"));

        var request = client.buildRequest(server.url("/test").toString(), null, null);
        var response = new OkHttpClient().newCall(request).execute();

        long waitMs = client.getRetryWaitMs(response, 1000);
        assertEquals(5000, waitMs);
        response.close();
    }

    @Test
    @DisplayName("getRetryWaitMs falls back to backoff when no Retry-After header")
    void getRetryWaitMs_fallsBackToBackoff() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(429)
                .setBody("rate limited"));

        var request = client.buildRequest(server.url("/test").toString(), null, null);
        var response = new OkHttpClient().newCall(request).execute();

        long waitMs = client.getRetryWaitMs(response, 4000);
        assertEquals(4000, waitMs);
        response.close();
    }
}
