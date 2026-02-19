package com.devpulse.extractor.client;

import com.devpulse.extractor.model.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * GitHub REST API client with pagination, rate-limit handling,
 * exponential backoff retry, and conditional request support.
 *
 * <p>Thread-safe: the underlying {@link OkHttpClient} and {@link ObjectMapper}
 * are both thread-safe, and this class holds no mutable per-request state.</p>
 */
public class GitHubApiClient {

    private static final Logger logger = LoggerFactory.getLogger(GitHubApiClient.class);

    private static final String BASE_URL = "https://api.github.com";
    private static final int RATE_LIMIT_THRESHOLD = 100;
    private static final long INITIAL_BACKOFF_MS = 1_000;
    private static final long MAX_BACKOFF_MS = 60_000;
    private static final int MAX_RETRIES = 7; // 1s, 2s, 4s, 8s, 16s, 32s, 60s
    private static final int PER_PAGE = 100;

    static final Pattern LINK_NEXT_PATTERN =
            Pattern.compile("<([^>]+)>;\\s*rel=\"next\"");

    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String token;
    private final String username;

    public GitHubApiClient(String token, String username) {
        this(token, username, defaultHttpClient());
    }

    public GitHubApiClient(String token, String username, OkHttpClient httpClient) {
        this.token = token;
        this.username = username;
        this.httpClient = httpClient;
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private static OkHttpClient defaultHttpClient() {
        return new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .build();
    }

    // -------------------------------------------------------------------------
    // Public API endpoint methods
    // -------------------------------------------------------------------------

    /**
     * Fetches all repositories for the authenticated user.
     * Endpoint: GET /user/repos?per_page=100&type=owner
     */
    public List<Repository> getRepositories() throws IOException, InterruptedException {
        String url = BASE_URL + "/user/repos?per_page=" + PER_PAGE + "&type=owner";
        return fetchAllPages(url, new TypeReference<>() {});
    }

    /**
     * Fetches commit history for a repository.
     * Endpoint: GET /repos/{owner}/{repo}/commits?per_page=100&author={username}
     */
    public List<Commit> getCommits(String repoFullName) throws IOException, InterruptedException {
        String url = BASE_URL + "/repos/" + repoFullName + "/commits?per_page=" + PER_PAGE
                + "&author=" + username;
        return fetchAllPages(url, new TypeReference<>() {});
    }

    /**
     * Fetches all pull requests (open + closed + merged) for a repository.
     * Endpoint: GET /repos/{owner}/{repo}/pulls?state=all&per_page=100
     */
    public List<PullRequest> getPullRequests(String repoFullName) throws IOException, InterruptedException {
        String url = BASE_URL + "/repos/" + repoFullName + "/pulls?state=all&per_page=" + PER_PAGE;
        return fetchAllPages(url, new TypeReference<>() {});
    }

    /**
     * Fetches reviews for a specific pull request.
     * Endpoint: GET /repos/{owner}/{repo}/pulls/{number}/reviews?per_page=100
     */
    public List<Review> getReviews(String repoFullName, int prNumber) throws IOException, InterruptedException {
        String url = BASE_URL + "/repos/" + repoFullName + "/pulls/" + prNumber
                + "/reviews?per_page=" + PER_PAGE;
        return fetchAllPages(url, new TypeReference<>() {});
    }

    /**
     * Fetches language byte counts for a repository.
     * Endpoint: GET /repos/{owner}/{repo}/languages
     *
     * This endpoint does not paginate — it returns a single JSON object.
     */
    public Language getLanguages(String repoFullName) throws IOException, InterruptedException {
        String url = BASE_URL + "/repos/" + repoFullName + "/languages";
        String json = executeWithRetry(buildRequest(url, null, null));
        if (json == null) {
            return new Language(repoFullName, Collections.emptyMap());
        }
        Map<String, Long> languages = objectMapper.readValue(json, new TypeReference<>() {});
        return new Language(repoFullName, languages);
    }

    // -------------------------------------------------------------------------
    // Core HTTP execution with pagination, retries, and rate-limit handling
    // -------------------------------------------------------------------------

    /**
     * Fetches all pages for a paginated endpoint and deserializes each page
     * into a list of {@code T}, concatenating them.
     */
    <T> List<T> fetchAllPages(String initialUrl, TypeReference<List<T>> typeRef)
            throws IOException, InterruptedException {
        List<T> allResults = new ArrayList<>();
        String url = initialUrl;

        while (url != null) {
            Request request = buildRequest(url, null, null);
            PageResult result = executePageWithRetry(request);

            if (result.body() != null) {
                List<T> page = objectMapper.readValue(result.body(), typeRef);
                allResults.addAll(page);
                logger.debug("Fetched page with {} items from {}", page.size(), url);
            }

            url = result.nextUrl();
        }

        return allResults;
    }

    /**
     * Builds a GET request with authentication and optional conditional headers.
     */
    Request buildRequest(String url, String etag, String ifModifiedSince) {
        Request.Builder builder = new Request.Builder()
                .url(url)
                .header("Authorization", "Bearer " + token)
                .header("Accept", "application/vnd.github+json")
                .header("X-GitHub-Api-Version", "2022-11-28");

        if (etag != null) {
            builder.header("If-None-Match", etag);
        }
        if (ifModifiedSince != null) {
            builder.header("If-Modified-Since", ifModifiedSince);
        }

        return builder.build();
    }

    /**
     * Executes a request with retry logic, returning only the response body string.
     * Used for non-paginated endpoints.
     */
    String executeWithRetry(Request request) throws IOException, InterruptedException {
        PageResult result = executePageWithRetry(request);
        return result.body();
    }

    /**
     * Executes a request with exponential backoff retry on 429/503 responses
     * and proactive rate-limit pausing.
     */
    PageResult executePageWithRetry(Request request) throws IOException, InterruptedException {
        long backoffMs = INITIAL_BACKOFF_MS;

        for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
            try (Response response = httpClient.newCall(request).execute()) {
                int statusCode = response.code();
                logResponse(request.url().toString(), statusCode, response);

                // Handle rate limit and server errors with retry
                if (statusCode == 429 || statusCode == 503) {
                    if (attempt == MAX_RETRIES) {
                        throw new IOException("Max retries exceeded for " + request.url()
                                + " (last status: " + statusCode + ")");
                    }
                    long waitMs = getRetryWaitMs(response, backoffMs);
                    logger.warn("Received {} from {}. Retrying in {}ms (attempt {}/{})",
                            statusCode, request.url(), waitMs, attempt + 1, MAX_RETRIES);
                    Thread.sleep(waitMs);
                    backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
                    continue;
                }

                // Proactive rate limit pause
                handleRateLimitPause(response);

                // 304 Not Modified — no new data
                if (statusCode == 304) {
                    return new PageResult(null, null);
                }

                if (statusCode < 200 || statusCode >= 300) {
                    throw new IOException("GitHub API error: " + statusCode + " for " + request.url());
                }

                ResponseBody body = response.body();
                String bodyString = body != null ? body.string() : null;
                String nextUrl = parseNextPageUrl(response.header("Link"));

                return new PageResult(bodyString, nextUrl);
            }
        }

        throw new IOException("Exhausted retries for " + request.url());
    }

    // -------------------------------------------------------------------------
    // Rate limit handling
    // -------------------------------------------------------------------------

    /**
     * If remaining rate limit is below the threshold, sleep until the reset time.
     */
    void handleRateLimitPause(Response response) throws InterruptedException {
        String remainingHeader = response.header("X-RateLimit-Remaining");
        String resetHeader = response.header("X-RateLimit-Reset");

        if (remainingHeader == null || resetHeader == null) {
            return;
        }

        int remaining = Integer.parseInt(remainingHeader);
        if (remaining < RATE_LIMIT_THRESHOLD) {
            long resetEpoch = Long.parseLong(resetHeader);
            long nowEpoch = Instant.now().getEpochSecond();
            long sleepSeconds = Math.max(resetEpoch - nowEpoch + 1, 1);

            logger.warn("Rate limit low ({} remaining). Pausing for {}s until reset.",
                    remaining, sleepSeconds);
            Thread.sleep(Duration.ofSeconds(sleepSeconds).toMillis());
        }
    }

    /**
     * Determines wait time for retries. Uses Retry-After header if present,
     * otherwise falls back to exponential backoff.
     */
    long getRetryWaitMs(Response response, long backoffMs) {
        String retryAfter = response.header("Retry-After");
        if (retryAfter != null) {
            try {
                return Long.parseLong(retryAfter) * 1_000;
            } catch (NumberFormatException ignored) {
                // fall through to backoff
            }
        }
        return backoffMs;
    }

    // -------------------------------------------------------------------------
    // Pagination parsing
    // -------------------------------------------------------------------------

    /**
     * Parses the "next" URL from the GitHub Link header.
     *
     * <p>Example header:
     * {@code <https://api.github.com/user/repos?page=2>; rel="next", <...>; rel="last"}
     *
     * @return the next page URL, or {@code null} if there is no next page
     */
    static String parseNextPageUrl(String linkHeader) {
        if (linkHeader == null || linkHeader.isEmpty()) {
            return null;
        }
        Matcher matcher = LINK_NEXT_PATTERN.matcher(linkHeader);
        return matcher.find() ? matcher.group(1) : null;
    }

    // -------------------------------------------------------------------------
    // Logging
    // -------------------------------------------------------------------------

    private void logResponse(String url, int statusCode, Response response) {
        String remaining = response.header("X-RateLimit-Remaining");
        logger.info("GitHub API {} {} | rate-limit-remaining: {}",
                statusCode, url, remaining != null ? remaining : "n/a");
    }

    // -------------------------------------------------------------------------
    // Internal result holder
    // -------------------------------------------------------------------------

    record PageResult(String body, String nextUrl) {}
}
