package com.devpulse.extractor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Data transfer object representing a GitHub commit.
 * Maps from the nested GitHub API response: /repos/{owner}/{repo}/commits
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Commit(
        @JsonProperty("sha") String sha,
        @JsonProperty("commit") CommitDetail commit,
        @JsonProperty("author") GitHubUser author,
        @JsonProperty("stats") CommitStats stats
) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record CommitDetail(
            @JsonProperty("message") String message,
            @JsonProperty("author") CommitAuthor author
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record CommitAuthor(
            @JsonProperty("name") String name,
            @JsonProperty("email") String email,
            @JsonProperty("date") String date
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record CommitStats(
            @JsonProperty("additions") int additions,
            @JsonProperty("deletions") int deletions,
            @JsonProperty("total") int total
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record GitHubUser(
            @JsonProperty("login") String login,
            @JsonProperty("id") long id
    ) {}
}
