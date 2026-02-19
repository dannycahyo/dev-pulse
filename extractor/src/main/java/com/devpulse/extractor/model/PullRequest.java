package com.devpulse.extractor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Data transfer object representing a GitHub pull request.
 * Maps from: /repos/{owner}/{repo}/pulls?state=all
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PullRequest(
        @JsonProperty("number") int number,
        @JsonProperty("title") String title,
        @JsonProperty("state") String state,
        @JsonProperty("created_at") String createdAt,
        @JsonProperty("merged_at") String mergedAt,
        @JsonProperty("user") User user
) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record User(
            @JsonProperty("login") String login,
            @JsonProperty("id") long id
    ) {}
}
