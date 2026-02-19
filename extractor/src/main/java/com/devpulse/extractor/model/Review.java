package com.devpulse.extractor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Data transfer object representing a GitHub pull request review.
 * Maps from: /repos/{owner}/{repo}/pulls/{number}/reviews
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Review(
        @JsonProperty("id") long id,
        @JsonProperty("state") String state,
        @JsonProperty("submitted_at") String submittedAt,
        @JsonProperty("user") User user
) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record User(
            @JsonProperty("login") String login,
            @JsonProperty("id") long id
    ) {}
}
