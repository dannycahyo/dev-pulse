package com.devpulse.extractor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Data transfer object representing a GitHub repository.
 * Maps from: /user/repos
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Repository(
        @JsonProperty("id") long id,
        @JsonProperty("name") String name,
        @JsonProperty("full_name") String fullName,
        @JsonProperty("owner") Owner owner,
        @JsonProperty("language") String language,
        @JsonProperty("created_at") String createdAt,
        @JsonProperty("updated_at") String updatedAt,
        @JsonProperty("visibility") String visibility,
        @JsonProperty("fork") boolean fork,
        @JsonProperty("stargazers_count") long stargazersCount
) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Owner(
            @JsonProperty("login") String login
    ) {}
}
