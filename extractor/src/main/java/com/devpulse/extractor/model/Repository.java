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
        @JsonProperty("language") String language,
        @JsonProperty("created_at") String createdAt,
        @JsonProperty("visibility") String visibility
) {}
