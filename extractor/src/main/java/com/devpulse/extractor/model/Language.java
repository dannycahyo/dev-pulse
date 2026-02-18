package com.devpulse.extractor.model;

import java.util.Map;

/**
 * Data transfer object representing language byte counts for a repository.
 * Maps from: /repos/{owner}/{repo}/languages
 *
 * The GitHub API returns a simple JSON object: {"Java": 12345, "Python": 6789}
 * which is deserialized as a Map and wrapped with the repo identifier.
 */
public record Language(
        String repoFullName,
        Map<String, Long> languages
) {}
