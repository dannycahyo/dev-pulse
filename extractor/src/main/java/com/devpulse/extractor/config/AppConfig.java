package com.devpulse.extractor.config;

import io.github.cdimascio.dotenv.Dotenv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration management class that reads environment variables
 * and .env file settings using dotenv-java. Validates required
 * variables on startup.
 */
public class AppConfig {

    private static final Logger logger = LoggerFactory.getLogger(AppConfig.class);

    private final String githubToken;
    private final String gcpProjectId;
    private final String githubUsername;
    private final String googleApplicationCredentials;

    public AppConfig() {
        Dotenv dotenv = Dotenv.configure()
                .ignoreIfMissing()
                .load();

        this.githubToken = resolve(dotenv, "GITHUB_TOKEN");
        this.gcpProjectId = resolve(dotenv, "GCP_PROJECT_ID");
        this.githubUsername = resolve(dotenv, "GITHUB_USERNAME");
        this.googleApplicationCredentials = resolveOptional(dotenv, "GOOGLE_APPLICATION_CREDENTIALS");

        validate();

        logger.info("Configuration loaded: githubUsername={}, gcpProjectId={}", githubUsername, gcpProjectId);
    }

    /**
     * Constructor for testing — accepts values directly.
     */
    public AppConfig(String githubToken, String gcpProjectId, String githubUsername) {
        this.githubToken = githubToken;
        this.gcpProjectId = gcpProjectId;
        this.githubUsername = githubUsername;
        this.googleApplicationCredentials = null;

        validate();
    }

    private void validate() {
        StringBuilder missing = new StringBuilder();
        if (isBlank(githubToken)) missing.append("GITHUB_TOKEN ");
        if (isBlank(gcpProjectId)) missing.append("GCP_PROJECT_ID ");
        if (isBlank(githubUsername)) missing.append("GITHUB_USERNAME ");

        if (!missing.isEmpty()) {
            throw new IllegalStateException(
                    "Missing required environment variables: " + missing.toString().trim());
        }
    }

    private static String resolve(Dotenv dotenv, String key) {
        String envValue = System.getenv(key);
        if (envValue != null && !envValue.isBlank()) {
            return envValue;
        }
        String dotenvValue = dotenv.get(key);
        return dotenvValue != null ? dotenvValue : "";
    }

    private static String resolveOptional(Dotenv dotenv, String key) {
        String envValue = System.getenv(key);
        if (envValue != null && !envValue.isBlank()) {
            return envValue;
        }
        return dotenv.get(key);
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    public String getGithubToken() {
        return githubToken;
    }

    public String getGcpProjectId() {
        return gcpProjectId;
    }

    public String getGithubUsername() {
        return githubUsername;
    }

    public String getGoogleApplicationCredentials() {
        return googleApplicationCredentials;
    }
}
