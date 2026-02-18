package com.devpulse.extractor.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link AppConfig} validation logic.
 */
class AppConfigTest {

    @Test
    @DisplayName("Test constructor creates config with valid values")
    void validConfig() {
        AppConfig config = new AppConfig("ghp_test_token", "my-project", "testuser");

        assertEquals("ghp_test_token", config.getGithubToken());
        assertEquals("my-project", config.getGcpProjectId());
        assertEquals("testuser", config.getGithubUsername());
    }

    @Test
    @DisplayName("Throws when GITHUB_TOKEN is missing")
    void missingToken_throws() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new AppConfig("", "project", "user"));

        assertTrue(ex.getMessage().contains("GITHUB_TOKEN"));
    }

    @Test
    @DisplayName("Throws when GCP_PROJECT_ID is missing")
    void missingProject_throws() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new AppConfig("token", "", "user"));

        assertTrue(ex.getMessage().contains("GCP_PROJECT_ID"));
    }

    @Test
    @DisplayName("Throws when GITHUB_USERNAME is missing")
    void missingUsername_throws() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new AppConfig("token", "project", ""));

        assertTrue(ex.getMessage().contains("GITHUB_USERNAME"));
    }

    @Test
    @DisplayName("Throws with all missing vars listed in message")
    void allMissing_listsAllVars() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new AppConfig("", "", ""));

        String msg = ex.getMessage();
        assertTrue(msg.contains("GITHUB_TOKEN"));
        assertTrue(msg.contains("GCP_PROJECT_ID"));
        assertTrue(msg.contains("GITHUB_USERNAME"));
    }

    @Test
    @DisplayName("Throws when values are blank (whitespace only)")
    void blankValues_throws() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new AppConfig("  ", "project", "user"));

        assertTrue(ex.getMessage().contains("GITHUB_TOKEN"));
    }
}
