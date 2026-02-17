package com.devpulse.extractor.orchestrator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for the DevPulse GitHub data extractor.
 * Coordinates extraction from GitHub API and loading into BigQuery.
 */
public class ExtractorApp {

    private static final Logger logger = LoggerFactory.getLogger(ExtractorApp.class);

    public static void main(String[] args) {
        logger.info("Starting DevPulse Extractor...");
        // TODO: Initialize config, client, and loader; run extraction pipeline
        logger.info("DevPulse Extractor finished.");
    }
}
