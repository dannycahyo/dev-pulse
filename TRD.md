# DevPulse 📊 — Technical Requirement Document

> Your Personal Developer Analytics Platform

**Version:** 1.0  
**Author:** Hijra Engineering  
**Date:** February 17, 2026  
**Status:** Draft  
**Related Document:** DevPulse — Product Requirement Document v1.0

---

## 1. System Architecture Overview

DevPulse follows the modern ELT (Extract-Load-Transform) architecture pattern. Raw data is extracted from GitHub APIs using a Java application, loaded directly into BigQuery's raw layer, then transformed in-place using dbt. Apache Airflow orchestrates the entire pipeline on a daily schedule, and Metabase connects directly to BigQuery's mart layer for visualization.

### 1.1 Architecture Layers

| Layer          | Technology                 | Responsibility                                           |
| -------------- | -------------------------- | -------------------------------------------------------- |
| Extraction     | Java 21 + OkHttp/Retrofit  | Pull data from GitHub REST API v3 and GraphQL API v4     |
| Loading        | Java + BigQuery Client SDK | Write raw JSON/structured data into BigQuery raw dataset |
| Storage        | Google BigQuery            | Data warehouse with raw, staging, and mart datasets      |
| Transformation | dbt Core (Python)          | SQL-based models to clean, join, aggregate data          |
| Orchestration  | Apache Airflow 2.x         | Schedule, monitor, and manage pipeline execution         |
| Visualization  | Metabase (OSS)             | Interactive dashboards and ad-hoc querying               |
| Infrastructure | Docker Compose             | Local development environment for all services           |

### 1.2 Data Flow

1. GitHub API emits events (commits, PRs, reviews, repos).
2. Java extractor fetches data via REST/GraphQL with pagination and rate-limit handling.
3. Raw JSON payloads are loaded into BigQuery raw dataset tables.
4. dbt staging models clean, type-cast, and deduplicate raw data.
5. dbt mart models join, aggregate, and produce analytical tables.
6. Metabase queries mart tables to render dashboards.
7. Airflow triggers steps 2–5 daily and monitors execution.

---

## 2. Technology Stack Specifications

### 2.1 Extraction Layer — Java Application

| Component       | Specification                                   |
| --------------- | ----------------------------------------------- |
| Runtime         | Java 21 (LTS)                                   |
| Build Tool      | Gradle 8.x with Kotlin DSL                      |
| HTTP Client     | OkHttp 4.x / Retrofit 2.x                       |
| JSON Processing | Jackson 2.x (databind + Java time module)       |
| BigQuery SDK    | google-cloud-bigquery 2.x                       |
| Configuration   | Environment variables + .env file (dotenv-java) |
| Logging         | SLF4J + Logback                                 |
| Testing         | JUnit 5 + Mockito                               |

The extractor is structured as a modular Java application with the following packages:

- **`com.devpulse.extractor.client`** — GitHub API client classes with retry logic and rate limiting.
- **`com.devpulse.extractor.model`** — Data transfer objects mapping GitHub API responses.
- **`com.devpulse.extractor.loader`** — BigQuery writer components for each entity type.
- **`com.devpulse.extractor.config`** — Configuration management and environment parsing.
- **`com.devpulse.extractor.orchestrator`** — Main entry point coordinating extraction and loading.

### 2.2 Storage Layer — Google BigQuery

#### 2.2.1 Dataset Structure

| Dataset          | Purpose                                      | Access Pattern                          |
| ---------------- | -------------------------------------------- | --------------------------------------- |
| devpulse_raw     | Raw data exactly as received from GitHub API | Write by extractor, read by dbt staging |
| devpulse_staging | Cleaned, typed, deduplicated data            | Managed by dbt, read by dbt mart        |
| devpulse_mart    | Analytical models optimized for queries      | Managed by dbt, read by Metabase        |

#### 2.2.2 Raw Layer Tables

| Table             | Source                                         | Partitioning         | Clustering            |
| ----------------- | ---------------------------------------------- | -------------------- | --------------------- |
| raw_commits       | `GET /repos/{owner}/{repo}/commits`            | ingestion_time (DAY) | repo_full_name        |
| raw_pull_requests | `GET /repos/{owner}/{repo}/pulls`              | ingestion_time (DAY) | repo_full_name, state |
| raw_reviews       | `GET /repos/{owner}/{repo}/pulls/{id}/reviews` | ingestion_time (DAY) | repo_full_name        |
| raw_repositories  | `GET /user/repos`                              | ingestion_time (DAY) | language              |
| raw_languages     | `GET /repos/{owner}/{repo}/languages`          | ingestion_time (DAY) | repo_full_name        |

#### 2.2.3 Staging Layer Models

| Model             | Source            | Key Transformations                                            |
| ----------------- | ----------------- | -------------------------------------------------------------- |
| stg_commits       | raw_commits       | Parse dates, extract file stats, deduplicate by SHA            |
| stg_pull_requests | raw_pull_requests | Calculate duration, normalize status, deduplicate by PR number |
| stg_reviews       | raw_reviews       | Map review states, link to PRs, deduplicate by review ID       |
| stg_repositories  | raw_repositories  | Extract owner, normalize names, latest snapshot only           |
| stg_languages     | raw_languages     | Pivot language bytes, calculate percentages                    |

#### 2.2.4 Mart Layer Models

| Model               | Type      | Description                                                                |
| ------------------- | --------- | -------------------------------------------------------------------------- |
| dim_date            | Dimension | Calendar dimension with day-of-week, week, month, quarter, year attributes |
| dim_repository      | Dimension | Repository attributes: name, primary language, created date, visibility    |
| fct_commits         | Fact      | One row per commit: timestamp, repo, files changed, lines added/removed    |
| fct_pull_requests   | Fact      | One row per PR: created, merged, review count, time-to-merge               |
| fct_reviews         | Fact      | One row per review: timestamp, PR, review state, reviewer                  |
| agg_daily_activity  | Aggregate | Daily rollup: commit count, PR count, lines changed, active repos          |
| agg_weekly_activity | Aggregate | Weekly rollup with same metrics plus weekly trends                         |
| agg_language_usage  | Aggregate | Language usage over time by bytes, commits, and repos                      |
| agg_hourly_heatmap  | Aggregate | Commit counts by hour-of-day and day-of-week for heatmap                   |

### 2.3 Transformation Layer — dbt

| Component                         | Specification                                           |
| --------------------------------- | ------------------------------------------------------- |
| Version                           | dbt Core 1.8+                                           |
| Adapter                           | dbt-bigquery                                            |
| Profile                           | BigQuery service account with dataset-level permissions |
| Materialization (staging)         | View (cost-efficient, always fresh)                     |
| Materialization (mart facts)      | Incremental (append new data, reduce processing)        |
| Materialization (mart dimensions) | Table (full refresh, small datasets)                    |
| Materialization (mart aggregates) | Table (full refresh for correctness)                    |
| Testing Framework                 | dbt built-in tests + custom tests                       |

#### 2.3.1 dbt Project Structure

```
devpulse-dbt/
├── dbt_project.yml
├── profiles.yml
├── models/
│   ├── staging/      (stg_*.sql)
│   ├── mart/         (dim_*, fct_*, agg_*)
│   └── sources.yml
├── tests/
├── macros/
└── seeds/
```

#### 2.3.2 dbt Testing Strategy

| Test Type        | Coverage                   | Example                                                               |
| ---------------- | -------------------------- | --------------------------------------------------------------------- |
| unique           | Primary keys in all models | unique on fct_commits.commit_sha                                      |
| not_null         | Required fields            | not_null on fct_commits.committed_at                                  |
| accepted_values  | Enum fields                | accepted_values on fct_pull_requests.state ['open','closed','merged'] |
| relationships    | Foreign keys               | fct_commits.repo_id references dim_repository.repo_id                 |
| Custom: recency  | Data freshness             | Assert max(committed_at) is within last 48 hours                      |
| Source freshness | Raw table freshness        | warn_after: 24h, error_after: 48h on raw tables                       |

### 2.4 Orchestration Layer — Apache Airflow

| Component    | Specification                                    |
| ------------ | ------------------------------------------------ |
| Version      | Apache Airflow 2.9+                              |
| Executor     | LocalExecutor (single-node for personal project) |
| Database     | PostgreSQL 15 (Airflow metadata)                 |
| Deployment   | Docker Compose with official Airflow image       |
| Schedule     | Daily at 02:00 UTC                               |
| Retry Policy | 3 retries with 5-minute exponential backoff      |
| Alerting     | Email on failure (configurable)                  |

#### 2.4.1 DAG Design

**`devpulse_daily_pipeline`** — Main DAG executing the full ELT cycle:

1. **extract_commits** — Run Java extractor for commit data
2. **extract_pull_requests** — Run Java extractor for PR data (parallel with commits)
3. **extract_reviews** — Run Java extractor for review data (depends on PRs)
4. **extract_repositories** — Run Java extractor for repo metadata (parallel)
5. **dbt_run_staging** — Execute dbt staging models (depends on all extractions)
6. **dbt_run_mart** — Execute dbt mart models (depends on staging)
7. **dbt_test** — Run dbt tests (depends on mart)

### 2.5 Visualization Layer — Metabase

| Component           | Specification                                          |
| ------------------- | ------------------------------------------------------ |
| Version             | Metabase OSS (latest stable)                           |
| Deployment          | Docker container in compose stack                      |
| Database Connection | BigQuery service account (read-only to mart dataset)   |
| Internal DB         | PostgreSQL 15 (Metabase metadata, shared with Airflow) |

#### 2.5.1 Dashboard Specifications

**Dashboard 1: Productivity Overview**

- Commit trend line (daily/weekly/monthly toggle).
- Coding activity heatmap (hour vs day-of-week).
- Current streak and longest streak cards.
- Daily average commits (30-day rolling).

**Dashboard 2: Language Analytics**

- Language usage pie chart (by bytes).
- Language trend over time (stacked area).
- Top repositories per language.

**Dashboard 3: Pull Request Analytics**

- PR open-to-merge time distribution.
- Review turnaround time trend.
- PR volume by repository.
- Merge rate percentage.

**Dashboard 4: Repository Focus**

- Commit distribution across repositories (treemap).
- Repository activity timeline.
- Lines of code changed per repository.

---

## 3. Infrastructure and Deployment

### 3.1 Local Development Environment

The entire stack runs locally via Docker Compose for development and personal use. This keeps costs at zero while providing a production-like environment.

#### 3.1.1 Docker Compose Services

| Service           | Image                         | Ports | Purpose                            |
| ----------------- | ----------------------------- | ----- | ---------------------------------- |
| airflow-webserver | apache/airflow:2.9-python3.11 | 8080  | Airflow UI                         |
| airflow-scheduler | apache/airflow:2.9-python3.11 | —     | DAG scheduling                     |
| airflow-init      | apache/airflow:2.9-python3.11 | —     | DB initialization                  |
| postgres          | postgres:15                   | 5432  | Metadata DB for Airflow + Metabase |
| metabase          | metabase/metabase:latest      | 3000  | Dashboard UI                       |

### 3.2 Configuration and Secrets

| Secret                         | Purpose                                | Storage                            |
| ------------------------------ | -------------------------------------- | ---------------------------------- |
| GITHUB_TOKEN                   | GitHub API personal access token       | .env file (git-ignored)            |
| GOOGLE_APPLICATION_CREDENTIALS | BigQuery service account key path      | .env file + key.json (git-ignored) |
| GCP_PROJECT_ID                 | Google Cloud project identifier        | .env file                          |
| AIRFLOW**CORE**FERNET_KEY      | Airflow encryption key for connections | Docker Compose env                 |
| METABASE*DB*\*                 | Metabase internal database connection  | Docker Compose env                 |

---

## 4. GitHub API Integration Design

### 4.1 API Endpoints

| Endpoint                                     | Method | Data Extracted                |
| -------------------------------------------- | ------ | ----------------------------- |
| /user/repos                                  | GET    | Repository list with metadata |
| /repos/{owner}/{repo}/commits                | GET    | Commit history with stats     |
| /repos/{owner}/{repo}/pulls?state=all        | GET    | All pull requests             |
| /repos/{owner}/{repo}/pulls/{number}/reviews | GET    | PR reviews                    |
| /repos/{owner}/{repo}/languages              | GET    | Language byte counts          |

### 4.2 Rate Limiting Strategy

- **Primary limit:** 5,000 requests/hour for authenticated requests.
- **Conditional requests:** Use If-Modified-Since and ETag headers to avoid consuming rate limits on unchanged data.
- **Backoff:** Exponential backoff starting at 1 second, doubling up to 60 seconds on 429/503 responses.
- **Rate tracking:** Read X-RateLimit-Remaining headers and pause proactively when approaching limits.

### 4.3 Incremental Extraction

To minimize API calls and processing time, the extractor tracks the latest extracted timestamp per entity type. On each run, it only fetches records newer than the last successful extraction. This state is persisted in a BigQuery metadata table (`devpulse_raw._extraction_metadata`).

---

## 5. Data Quality Framework

| Layer      | Quality Check           | Implementation                                              |
| ---------- | ----------------------- | ----------------------------------------------------------- |
| Extraction | API response validation | Java DTOs with required field validation                    |
| Raw        | Schema enforcement      | BigQuery schema definitions with REQUIRED fields            |
| Staging    | Deduplication           | dbt models using ROW_NUMBER() with partition by natural key |
| Staging    | Type casting            | SAFE_CAST with null handling for malformed data             |
| Mart       | Referential integrity   | dbt relationship tests between fact and dimension tables    |
| Mart       | Freshness checks        | Custom dbt tests asserting max timestamp within SLA         |
| Mart       | Value constraints       | dbt accepted_values and custom range tests                  |

---

## 6. Security Considerations

- **Principle of least privilege:** BigQuery service account has only dataset-level read/write permissions, not project-level admin.
- **Secret management:** All tokens, keys, and credentials stored in .env files excluded from version control via .gitignore.
- **GitHub token scope:** Personal access token with minimum required scopes (repo:read, user:read).
- **Network isolation:** Docker services communicate on an internal bridge network; only UI ports are exposed to host.
- **Data sensitivity:** No PII beyond public GitHub profile data. Commit messages may contain sensitive info; treat warehouse access as confidential.

---

## 7. Monitoring and Observability

- **Airflow UI:** DAG run history, task duration trends, failure logs, and Gantt charts.
- **dbt artifacts:** Run results and test results JSON files tracked per execution.
- **BigQuery audit logs:** Query costs, slot usage, and data access patterns.
- **Extraction logs:** Java application logs with structured JSON format (request counts, errors, durations).
- **Metabase usage:** Dashboard view counts and query performance metrics.

---

## 8. Repository Structure

```
devpulse/
├── extractor/              # Java Gradle project
│   ├── build.gradle.kts
│   └── src/main/java/com/devpulse/
├── dbt/                    # dbt project
│   ├── models/staging/
│   ├── models/mart/
│   └── tests/
├── airflow/                # Airflow DAGs and config
│   ├── dags/
│   └── plugins/
├── metabase/               # Metabase config and exports
├── docker-compose.yml
├── .env.example
└── README.md
```
