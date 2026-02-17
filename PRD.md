# DevPulse 📊 — Product Requirement Document

> Your Personal Developer Analytics Platform

**Version:** 1.0  
**Author:** Hijra Engineering  
**Date:** February 17, 2026  
**Status:** Draft  
**Stakeholders:** Engineering Team

---

## 1. Executive Summary

DevPulse is a personal data engineering project designed to transform raw GitHub activity data into actionable developer analytics. The platform ingests commits, pull requests, code reviews, and repository metadata from the GitHub API, processes it through a modern data stack, and presents insights via interactive dashboards.

This project serves a dual purpose: it provides genuine value as a developer productivity tool while functioning as a comprehensive hands-on learning platform for data engineering concepts including ETL/ELT pipelines, data warehousing, transformation layers, workflow orchestration, and business intelligence visualization.

---

## 2. Problem Statement

### 2.1 Context

Developers generate significant amounts of activity data through their daily work on GitHub, including commits, pull requests, code reviews, issues, and repository interactions. However, this data remains fragmented across GitHub's interface and is difficult to analyze holistically for patterns and productivity insights.

### 2.2 Pain Points

- **Lack of historical perspective:** GitHub's contribution graph provides limited analytical depth and no ability to drill down by language, project, or time-of-day.
- **No cross-repository analytics:** Activity across multiple repositories is siloed, making it hard to understand total development effort.
- **Missing productivity patterns:** Developers cannot easily identify when they are most productive or how their coding habits evolve over time.
- **PR review bottlenecks hidden:** Review turnaround times and patterns are not surfaced, making it hard to optimize collaboration.

---

## 3. Goals and Objectives

### 3.1 Primary Goals

1. **Build a complete data pipeline** that extracts GitHub activity data, loads it into a warehouse, transforms it into analytical models, and visualizes it in dashboards.
2. **Learn modern data engineering practices** through hands-on implementation of industry-standard tools (BigQuery, dbt, Airflow, Metabase).
3. **Generate actionable insights** about personal coding patterns, productivity rhythms, and growth trajectory.

### 3.2 Learning Objectives

- Understand how data flows through extraction, loading, and transformation stages.
- Gain proficiency with dbt for SQL-based data transformations and data modeling.
- Learn Apache Airflow for orchestrating complex data workflows with dependencies.
- Practice dimensional modeling and data warehouse design patterns.
- Build interactive dashboards that answer real analytical questions.

---

## 4. Scope

### 4.1 In Scope

| Feature Area           | Description                                                                              |
| ---------------------- | ---------------------------------------------------------------------------------------- |
| GitHub Data Extraction | Commits, pull requests, reviews, repositories, and languages via GitHub REST/GraphQL API |
| Data Warehousing       | Raw, staging, and mart layer tables in Google BigQuery                                   |
| Data Transformation    | dbt models for cleaning, joining, aggregating, and enriching raw data                    |
| Orchestration          | Airflow DAGs for scheduled extraction, loading, and transformation                       |
| Dashboards             | Metabase dashboards for productivity, language, PR, and trend analytics                  |
| Single User            | Analytics for the developer's own GitHub account                                         |

### 4.2 Out of Scope

- Multi-user or team analytics (can be extended later).
- Real-time streaming ingestion (batch processing only).
- CI/CD pipeline analytics (GitHub Actions, Jenkins, etc.).
- Integration with non-GitHub platforms (GitLab, Bitbucket).
- Machine learning or predictive analytics.

---

## 5. User Stories and Requirements

### 5.1 Core User Stories

| ID    | User Story                                                                                                | Priority    |
| ----- | --------------------------------------------------------------------------------------------------------- | ----------- |
| US-01 | As a developer, I want to see my commit activity over time so I can understand my productivity trends.    | Must Have   |
| US-02 | As a developer, I want a heatmap of my coding hours so I can identify my most productive times.           | Must Have   |
| US-03 | As a developer, I want to see which languages I use most so I can track my skill development.             | Must Have   |
| US-04 | As a developer, I want to track PR review turnaround so I can improve collaboration speed.                | Should Have |
| US-05 | As a developer, I want to see code velocity (lines added/removed) so I can understand my output patterns. | Should Have |
| US-06 | As a developer, I want repository-level breakdowns so I can see where my effort is concentrated.          | Should Have |
| US-07 | As a developer, I want daily/weekly/monthly aggregations so I can compare periods.                        | Must Have   |
| US-08 | As a developer, I want the pipeline to run automatically so my data stays current.                        | Must Have   |

### 5.2 Functional Requirements

#### FR-01: Data Extraction

- Extract commit history with author, timestamp, message, files changed, and lines added/removed.
- Extract pull request data including creation date, merge date, reviewers, and status.
- Extract repository metadata including languages, stars, and creation dates.
- Support incremental extraction to avoid re-fetching historical data.
- Handle GitHub API rate limiting gracefully with exponential backoff.

#### FR-02: Data Storage

- Store raw extracted data in BigQuery raw layer with extraction timestamps.
- Maintain staging layer with cleaned and typed data.
- Build mart layer with analytical models optimized for dashboard queries.
- Implement partitioning and clustering for query performance.

#### FR-03: Data Transformation

- Clean and deduplicate raw data in staging models.
- Build fact tables for commits, pull requests, and reviews.
- Build dimension tables for repositories, time, and languages.
- Create aggregation models for daily, weekly, and monthly summaries.
- Implement dbt tests for data quality validation.

#### FR-04: Orchestration

- Schedule daily extraction and transformation pipeline.
- Define task dependencies ensuring correct execution order.
- Implement error handling, retries, and alerting.
- Support manual backfill for historical data.

#### FR-05: Visualization

- Productivity dashboard with commit trends, heatmaps, and streak tracking.
- Language analytics dashboard with usage distribution and trends.
- PR analytics dashboard with review times and merge patterns.
- Repository focus dashboard with effort distribution.

### 5.3 Non-Functional Requirements

- **Performance:** Dashboard queries should return within 3 seconds for up to 2 years of data.
- **Reliability:** Pipeline should handle transient API failures with automatic retries.
- **Maintainability:** Code should be well-documented with clear separation of concerns.
- **Cost Efficiency:** Stay within BigQuery free tier (1 TB query / 10 GB storage per month) for personal use.

---

## 6. Success Metrics

| Metric               | Target                             | Measurement                                                 |
| -------------------- | ---------------------------------- | ----------------------------------------------------------- |
| Pipeline Reliability | >95% daily success rate            | Airflow task success ratio over 30 days                     |
| Data Freshness       | <24 hours                          | Time between latest GitHub event and warehouse availability |
| Dashboard Load Time  | <3 seconds                         | Average Metabase query response time                        |
| Data Coverage        | >90% of GitHub events captured     | Comparison of warehouse records vs GitHub API totals        |
| Learning Milestones  | All 5 stack components operational | Each component deployed and functioning                     |

---

## 7. Project Phases and Timeline

| Phase                  | Description                                                  | Duration  |
| ---------------------- | ------------------------------------------------------------ | --------- |
| Phase 1: Foundation    | Project setup, GitHub extractor in Java, BigQuery raw tables | Week 1–2  |
| Phase 2: Warehouse     | dbt project setup, staging and mart models, data tests       | Week 3–4  |
| Phase 3: Orchestration | Airflow DAGs, scheduling, error handling, monitoring         | Week 5–6  |
| Phase 4: Visualization | Metabase setup, dashboard design, iterative refinement       | Week 7–8  |
| Phase 5: Polish        | Documentation, optimization, backfill, final testing         | Week 9–10 |

---

## 8. Risks and Mitigations

| Risk                     | Impact | Mitigation                                                                     |
| ------------------------ | ------ | ------------------------------------------------------------------------------ |
| GitHub API rate limiting | High   | Implement incremental extraction, caching, and conditional requests with ETags |
| BigQuery cost overrun    | Medium | Use partitioning, clustering, and query optimization; monitor usage            |
| Airflow complexity       | Medium | Start with simple DAGs and iterate; use managed Airflow if needed              |
| Scope creep              | Medium | Strict adherence to phased delivery; defer nice-to-haves                       |
| Data quality issues      | High   | Comprehensive dbt tests; data contracts for raw layer                          |

---

## 9. Glossary

| Term            | Definition                                                                                 |
| --------------- | ------------------------------------------------------------------------------------------ |
| ETL/ELT         | Extract-Transform-Load / Extract-Load-Transform — patterns for moving data into warehouses |
| dbt             | Data Build Tool — SQL-based transformation framework for analytics engineering             |
| DAG             | Directed Acyclic Graph — a workflow structure used by Airflow to define task dependencies  |
| Mart            | A curated data layer optimized for specific analytical use cases                           |
| Fact Table      | A table storing measurable events (e.g., commits, PRs) in a dimensional model              |
| Dimension Table | A table storing descriptive attributes (e.g., repos, dates) for context                    |
