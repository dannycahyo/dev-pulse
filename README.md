# DevPulse

Personal developer analytics platform that transforms GitHub activity data into actionable insights. DevPulse extracts commits, pull requests, code reviews, and repository metadata from the GitHub API, processes it through a modern data stack, and presents findings via interactive dashboards.

## Tech Stack

| Layer          | Technology                | Version     |
|----------------|---------------------------|-------------|
| Extraction     | Java + OkHttp + Jackson   | Java 21     |
| Build          | Gradle (Kotlin DSL)       | 8.x         |
| Storage        | Google BigQuery           | —           |
| Transformation | dbt Core + dbt-bigquery   | 1.8+        |
| Orchestration  | Apache Airflow            | 2.9+        |
| Visualization  | Metabase (OSS)            | latest      |
| Infrastructure | Docker Compose            | 3.8         |
| Database       | PostgreSQL                | 15          |

## Repository Structure

```
devpulse/
├── extractor/                 # Java Gradle project — GitHub API extractor
│   ├── build.gradle.kts
│   ├── settings.gradle.kts
│   └── src/
│       ├── main/java/com/devpulse/extractor/
│       │   ├── client/        # GitHub API client with retry & rate limiting
│       │   ├── model/         # DTOs mapped from GitHub API responses
│       │   ├── loader/        # BigQuery writer components
│       │   ├── config/        # Environment & configuration management
│       │   └── orchestrator/  # Main entry point
│       └── main/resources/
│           └── logback.xml
├── dbt/                       # dbt project — SQL transformations
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── sources.yml
│       ├── staging/           # Cleaned, typed, deduplicated models
│       └── mart/              # Analytical models (facts, dims, aggregates)
├── airflow/                   # Apache Airflow — pipeline orchestration
│   ├── dags/
│   └── plugins/
├── metabase/                  # Metabase configuration & exports
├── docker-compose.yml         # Local development environment
├── .env.example               # Template for environment variables
└── .gitignore
```

## Prerequisites

- **Java 21** (LTS)
- **Gradle 8.x** (or use the wrapper)
- **Docker** and **Docker Compose**
- **Python 3.11+** (for dbt)
- **Google Cloud** project with BigQuery enabled
- **GitHub** personal access token (scopes: `repo:read`, `user:read`)

## Setup

### 1. Clone and configure environment

```bash
git clone <repository-url>
cd devpulse
cp .env.example .env
```

Edit `.env` and fill in your credentials:
- `GITHUB_TOKEN` — GitHub personal access token
- `GCP_PROJECT_ID` — your Google Cloud project ID
- `GOOGLE_APPLICATION_CREDENTIALS` — path to BigQuery service account key JSON

### 2. Generate Airflow Fernet key

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Add the output to `AIRFLOW__CORE__FERNET_KEY` in your `.env` file.

### 3. Build the Java extractor

```bash
cd extractor
gradle build
```

### 4. Install dbt

```bash
pip install dbt-bigquery
cd dbt
dbt debug  # verify BigQuery connection
```

### 5. Start infrastructure services

```bash
docker compose up -d
```

Services will be available at:
- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Metabase**: http://localhost:3000
- **PostgreSQL**: localhost:5432

### 6. Verify setup

```bash
# Check all containers are healthy
docker compose ps

# Run extractor tests
cd extractor && gradle test

# Verify dbt connection
cd dbt && dbt debug
```

## Data Flow

1. Java extractor fetches data from GitHub REST API with pagination and rate-limit handling
2. Raw JSON payloads are loaded into BigQuery `devpulse_raw` dataset
3. dbt staging models clean, type-cast, and deduplicate raw data
4. dbt mart models produce analytical tables (facts, dimensions, aggregates)
5. Metabase queries mart layer to render dashboards
6. Airflow orchestrates steps 1-4 on a daily schedule

## License

See [LICENSE](LICENSE) for details.
