# Reproducible Analytics on a Non-Replayable API

## Overview

Most public APIs do not provide historical snapshots, making reproducible analytics difficult.

The TMDB trending API only returns the current result for a given time window, which makes backfills and reruns unreliable. Once the data changes, the same request may no longer return the same result.

This project starts with that question:

**How do you build a reliable and reproducible data pipeline on top of a non-replayable API?**

One practical answer is to shift the source of truth away from the API.

Instead of relying on the API to reproduce past results, the pipeline persists immutable raw snapshots in S3 and uses them as the replayable source of truth. This ensures that every run can be reproduced from stored data rather than depending on the external API.

To make this design work in practice, the system enforces three constraints:

- **Reproducibility** — all analytical results must be derived from persisted raw data, not live API responses  
- **Incremental enrichment** — additional data is fetched only for previously unseen entities to avoid unnecessary API calls  
- **Separation of concerns** — orchestration (Airflow) and transformation (dbt) are decoupled to keep the system maintainable


## Architecture
![Architecture Overview](docs/architecture_overview.png)

The system is designed to make reproducible analytics possible despite a non-replayable upstream API.

To achieve this, the pipeline separates storage, orchestration, and transformation into independent components:

- **S3** stores immutable raw API snapshots and serves as the replayable source of truth  
- **Airflow** orchestrates ingestion, replay decisions, enrichment, and downstream execution  
- **Snowflake** acts as the analytical warehouse  
- **dbt** handles transformation and data quality within the warehouse  

A key design choice is the strict separation between orchestration and transformation.

Airflow operates as a **stateful orchestrator**, managing scheduling, retries, and execution state, while dbt runs in a **separate, stateless container** and is invoked only when transformations are required.

This prevents tight coupling between pipeline control logic and transformation logic, making the system easier to reason about and evolve independently.

Overall, the architecture is intentionally structured so that **data reproducibility is guaranteed by storage (S3), not by the external API**.


## Replay-Aware Data Flow
![Data Fow](docs/dataflow.png)

The pipeline is built around a replay-aware ingestion strategy to compensate for the lack of historical access in the TMDB API.

Each run begins by checking whether a raw snapshot exists in S3 for the given `logical_date`.

This leads to three possible execution paths:

- **Snapshot exists in S3**  
  The pipeline reuses the stored raw data and loads it into Snowflake.

- **Snapshot does not exist and `logical_date` is today**  
  The pipeline fetches fresh data from the TMDB API, persists it in S3, and then loads it into Snowflake.

- **Snapshot does not exist and `logical_date` is in the past**  
  The pipeline fails.

This fail-fast behavior is intentional: the system prioritizes reproducibility over silently reconstructing past data from a non-deterministic upstream source.

Once trending data is loaded, the pipeline extracts `tmdb_id` values and performs incremental enrichment:

- Additional datasets (details, credits) are fetched only for IDs not already present in S3  
- This avoids redundant API calls and ensures efficient data expansion  

Finally, dbt transforms the data in Snowflake into structured analytical models (staging → intermediate → marts) and applies tests to validate model integrity.


## Data Modeling & Transformation
![Data Lineage](docs/data-lineage.png)

Raw data from the TMDB API is semi-structured, fragmented across multiple endpoints, and not immediately suitable for analysis.

To make this data usable, the pipeline applies a layered transformation strategy in Snowflake using dbt.

Transformations are executed only after all required raw datasets are fully loaded, ensuring that downstream models are built on a complete and consistent snapshot of data.

The modeling approach follows three layers:

### Staging Layer — Standardize raw API data

The staging layer converts raw JSON payloads into structured, analysis-friendly tables.

- Flattens nested JSON fields from TMDB API responses  
- Renames and casts columns into consistent formats  
- Applies basic data quality checks (e.g., not null, accepted values)  

Each source dataset (trending, details, credits) is transformed independently, preserving source-level granularity.

### Intermediate Layer — Enrich and reshape data

The intermediate layer combines multiple datasets into enriched, analysis-ready structures.

- Uses `tmdb_id` from trending data as the primary join key  
- Joins additional datasets (details, credits)  
- Aggregates and reshapes data into unified representations  

This layer bridges raw ingestion and analytical models, making relationships between datasets explicit.

### Mart Layer — Define analytical models

The mart layer exposes business-facing models optimized for querying and reporting.

- **dim_content**  
  Consolidates core attributes of movies and TV shows  

- **fct_trending_daily**  
  Captures daily trending metrics such as popularity and vote counts  

These models follow dimensional modeling principles and are designed for analytical consumption.


## Data Quality

Data quality is enforced using dbt tests applied at the transformation layer.

- Not null constraints on key fields (e.g., `tmdb_id`)  
- Uniqueness checks on business keys  
- Basic integrity checks through model relationships  

Tests are defined alongside models and executed as part of the dbt pipeline.


## Key Design Decisions

### 1. Persist raw snapshots as the source of truth

**Problem**  
The TMDB API does not provide historical snapshots, making past data non-reproducible.

**Decision**  
Persist all raw API responses in S3 and treat them as immutable source data.

**Rationale**  
This shifts reproducibility from the external API to the data platform itself.  
All downstream processing is based on stored data rather than live API responses.


### 2. Fail fast when historical reproducibility cannot be guaranteed

**Problem**  
Re-running pipelines for past dates without stored snapshots would require re-fetching data from the API, which may no longer match the original result.

**Decision**  
If no snapshot exists for a past `logical_date`, the pipeline fails instead of attempting reconstruction.

**Rationale**  
This avoids silently producing inconsistent or misleading results and enforces strict reproducibility guarantees.


### 3. Incremental enrichment to minimize API usage

**Problem**  
Fetching enrichment data (details, credits) for all records repeatedly would lead to unnecessary API calls and increased cost.

**Decision**  
Only fetch additional datasets for `tmdb_id` values not already present in S3.

**Rationale**  
This ensures efficient API usage while allowing the dataset to grow incrementally over time.


### 4. Separate orchestration from transformation

**Problem**  
Coupling orchestration logic (Airflow) with transformation logic (SQL/dbt) leads to tightly bound systems that are harder to maintain and extend.

**Decision**  
Run dbt in a separate container and invoke it from Airflow only when needed.

**Rationale**  
This keeps responsibilities clearly separated and allows each layer to evolve independently without introducing unnecessary dependencies.



## Limitations & Trade-offs

### 1. Reproducibility depends on snapshot availability

Reproducibility is only guaranteed after raw snapshots have been captured in S3.

Data prior to the initial ingestion cannot be reconstructed, as the upstream API does not provide historical access.

**Trade-off**  
The system guarantees strong reproducibility going forward, but cannot recover data that was never captured.


### 2. Increased storage cost due to raw data persistence

Persisting all raw API responses in S3 ensures replayability but increases storage usage over time.

**Trade-off**  
The system favors reproducibility and auditability over storage efficiency.


### 3. Batch-oriented design limits real-time capabilities

The pipeline is designed around batch execution with Airflow and does not support real-time ingestion or streaming updates.

**Trade-off**  
The design remains simple and reproducible, but does not provide low-latency data updates.


### 4. Upstream schema changes require explicit handling

Changes in the TMDB API response structure (e.g., new fields, removed fields, schema shifts) are not automatically handled.

**Trade-off**  
The system avoids implicit assumptions and favors explicit schema management, at the cost of requiring manual updates when upstream changes occur.



## Project Structure

```text
.
├── dags/                           # Airflow DAG definitions (workflow orchestration)
│   ├── tmdb_ingestion_dag.py
│   └── tmdb_transformation_dag.py
│
├── src/                            # Core application logic 
│   ├── connector/                  # External system connectors (S3, Snowflake)                  
│   │   ├── s3.py
│   │   └── snowflake.py
│   │
│   ├── ingestion/              
│   │   ├── load/
│   │   │   └── snowflake_raw.py    # Handles S3 → Snowflake RAW loading
│   │   ├── sql/                    # SQL templates for COPY INTO / ingestion
│   │   │   ├── load_tmdb_trending.py
│   │   │   ├── load_tmdb_details.py
│   │   │   └── load_tmdb_credits.py
│   │   └── tmdb/                   # TMDB API clients
│   │       ├── client.py
│   │       ├── credits.py
│   │       ├── details.py
│   │       └── trending.py
│   └── tmdb_orchestration.py      # Coordinates ingestion workflows
│
├── dbt/                           # dbt project (transformation layer)
│   └── tmdb_dbt/
│       ├── models/
│       │   ├── staging/
│       │   │   ├── stg_tmdb_trending.sql
│       │   │   ├── stg_tmdb_details.sql
│       │   │   ├── stg_tmdb_details_genres.sql
│       │   │   ├── stg_tmdb_credits_cast.sql
│       │   │   └── stg_tmdb_credits_crew.sql
│       │   ├── intermediate/
│       │   │   ├── int_tmdb_credits_summary.sql
│       │   │   └── int_tmdb_genres_summary.sql
│       │   └── marts/
│       │       ├── dim_content.sql
│       │       └── fct_trending_daily.sql
│       ├── dbt_project.yml
│       └── profiles.yml
│
├── tests/                          # Local smoke / validation tests
│
├── docker-compose.yml              # Multi-container setup (Airflow, dbt, etc.)
├── Dockerfile.airflow              # Airflow container image
├── Dockerfile.dbt                  # dbt container image
├── requirements-airflow.txt
├── requirements-dbt.txt
├── .env                            # Environment variables (not committed)
└── README.md

```
The project is structured to clearly separate orchestration, ingestion, and transformation responsibilities.

- **Airflow (`dags/`)** defines and schedules workflows, acting as the orchestration layer.
- **Core logic (`src/`)** contains modular ingestion components, including API clients, data loaders, and system connectors.
- **dbt (`dbt/`)** is fully isolated and handles all transformation logic inside Snowflake, including staging, intermediate models, and data marts.
- **Docker** is used to containerize Airflow and dbt separately, ensuring environment consistency and clear separation of concerns.

This structure allows each layer of the pipeline to be developed, tested, and operated independently.


## Getting Started
To run the pipeline locally:

```bash
git clone https://github.com/kngsoomin/tmdb-trending-data-pipeline
cd tmdb-trending-data-pipeline


# set environment variables
cp .env.example .env

# start services
docker compose up -d
```
Airflow UI is available at http://localhost:8081.

dbt transformations can be triggered via Airflow or executed manually:
```bash
# find dbt container name (e.g. *dbt*)
docker ps | grep dbt

# run dbt inside the container
docker exec -it <dbt-container-name> dbt run
docker exec -it <dbt-container-name> dbt test
```

## Future Work

- Extend data quality framework with stronger validation (e.g., schema enforcement and completeness checks)  
- Add monitoring and alerting for ingestion and transformation failures  
- Expand enrichment pipeline with additional third-party data sources  
- Introduce downstream consumers (e.g., BI dashboards or analytics applications)


