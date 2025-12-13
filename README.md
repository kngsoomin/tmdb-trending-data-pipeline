# 🎥 TMDB Trending Data Pipeline (Snowflake + Airflow)

**Tech Stack:** Python · Apache Airflow · Snowflake · Docker · TMDB API

## Overview

This project demonstrates the design and implementation of a production-style data pipeline that ingests daily trending movies and TV shows from the TMDB API and delivers analytics-ready tables in Snowflake.

The focus is on reliability under real-world constraints (API limitations),
layered transformations, and idempotent delivery.

The resulting dataset captures daily popularity snapshots of content
and supports trend and time-series analysis.

---

## Design Focus

This pipeline was built with real-world production considerations in mind:

- Incremental ingestion under external API constraints
- Idempotent transformations and upserts
- Clear separation of RAW, STG, and DW layers
- Built-in data quality checks and operational metrics for better observability

---

## Architecture

```mermaid
flowchart LR
    TMDB[TMDB Trending API]

    subgraph Airflow["Apache Airflow"]
        FETCH[Fetch Trending Snapshot]
        LOAD_RAW[Load RAW JSON]
        STG_BUILD[Build STG Tables]
        DQ[Data Quality Checks]
        MERGE_DW[Merge into DW]
        METRICS[Log Pipeline Metrics]
    end

    subgraph Snowflake["Snowflake"]
        RAW[(RAW\nTMDB_TRENDING_RAW)]
        STG[(STG\nTMDB_TRENDING_STG)]
        DIM[(DW\nDIM_CONTENT)]
        FACT[(DW\nFACT_TRENDING)]
        METRIC_TBL[(DW\nPIPELINE_METRICS)]
    end

    TMDB --> FETCH
    FETCH --> LOAD_RAW
    LOAD_RAW --> RAW

    RAW --> STG_BUILD
    STG_BUILD --> STG
    STG --> DQ

    STG --> MERGE_DW
    MERGE_DW --> DIM
    MERGE_DW --> FACT

    LOAD_RAW --> METRICS
    STG_BUILD --> METRICS
    MERGE_DW --> METRICS
    METRICS --> METRIC_TBL
```

The pipeline is orchestrated by Airflow and follows a layered **RAW → STG → DW** design in Snowflake, with built-in data quality checks and operational metrics to ensure correctness and observability.

---

## Data Flow

### 1. Ingestion (RAW)

- Fetches the current TMDB trending snapshot
- Stores the raw JSON payload without transformation
- Loads data into Snowflake using PUT and COPY INTO

### 2. Transformation (STG)

- Flattens nested JSON arrays
- Extracts and type-casts structured fields
- Applies basic normalization
- Enforces data quality constraints

### 3. Analytics (DW)

- Upserts content metadata into a dimension table
- Maintains historical popularity metrics in a fact table

---

## Data Model

### Raw Layer

Stores the original API response as semi-structured JSON.

- Minimal processing with no business logic applied
- Schema-on-read design
- Replace-on-rerun semantics for the same date

### STG Layer

Represents a cleaned, flattened view of the raw payload.

- One row per `(trending_date, content, time_window)`
- Handles schema normalization and validation
- Designed to be fully reproducible for a given date

### DW Layer

- `DIM_CONTENT` stores stable metadata for movies and TV shows
- `FACT_TRENDING` stores daily popularity metrics for analytics and trend analysis

---

## Orchestration & Reliability

- Tasks are grouped by data layer (RAW / STG / DW)
- Each task performs a single, clearly scoped responsibility
- Failures prevent downstream execution to protect data correctness

### Operational Metrics

Each layer logs row counts and execution metadata into a metrics table, allowing:

- Detection of empty or partial loads
- Monitoring of volume changes over time
- Easier debugging during pipeline failures

---

## Backfill & Idempotency

### API Backfill Constraint

The TMDB Trending API only provides the current popularity snapshot.
Historical snapshots cannot be retrieved.

To prevent incorrect data:

- API ingestion is skipped for historical logical dates
- Downstream backfills are supported from existing RAW snapshots (RAW → STG → DW)

## Idempotent Loads

- Date-partitioned ingestion
- Deterministic transformations
- MERGE-based upserts in the DW layer

Re-running the pipeline for the same date does not create duplicates.

---

## How to Run

### Prerequisites

- Docker & Docker Compose
- Snowflake account
- TMDB API key

### Set up

#### Environment Variables

Runtime configuration is managed via a `.env` file.
Create a `.env` file in the project root with the following variables:

```env
AIRFLOW_UID=
AIRFLOW_PROJ_DIR=.
_AIRFLOW_WWW_USER_USERNAME=
_AIRFLOW_WWW_USER_PASSWORD=
TMDB_API_KEY=
TMDB_READ_ACCESS_TOKEN=
```

- `TMDB_API_KEY` is required for fetching trending data from the TMDB API.
- Airflow-related variables are used only for local development.

#### Snowflake Connection

- Snowflake credentials are managed via an Airflow Connection
  (`conn_id = snowflake_conn`) and are not stored in code or environment variables.

### Start Local Airflow Environment

```bash
make up
```

### Initialize Database Objects

Open http://localhost:8081 and run once:

```bash
tmdb_trending_bootstrap
```

### Run the Pipeline

Enable and trigger:

```bash
tmdb_trending_pipeline
```

---

## Testing

Unit tests focus on deterministic and isolated behavior of the pipeline logic.

- External API calls are mocked to avoid external dependencies
- No Airflow scheduler or Snowflake connection is required

This ensures fast, repeatable tests while keeping external systems outside the unit test boundary.

Run tests locally:

```bash
make test
```
# tmdb-trending-data-pipeline
