# TMDB Data Engineering Pipeline

## Overview

This repository contains an end-to-end data engineering pipeline built around data from The Movie Database (TMDB) API. The project ingests raw TMDB data, lands it in object storage, loads it into Snowflake, transforms it through dbt into analytics-ready layers, orchestrates pipeline execution with Airflow, and serves curated insights through a Streamlit dashboard.

The architecture follows a layered medallion-style design:

TMDB API -> MinIO Bronze -> Snowflake Bronze -> dbt Silver -> dbt Gold -> Airflow -> Streamlit

The project was designed to demonstrate practical engineering across ingestion, storage, warehousing, transformation, orchestration, and analytics delivery.

## Architecture

### Pipeline flow

1. TMDB trending data is fetched from the TMDB API using an authenticated Python client.
2. Raw JSON responses are written to MinIO in a date-partitioned Bronze storage layout.
3. Raw Bronze files are loaded into Snowflake Bronze as semi-structured `VARIANT` records.
4. dbt transforms raw warehouse data into structured Silver and Gold models.
5. Airflow orchestrates the ingestion, loading, and transformation workflow.
6. Streamlit reads curated Gold models from Snowflake and presents them in a dashboard layer.

### Data layers

#### Bronze
The Bronze layer preserves raw ingestion outputs with minimal transformation.

- MinIO stores raw JSON objects from TMDB
- Snowflake Bronze stores one raw record per object
- Metadata such as object key, partition date, run ID, payload hash, and load timestamp are preserved

#### Silver
The Silver layer converts raw nested payloads into structured, row-level tables.

- TMDB trending payloads are flattened
- One row is produced per trending item
- Key attributes such as media type, title, popularity, and vote metrics are extracted

#### Gold
The Gold layer exposes analytics-ready models.

Current Gold models include:

- `fct_tmdb_trending_daily_summary`
- `dim_tmdb_trending_latest`
- `fct_tmdb_trending_top_items_daily`

These support summary reporting, latest-state analysis, and leaderboard-style trend exploration.

## Repository Structure

```text
tmdb-data-pipeline/
├── airflow/
│   ├── dags/
│   ├── Dockerfile
│   ├── docker-compose.yml
│   ├── logs/
│   ├── plugins/
│   └── requirements-airflow.txt
├── dbt/
│   └── tmdb_dbt/
│       ├── dbt_project.yml
│       ├── macros/
│       ├── models/
│       │   ├── gold/
│       │   ├── silver/
│       │   └── sources.yml
│       ├── packages.yml
│       └── target/
├── infra/
│   └── docker-compose.yml
├── ingestion/
│   ├── requirements.txt
│   └── src/
│       ├── config.py
│       ├── ingest_trending_daily.py
│       ├── load_trending_daily_to_snowflake.py
│       ├── logger.py
│       ├── minio_reader.py
│       ├── minio_writer.py
│       ├── snowflake_client.py
│       ├── tmdb_client.py
│       └── utils.py
├── streamlit_app/
│   ├── app.py
│   └── .streamlit/
├── .gitignore
└── README.md
```

## Core Components

## 1. Ingestion Layer

The ingestion layer is implemented in Python and is responsible for collecting TMDB data and persisting the raw response.

### Key modules

- `tmdb_client.py`  
  Handles authenticated TMDB API requests with timeout handling and retry support.

- `ingest_trending_daily.py`  
  Fetches the daily trending TMDB dataset and writes it to MinIO.

- `minio_writer.py`  
  Writes JSON payloads into the Bronze bucket.

- `config.py`  
  Loads and validates required environment variables.

- `utils.py`  
  Contains shared helpers such as date resolution, payload hashing, and key metadata parsing.

### Object key design

Raw files are stored using a partitioned object-key pattern:

```text
endpoint=trending/time_window=day/dt=YYYY-MM-DD/run_id=<uuid>.json
```

This enables:

- deterministic date partitioning
- lineage tracking
- easier downstream loading
- support for repeated runs on the same date

## 2. Warehouse Loading Layer

The warehouse loading layer moves raw Bronze files from MinIO into Snowflake Bronze.

### Loader behavior

- reads objects from a specified date partition
- validates object-key metadata
- skips objects already loaded
- writes new records into Snowflake Bronze
- records operational audit rows in `OPS.LOAD_AUDIT`

### Key features

- idempotent load logic
- object-key-based duplicate prevention
- payload hashing
- structured success, skipped, and failed audit tracking
- date-driven execution using `--dt`

## 3. Snowflake Design

Snowflake is used as the warehouse and is organized into multiple schemas.

### Schemas

- `BRONZE`
- `SILVER`
- `GOLD`
- `OPS`

### Examples of current objects

#### Bronze
- `TMDB_DB.BRONZE.TMDB_TRENDING_DAILY_RAW`

#### Ops
- `TMDB_DB.OPS.LOAD_AUDIT`

#### Silver
- `TMDB_DB.SILVER.STG_TMDB_TRENDING_DAILY`

#### Gold
- `TMDB_DB.GOLD.FCT_TMDB_TRENDING_DAILY_SUMMARY`
- `TMDB_DB.GOLD.DIM_TMDB_TRENDING_LATEST`
- `TMDB_DB.GOLD.FCT_TMDB_TRENDING_TOP_ITEMS_DAILY`

## 4. dbt Transformation Layer

dbt is used to model and test the warehouse transformation layers.

### Current dbt setup

- Bronze source definitions created
- Silver staging model created for flattened trending items
- Gold summary and dimension models created
- model documentation added
- source tests and model tests implemented
- custom `generate_schema_name` macro added so models materialize into the intended Snowflake schemas

### Current dbt models

#### Silver

**`stg_tmdb_trending_daily`**  
Flattens raw TMDB payloads into one row per trending item.

#### Gold

**`fct_tmdb_trending_daily_summary`**  
Aggregates daily trending metrics by media type.

**`dim_tmdb_trending_latest`**  
Keeps the latest available record for each TMDB item and media type.

**`fct_tmdb_trending_top_items_daily`**  
Presents top-ranked trending items by day and media type for dashboard consumption.

### Testing

dbt tests are applied to both source and model layers, including:

- `not_null`
- `unique`
- unique combination tests using `dbt_utils`

## 5. Airflow Orchestration

Airflow orchestrates the end-to-end workflow through a local Docker-based environment.

### Airflow stack includes

- Postgres metadata database
- Airflow API server
- Airflow scheduler
- Airflow DAG processor
- custom Airflow image with project dependencies installed

### Current DAG

**`tmdb_pipeline_dag`**

Pipeline steps:

1. `ingest_trending_daily`
2. `load_trending_daily_to_snowflake`
3. `dbt_build_tmdb`

### Airflow implementation notes

- tasks are executed via `BashOperator`
- Airflow passes the run date into the pipeline using `{{ ds }}`
- execution API URL and shared secrets were configured to support successful UI-triggered runs
- the DAG has been validated both through Airflow test execution and through successful UI-triggered runs

## 6. Streamlit Dashboard

The Streamlit dashboard provides a front-end analytics layer on top of the Gold models.

### Current dashboard capabilities

- KPI cards
- date filter
- media type filter
- top 10 trending leaderboard
- popularity and vote charts
- latest trending snapshot view
- poster images for top-ranked items

### Dashboard data sources

- `FCT_TMDB_TRENDING_DAILY_SUMMARY`
- `FCT_TMDB_TRENDING_TOP_ITEMS_DAILY`
- `DIM_TMDB_TRENDING_LATEST`

## Key Engineering Features

This project currently demonstrates the following engineering capabilities:

- API ingestion with authenticated requests
- retry-aware data collection
- raw JSON preservation
- object storage partitioning
- idempotent warehouse loads
- operational audit logging
- semi-structured data handling in Snowflake
- dbt source modeling and transformations
- schema and data quality testing
- Airflow-based orchestration
- Streamlit-based consumption layer

## Environment Configuration

The project relies on environment variables for credentials and runtime configuration.

Typical categories include:

- TMDB credentials and API settings
- MinIO endpoint and access keys
- Snowflake account and warehouse credentials
- Airflow shared secret settings
- Streamlit Snowflake secrets

Secrets should not be committed to version control. Use local `.env`, `.env.example`, Docker Compose environment variable substitution, and Streamlit `secrets.toml` appropriately.

## Running the Project

The exact setup may vary by environment, but the general workflow is:

### 1. Start supporting services
- Start MinIO from the infrastructure stack
- Start Airflow from the Airflow stack

### 2. Run ingestion and warehouse loading manually if needed
- ingest TMDB data into MinIO
- load MinIO Bronze data into Snowflake Bronze

### 3. Run dbt transformations
- install packages with `dbt deps`
- run transformations with `dbt run`
- validate models with `dbt test`

### 4. Run the Streamlit dashboard
- start the app locally with Streamlit
- query Snowflake Gold models for the dashboard views

### 5. Trigger orchestration in Airflow
- trigger `tmdb_pipeline_dag`
- confirm all tasks succeed in the Airflow UI

## Current Project Status

The project currently includes a complete working vertical slice across all major layers:

- ingestion completed
- Bronze storage completed
- Snowflake Bronze loading completed
- dbt Silver and Gold models completed
- Airflow orchestration completed
- Streamlit dashboard completed

The repository is therefore already functional as an end-to-end analytics engineering project, with additional improvements possible in areas such as deployment, styling, extended metrics, and production hardening.

## Potential Next Improvements

Examples of next-stage enhancements include:

- deduplicating top-items ranking logic across same-day multiple runs
- expanding Gold models for richer time-series analytics
- improving dashboard styling and interactivity
- adding deployment workflows
- adding CI/CD checks
- tightening dependency compatibility warnings
- adding observability and alerting

## Tech Stack

- Python
- TMDB API
- MinIO
- Snowflake
- dbt
- Airflow
- Streamlit
- Docker Compose
- SQL
- pandas
- matplotlib

## Resume Summary

This project demonstrates practical experience in building a modern data platform from ingestion through analytics delivery, including object storage, cloud warehousing, transformation modeling, orchestration, testing, and dashboard development.