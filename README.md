# TMDB Data Engineering Pipeline

## Overview

This project is a local-first data engineering pipeline built around data from The Movie Database (TMDB) API. Its purpose is to ingest raw API responses, persist them in object storage, load them into a cloud data warehouse, and prepare them for downstream transformation and analytics.

The current pipeline flow is:

TMDB API → MinIO (Bronze JSON) → Snowflake (Bronze VARIANT) → dbt (planned Silver/Gold) → Airflow (planned orchestration) → dashboards (planned consumption)

At this stage, the project includes a working ingestion path from TMDB into MinIO, a working loader from MinIO into Snowflake Bronze, idempotent load behavior, audit logging in Snowflake, and date-driven script execution for controlled daily processing.

---

## Current Status

The following components are implemented and working:

- Repository structure initialized for infrastructure, ingestion, dbt, and Airflow
- Local MinIO environment running via Docker Compose
- Bronze bucket created and verified in MinIO
- Python ingestion environment created under `ingestion/.venv`
- TMDB API client implemented using Bearer token authentication
- Raw TMDB trending daily payloads written to MinIO as JSON
- Snowflake environment created for the pipeline
- Bronze raw table created in Snowflake using `VARIANT` for raw JSON storage
- OPS audit table created in Snowflake for load tracking
- Snowflake Python connector integrated into the ingestion layer
- MinIO reader implemented for listing and reading stored raw objects
- Loader script implemented to move raw objects from MinIO into Snowflake Bronze
- Idempotent load logic added to prevent duplicate object ingestion
- Audit logging added for `SUCCESS`, `SKIPPED`, and `FAILED` outcomes
- Both ingestion and loading scripts made date-driven with `--dt`
- Default local behavior added to use the current date when `--dt` is omitted

---

## Architecture

### End-to-End Flow

1. The ingestion script calls the TMDB API endpoint `/trending/all/day`
2. The raw JSON response is written to MinIO under a partitioned object key
3. The loader script reads raw objects from MinIO for a specific date partition
4. Each object is checked against Snowflake Bronze to avoid duplicate loading
5. New objects are inserted into a raw Bronze table in Snowflake
6. Each load attempt is recorded in an operational audit table

### Current Storage Layers

#### MinIO Bronze
Stores raw TMDB API responses as JSON files in object storage.

#### Snowflake Bronze
Stores one row per raw object with the full payload in a `VARIANT` column plus operational metadata.

#### Snowflake OPS
Stores audit records for each loader action, including success, skip, and failure outcomes.

---

## Repository Structure

```text
tmdb-data-pipeline/
├── airflow/
│   └── dags/
├── dbt/
├── infra/
│   └── docker-compose.yml
├── ingestion/
│   ├── .venv/
│   └── src/
│       ├── config.py
│       ├── ingest_trending_daily.py
│       ├── load_trending_daily_to_snowflake.py
│       ├── logger.py
│       ├── minio_reader.py
│       ├── minio_writer.py
│       ├── snowflake_client.py
│       ├── test_minio_read.py
│       ├── test_skip_if_loaded.py
│       ├── test_snowflake_connection.py
│       ├── test_insert_trending_raw.py
│       └── tmdb_client.py
├── .env
├── .gitignore
└── README.md

---

### Environment Configuration

A root-level .env file is used for all sensitive configuration. This file is ignored by Git.

TMDB settings
TMDB_BASE_URL
TMDB_READ_ACCESS_TOKEN
TMDB_LANGUAGE
TMDB_REGION

MinIO settings
MINIO_ENDPOINT
MINIO_ACCESS_KEY
MINIO_SECRET_KEY
MINIO_BRONZE_BUCKET
MINIO_REGION

Snowflake settings
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
SNOWFLAKE_SCHEMA
SNOWFLAKE_ROLE

The current config.py validates required environment variables before running ingestion or loading logic.

---

