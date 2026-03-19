"""
Airflow DAG for the TMDB pipeline.

Flow:
1. Ingest TMDB trending daily data into MinIO Bronze
2. Load MinIO Bronze objects into Snowflake Bronze
3. Run dbt models to build Silver and Gold layers
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


REPO_ROOT = "/opt/airflow/tmdb-data-pipeline"
DBT_PROJECT_DIR = f"{REPO_ROOT}/dbt/tmdb_dbt"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="tmdb_pipeline_dag",
    description="Ingest TMDB data to MinIO, load Snowflake Bronze, and build dbt Silver/Gold models.",
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["tmdb", "minio", "snowflake", "dbt"],
) as dag:

    ingest_trending_daily = BashOperator(
        task_id="ingest_trending_daily",
        cwd=REPO_ROOT,
        bash_command="python -m ingestion.src.ingest_trending_daily --dt {{ ds }}",
    )

    load_trending_daily_to_snowflake = BashOperator(
        task_id="load_trending_daily_to_snowflake",
        cwd=REPO_ROOT,
        bash_command="python -m ingestion.src.load_trending_daily_to_snowflake --dt {{ ds }}",
    )

    dbt_build_tmdb = BashOperator(
        task_id="dbt_build_tmdb",
        cwd=DBT_PROJECT_DIR,
        bash_command=(
            "dbt deps && "
            "dbt run --select stg_tmdb_trending_daily "
            "fct_tmdb_trending_daily_summary "
            "dim_tmdb_trending_latest "
            "fct_tmdb_trending_top_items_daily && "
            "dbt test --select stg_tmdb_trending_daily "
            "fct_tmdb_trending_daily_summary "
            "dim_tmdb_trending_latest "
            "fct_tmdb_trending_top_items_daily"
        ),
    )

    ingest_trending_daily >> load_trending_daily_to_snowflake >> dbt_build_tmdb