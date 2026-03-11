"""
Load TMDB trending daily raw files from MinIO into Snowflake Bronze.

Purpose:
- List raw trending/day objects from the Bronze bucket
- Skip objects that are already loaded
- Read JSON payloads from MinIO
- Insert one row per object into Snowflake Bronze
"""

import hashlib
import json
import argparse
from datetime import date
from ingestion.src.logger import get_logger

from ingestion.src.minio_reader import get_json, list_objects
from ingestion.src.snowflake_client import get_connection

logger = get_logger(__name__)

def compute_payload_hash(payload: dict) -> str:
    """
    Create a stable hash of the JSON payload for dedup/debugging.
    """
    raw = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def parse_key_metadata(key: str) -> dict:
    """
    Parse metadata from a key like:
    endpoint=trending/time_window=day/dt=2026-03-10/run_id=<uuid>.json
    """
    parts = key.split("/")
    meta = {}

    for part in parts:
        if "=" in part:
            k, v = part.split("=", 1)
            meta[k] = v

    return {
        "endpoint": meta["endpoint"],
        "time_window": meta["time_window"],
        "dt": meta["dt"],
        "run_id": meta["run_id"].replace(".json", ""),
    }


def already_loaded(cur, object_key: str) -> bool:
    """
    Return True if the given object_key already exists in the Bronze raw table.
    """
    cur.execute(
        """
        SELECT 1
        FROM TMDB_DB.BRONZE.TMDB_TRENDING_DAILY_RAW
        WHERE object_key = %s
        LIMIT 1
        """,
        (object_key,),
    )
    return cur.fetchone() is not None


def insert_raw_row(cur, key: str, payload: dict, response: dict) -> None:
    """
    Insert one raw MinIO object into the Snowflake Bronze raw table.
    """
    key_meta = parse_key_metadata(key)
    payload_hash = compute_payload_hash(payload)

    cur.execute(
        """
        INSERT INTO TMDB_DB.BRONZE.TMDB_TRENDING_DAILY_RAW (
            source_system,
            endpoint,
            time_window,
            object_key,
            object_etag,
            object_size,
            dt,
            run_id,
            payload,
            payload_hash
        )
        SELECT
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            PARSE_JSON(%s),
            %s
        """,
        (
            "tmdb",
            key_meta["endpoint"],
            key_meta["time_window"],
            key,
            response.get("ETag"),
            response.get("ContentLength"),
            key_meta["dt"],
            key_meta["run_id"],
            json.dumps(payload),
            payload_hash,
        ),
    )


def insert_audit_row(
        cur,
        pipeline_name: str,
        source_system: str,
        source_object_key: str,
        target_table: str,
        run_id: str | None,
        status: str,
        rows_loaded: int,
        error_message: str | None,
    ) -> None:
    """
    Insert one audit record into the OPS.LOAD_AUDIT table.
    """
    cur.execute(
        """
        INSERT INTO TMDB_DB.OPS.LOAD_AUDIT (
            pipeline_name,
            source_system,
            source_object_key,
            target_table,
            run_id,
            status,
            rows_loaded,
            error_message,
            started_at,
            finished_at
        )
        SELECT
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        """,
        (
            pipeline_name,
            source_system,
            source_object_key,
            target_table,
            run_id,
            status,
            rows_loaded,
            error_message,
        ),
    )


def get_args():
    """
    Parse command-line arguments for the loader.
    """
    parser = argparse.ArgumentParser(
        description="Load TMDB trending daily raw files from MinIO into Snowflake Bronze."
    )
    parser.add_argument(
        "--dt",
        required=False,
        help="Partition date to load in YYYY-MM-DD format",
    )
    return parser.parse_args()


def main():
    args = get_args()
    dt = args.dt or str(date.today())

    prefix = f"endpoint=trending/time_window=day/dt={dt}/"
    objects = list_objects(prefix)

    if not objects:
        logger.info("No objects found for prefix: %s", prefix)
        return

    conn = get_connection()

    try:
        cur = conn.cursor()

        loaded_count = 0
        skipped_count = 0
        failed_count = 0


        for obj in objects:
            key = obj["Key"]

            try:
                key_meta = parse_key_metadata(key)

                if already_loaded(cur, key):
                    logger.info("Skipping already loaded object: %s", key)

                    insert_audit_row(
                        cur=cur,
                        pipeline_name="load_trending_daily_to_snowflake",
                        source_system="tmdb",
                        source_object_key=key,
                        target_table="TMDB_DB.BRONZE.TMDB_TRENDING_DAILY_RAW",
                        run_id=key_meta["run_id"],
                        status="SKIPPED",
                        rows_loaded=0,
                        error_message=None,
                    )

                    skipped_count += 1
                    continue

                payload, response = get_json(key)
                insert_raw_row(cur, key, payload, response)

                insert_audit_row(
                    cur=cur,
                    pipeline_name="load_trending_daily_to_snowflake",
                    source_system="tmdb",
                    source_object_key=key,
                    target_table="TMDB_DB.BRONZE.TMDB_TRENDING_DAILY_RAW",
                    run_id=key_meta["run_id"],
                    status="SUCCESS",
                    rows_loaded=1,
                    error_message=None,
                )
                
                logger.info("Loaded object: %s", key)
                loaded_count += 1


            except Exception as e:

                logger.exception("Failed to process object: %s", key)
                print("Error:", str(e))

                run_id = None
                try:
                    run_id = parse_key_metadata(key)["run_id"]
                except Exception:
                    pass

                insert_audit_row(
                    cur=cur,
                    pipeline_name="load_trending_daily_to_snowflake",
                    source_system="tmdb",
                    source_object_key=key,
                    target_table="TMDB_DB.BRONZE.TMDB_TRENDING_DAILY_RAW",
                    run_id=run_id,
                    status="FAILED",
                    rows_loaded=0,
                    error_message=str(e),
                )

                failed_count += 1

        logger.info(
            "Finished. loaded=%s, skipped=%s, failed=%s",
            loaded_count,
            skipped_count,
            failed_count,
        )

    finally:
        conn.close()


if __name__ == "__main__":
    main()