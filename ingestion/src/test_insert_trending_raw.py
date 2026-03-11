"""
One-row Snowflake insert test for TMDB trending raw data.

Purpose:
- Read one JSON object from MinIO
- Extract simple metadata from the object key
- Insert one raw row into Snowflake Bronze
"""

import hashlib
import json

from ingestion.src.minio_reader import get_json
from ingestion.src.snowflake_client import get_connection


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


def main():
    key = "endpoint=trending/time_window=day/dt=2026-03-10/run_id=8f88b8cc-4ae6-4e6d-ae57-d6c51bb079ee.json"

    payload, response = get_json(key)
    key_meta = parse_key_metadata(key)
    payload_hash = compute_payload_hash(payload)

    conn = get_connection()

    try:
        cur = conn.cursor()

        insert_sql = """
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
        """

        cur.execute(
            insert_sql,
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

        print("Inserted 1 row into TMDB_DB.BRONZE.TMDB_TRENDING_DAILY_RAW")

    finally:
        conn.close()


if __name__ == "__main__":
    main()