"""
Check whether a MinIO object has already been loaded into Snowflake Bronze.

Purpose:
- Verify idempotency logic before building the full loader
- Return True if the object_key already exists in the raw Bronze table
"""

from ingestion.src.snowflake_client import get_connection


def already_loaded(object_key: str) -> bool:
    """
    Return True if the given object_key already exists in the Bronze raw table.
    """
    conn = get_connection()

    try:
        cur = conn.cursor()
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
    finally:
        conn.close()


def main():
    key = "endpoint=trending/time_window=day/dt=2026-03-10/run_id=8f88b8cc-4ae6-4e6d-ae57-d6c51bb079ee.json"
    print("already_loaded =", already_loaded(key))


if __name__ == "__main__":
    main()