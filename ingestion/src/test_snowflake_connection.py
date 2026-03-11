"""
Simple Snowflake connectivity test.

Purpose:
- Verify Python can connect to Snowflake
- Confirm the active user, role, warehouse, database, and schema
"""

from ingestion.src.snowflake_client import get_connection


def main():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                CURRENT_USER(),
                CURRENT_ROLE(),
                CURRENT_WAREHOUSE(),
                CURRENT_DATABASE(),
                CURRENT_SCHEMA()
            """
        )
        row = cur.fetchone()
        print("CURRENT_USER     =", row[0])
        print("CURRENT_ROLE     =", row[1])
        print("CURRENT_WAREHOUSE=", row[2])
        print("CURRENT_DATABASE =", row[3])
        print("CURRENT_SCHEMA   =", row[4])
    finally:
        conn.close()


if __name__ == "__main__":
    main()