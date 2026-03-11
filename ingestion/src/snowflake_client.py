"""
Snowflake connection helper.

Purpose:
- Centralize Snowflake connection creation in one place
- Reuse the same env-based config across loader scripts
- Keep connection logic separate from pipeline business logic
"""

import snowflake.connector

from ingestion.src import config


def get_connection():
    """
    Create and return a Snowflake connection using values from config.py.
    """
    return snowflake.connector.connect(
        account=config.SNOWFLAKE_ACCOUNT,
        user=config.SNOWFLAKE_USER,
        password=config.SNOWFLAKE_PASSWORD,
        warehouse=config.SNOWFLAKE_WAREHOUSE,
        database=config.SNOWFLAKE_DATABASE,
        schema=config.SNOWFLAKE_SCHEMA,
        role=config.SNOWFLAKE_ROLE,
    )