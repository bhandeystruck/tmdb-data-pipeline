import uuid
from datetime import datetime, timezone

from .logger import get_logger
from .config import validate_env
from .tmdb_client import tmdb_get
from .minio_writer import put_json

logger = get_logger("ingest_trending_daily")


def main():
    validate_env()

    run_id = str(uuid.uuid4())
    dt = datetime.now(timezone.utc).date().isoformat()

    logger.info("Fetching TMDB trending (all/day) ...")
    payload = tmdb_get("/trending/all/day", params={})

    key = f"endpoint=trending/time_window=day/dt={dt}/run_id={run_id}.json"
    uri = put_json(key, payload)

    count = len(payload.get("results", []))
    logger.info(f"Saved trending payload to {uri}")
    logger.info(f"HTTP 200 | results count: {count}")


if __name__ == "__main__":
    main()