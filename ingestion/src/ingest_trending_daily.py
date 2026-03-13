import uuid
from datetime import date
import argparse


from .logger import get_logger
from .config import validate_env
from .tmdb_client import tmdb_get
from .minio_writer import put_json
from .utils import resolve_dt

logger = get_logger("ingest_trending_daily")

def get_args():
    """
    Parse command-line arguments for the ingestion script.
    """
    parser = argparse.ArgumentParser(
        description="Ingest TMDB trending daily data into MinIO Bronze."
    )
    parser.add_argument(
        "--dt",
        required=False,
        help="Partition date to write in YYYY-MM-DD format",
    )
    return parser.parse_args()

def main():
    validate_env()

    run_id = str(uuid.uuid4())
    args = get_args()
    dt = resolve_dt(args.dt)

    logger.info("Fetching TMDB trending (all/day) ...")
    payload = tmdb_get("/trending/all/day", params={})

    key = f"endpoint=trending/time_window=day/dt={dt}/run_id={run_id}.json"
    uri = put_json(key, payload)

    count = len(payload.get("results", []))
    logger.info(f"Saved trending payload to {uri}")
    logger.info(f"HTTP 200 | results count: {count}")


if __name__ == "__main__":
    main()