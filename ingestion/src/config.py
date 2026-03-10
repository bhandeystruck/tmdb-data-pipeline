import os
from dotenv import load_dotenv

# Load .env from repo root
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

TMDB_BASE_URL = os.getenv("TMDB_BASE_URL", "https://api.themoviedb.org/3")
TMDB_READ_ACCESS_TOKEN = os.getenv("TMDB_READ_ACCESS_TOKEN")
TMDB_LANGUAGE = os.getenv("TMDB_LANGUAGE", "en-US")
TMDB_REGION = os.getenv("TMDB_REGION", "US")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BRONZE_BUCKET = os.getenv("MINIO_BRONZE_BUCKET", "tmdb-bronze")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")


def _require(name: str, value: str | None) -> str:
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def validate_env() -> None:
    _require("TMDB_READ_ACCESS_TOKEN", TMDB_READ_ACCESS_TOKEN)
    _require("MINIO_ACCESS_KEY", MINIO_ACCESS_KEY)
    _require("MINIO_SECRET_KEY", MINIO_SECRET_KEY)