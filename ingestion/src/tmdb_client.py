import requests

from .config import TMDB_BASE_URL, TMDB_READ_ACCESS_TOKEN, TMDB_LANGUAGE, TMDB_REGION


def tmdb_get(path: str, params: dict | None = None) -> dict:
    """
    Calls TMDB v3 endpoints using the v4 Read Access Token as a Bearer token.
    """
    url = TMDB_BASE_URL.rstrip("/") + "/" + path.lstrip("/")
    headers = {
        "Authorization": f"Bearer {TMDB_READ_ACCESS_TOKEN}",
        "Accept": "application/json",
    }

    merged = params.copy() if params else {}
    # Common defaults
    merged.setdefault("language", TMDB_LANGUAGE)
    merged.setdefault("region", TMDB_REGION)

    resp = requests.get(url, headers=headers, params=merged, timeout=30)
    resp.raise_for_status()
    return resp.json()