"""
TMDB API client.

Purpose:
- Centralize authenticated requests to TMDB
- Apply default language and region parameters
- Add retry behavior for temporary API/server failures
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import TMDB_BASE_URL, TMDB_READ_ACCESS_TOKEN, TMDB_LANGUAGE, TMDB_REGION


def _build_session() -> requests.Session:
    """
    Create a requests session with retry behavior for transient failures.
    """
    session = requests.Session()

    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


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
    merged.setdefault("language", TMDB_LANGUAGE)
    merged.setdefault("region", TMDB_REGION)

    session = _build_session()
    resp = session.get(url, headers=headers, params=merged, timeout=30)
    resp.raise_for_status()
    return resp.json()