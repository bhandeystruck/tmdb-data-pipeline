"""
Simple MinIO read test.

Purpose:
- Verify we can read a JSON file back from MinIO
- Confirm payload structure and object metadata
"""

from ingestion.src.minio_reader import get_json


def main():
    key = "endpoint=trending/time_window=day/dt=2026-03-10/run_id=8f88b8cc-4ae6-4e6d-ae57-d6c51bb079ee.json"

    payload, response = get_json(key)

    print("object key       =", key)
    print("etag             =", response.get("ETag"))
    print("content length   =", response.get("ContentLength"))
    print("top-level keys   =", list(payload.keys()))

    if "results" in payload:
        print("results count    =", len(payload["results"]))


if __name__ == "__main__":
    main()