from datetime import date
import hashlib
import json

def parse_key_metadata(key: str) -> dict:
    """
    Parse and validate metadata from a key like:
    endpoint=trending/time_window=day/dt=2026-03-10/run_id=<uuid>.json
    """
    parts = key.split("/")

    expected_prefixes = ["endpoint=", "time_window=", "dt=", "run_id="]
    if len(parts) != 4:
        raise ValueError(
            f"Invalid object key '{key}'. Expected 4 path parts: "
            "endpoint=.../time_window=.../dt=.../run_id=....json"
        )

    meta = {}

    for part, expected_prefix in zip(parts, expected_prefixes):
        if not part.startswith(expected_prefix):
            raise ValueError(
                f"Invalid object key '{key}'. Expected part starting with "
                f"'{expected_prefix}' but got '{part}'"
            )

        k, v = part.split("=", 1)
        meta[k] = v

    run_id = meta["run_id"]
    if not run_id.endswith(".json"):
        raise ValueError(
            f"Invalid object key '{key}'. Expected run_id part to end with '.json'"
        )

    run_id = run_id.replace(".json", "")

    try:
        dt = str(date.fromisoformat(meta["dt"]))
    except ValueError as e:
        raise ValueError(
            f"Invalid object key '{key}'. dt must be in YYYY-MM-DD format"
        ) from e

    if not run_id:
        raise ValueError(
            f"Invalid object key '{key}'. run_id cannot be empty"
        )

    return {
        "endpoint": meta["endpoint"],
        "time_window": meta["time_window"],
        "dt": dt,
        "run_id": run_id,
    }


def resolve_dt(dt_arg: str | None) -> str:
    """
    Resolve and validate the partition date.

    If no date is provided, use today's date.
    If a date is provided, validate that it matches YYYY-MM-DD.
    """
    if dt_arg is None:
        return str(date.today())
    
    try:
        return str(date.fromisoformat(dt_arg))
    except ValueError as e:
        raise ValueError(
            f"Invalid --dt value '{dt_arg}'. Expected format: YYYY-MM-DD"
        ) from e


def compute_payload_hash(payload: dict) -> str:
    """
    Create a stable hash of the JSON payload for dedup/debugging.
    """
    raw = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()