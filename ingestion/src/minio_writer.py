import json
from io import BytesIO
from typing import Any

import boto3
from botocore.client import Config

from .config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BRONZE_BUCKET, MINIO_REGION


def _client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION,
        config=Config(signature_version="s3v4"),
    )


def put_json(key: str, payload: Any) -> str:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    bio = BytesIO(body)

    s3 = _client()
    s3.put_object(
        Bucket=MINIO_BRONZE_BUCKET,
        Key=key,
        Body=bio,
        ContentType="application/json",
    )
    return f"s3://{MINIO_BRONZE_BUCKET}/{key}"