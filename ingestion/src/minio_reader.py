"""
MinIO read helper.

Purpose:
- List raw objects from the Bronze bucket
- Read JSON files back from MinIO for downstream loading
"""

import json
import boto3

from ingestion.src import config


def get_s3_client():
    """
    Create and return an S3-compatible client for MinIO.
    """
    return boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT,
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY,
        region_name=config.MINIO_REGION,
    )


def list_objects(prefix: str) -> list[dict]:
    """
    List objects in the Bronze bucket under a given prefix.
    """
    s3 = get_s3_client()
    response = s3.list_objects_v2(
        Bucket=config.MINIO_BRONZE_BUCKET,
        Prefix=prefix,
    )
    return response.get("Contents", [])


def get_json(key: str) -> tuple[dict, dict]:
    """
    Read a JSON object from the Bronze bucket.

    Returns:
    - parsed JSON payload
    - raw S3/MinIO response metadata
    """
    s3 = get_s3_client()
    response = s3.get_object(
        Bucket=config.MINIO_BRONZE_BUCKET,
        Key=key,
    )
    content = response["Body"].read().decode("utf-8")
    return json.loads(content), response