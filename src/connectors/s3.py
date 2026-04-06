from __future__ import annotations

import json
import os
from typing import Any
from pathlib import Path

import boto3
import botocore


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def get_s3_client():
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    return boto3.client("s3", region_name=region)


def build_s3_key(source: str, file_name: str) -> str:
    bucket_prefix = _get_required_env("TMDB_S3_PREFIX").strip("/")
    return f"{bucket_prefix}/{source}/{file_name}"


def upload_raw_file(local_path: str, source: str) -> str:
    bucket = _get_required_env("TMDB_S3_BUCKET")
    file_name = Path(local_path).name
    s3_key = build_s3_key(source=source, file_name=file_name)

    client = get_s3_client()
    client.upload_file(local_path, bucket, s3_key)

    return f"s3://{bucket}/{s3_key}"


def put_json_payload(payload: Any, source: str, file_name: str) -> str:
    bucket = _get_required_env("TMDB_S3_BUCKET")
    s3_key = build_s3_key(source=source, file_name=file_name)

    client = get_s3_client()
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    client.put_object(Body=body, Bucket=bucket, Key=s3_key)

    return f"s3://{bucket}/{s3_key}"


def s3_object_exists(source: str, file_name: str) -> bool:
    bucket = _get_required_env("TMDB_S3_BUCKET")
    s3_key = build_s3_key(source=source, file_name=file_name)
    client = get_s3_client()

    try:
        client.head_object(Bucket=bucket, Key=s3_key)
        return True
    except botocore.exceptions.ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise