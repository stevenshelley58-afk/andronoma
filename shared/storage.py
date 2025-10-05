"""Utilities for interacting with MinIO/S3 storage."""
from __future__ import annotations

from typing import BinaryIO

from minio import Minio

from .config import get_settings


settings = get_settings()

client = Minio(
    endpoint=settings.minio_endpoint,
    access_key=settings.minio_access_key,
    secret_key=settings.minio_secret_key,
    secure=False,
)


def ensure_bucket() -> None:
    if not client.bucket_exists(settings.minio_bucket):
        client.make_bucket(settings.minio_bucket)


def put_object(key: str, data: BinaryIO, length: int, content_type: str = "application/octet-stream") -> str:
    ensure_bucket()
    client.put_object(settings.minio_bucket, key, data, length, content_type=content_type)
    return f"s3://{settings.minio_bucket}/{key}"
