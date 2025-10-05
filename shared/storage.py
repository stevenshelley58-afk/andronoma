"""Utilities for interacting with MinIO/S3 storage."""
from __future__ import annotations

import io
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import BinaryIO, Optional

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


@dataclass
class SignedUpload:
    """Representation of an object uploaded to storage with a signed URL."""

    key: str
    size: int
    content_type: str
    signed_url: str
    expires_at: datetime


@dataclass
class SignedURL:
    """Represents a presigned GET URL for an object."""

    url: str
    expires_at: datetime


def presign_get_object(key: str, ttl_seconds: Optional[int] = None) -> SignedURL:
    """Generate a presigned download URL for an object stored in MinIO."""

    ensure_bucket()
    ttl = ttl_seconds or settings.export_bundle_ttl_seconds
    ttl = max(ttl, 1)
    expires_delta = timedelta(seconds=ttl)
    url = client.presigned_get_object(
        settings.minio_bucket,
        key,
        expires=expires_delta,
    )
    expires_at = datetime.now(timezone.utc) + expires_delta
    return SignedURL(url=url, expires_at=expires_at)


def upload_bytes(
    key: str,
    payload: bytes,
    content_type: str = "application/octet-stream",
    ttl_seconds: Optional[int] = None,
) -> SignedUpload:
    """Upload a payload to storage and return metadata with a presigned URL."""

    ensure_bucket()
    buffer = io.BytesIO(payload)
    size = len(payload)
    client.put_object(
        settings.minio_bucket,
        key,
        buffer,
        size,
        content_type=content_type,
    )
    signed = presign_get_object(key, ttl_seconds=ttl_seconds)
    return SignedUpload(
        key=key,
        size=size,
        content_type=content_type,
        signed_url=signed.url,
        expires_at=signed.expires_at,
    )
