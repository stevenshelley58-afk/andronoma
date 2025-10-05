"""Application-wide configuration helpers."""
from __future__ import annotations

import os
from functools import lru_cache
from typing import Any, Dict

from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Central configuration loaded from environment variables.

    The values here power both the API surface and the Celery workers so we keep
    everything in a single object.  The defaults are intentionally conservative
    and assume local development using docker-compose or the helper scripts
    shipped with the repository.
    """

    api_host: str = Field("0.0.0.0", description="Bind address for FastAPI")
    api_port: int = Field(8001, description="Port for the HTTP API")
    database_url: str = Field(
        "postgresql+asyncpg://andronoma:andronoma@localhost:5432/andronoma",
        description="SQLAlchemy compatible database URL",
    )
    sync_database_url: str = Field(
        "postgresql+psycopg://andronoma:andronoma@localhost:5432/andronoma",
        description="Blocking SQLAlchemy database URL for Celery",
    )
    minio_endpoint: str = Field("localhost:9000", description="MinIO S3 endpoint")
    minio_access_key: str = Field("minio", description="MinIO access key")
    minio_secret_key: str = Field("miniopass", description="MinIO secret key")
    minio_bucket: str = Field("andronoma", description="Default bucket")
    broker_url: str = Field("redis://localhost:6379/0", description="Celery broker URL")
    result_backend: str = Field(
        "redis://localhost:6379/1", description="Celery result backend URL"
    )
    jwt_secret: str = Field("change-me", description="Secret used to sign auth tokens")
    jwt_algorithm: str = Field("HS256", description="Token signing algorithm")
    telemetry_namespace: str = Field("andronoma", description="Telemetry namespace")
    budget_default: float = Field(1000.0, description="Fallback campaign budget")

    class Config:
        env_prefix = "ANDRONOMA_"
        env_file = os.environ.get("ANDRONOMA_ENV_FILE", ".env")


@lru_cache()
def get_settings() -> Settings:
    """Return cached settings so modules can share the same instance."""

    return Settings()  # type: ignore[call-arg]


def settings_dict() -> Dict[str, Any]:
    """Expose settings as primitives for OpenAPI docs and the frontend."""

    settings = get_settings()
    return settings.dict()
