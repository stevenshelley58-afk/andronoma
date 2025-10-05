"""Database session helpers for both async and sync contexts."""
from __future__ import annotations

from contextlib import contextmanager
from typing import AsyncIterator, Iterator

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, sessionmaker

from .config import get_settings


settings = get_settings()

async_engine = create_async_engine(settings.database_url, echo=False, future=True)
AsyncSessionFactory = async_sessionmaker(async_engine, expire_on_commit=False)

sync_engine = create_engine(settings.sync_database_url, future=True)
SyncSessionFactory = sessionmaker(bind=sync_engine, expire_on_commit=False, future=True)


async def get_async_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency that yields an AsyncSession."""

    async with AsyncSessionFactory() as session:
        yield session


@contextmanager
def get_sync_session() -> Iterator[Session]:
    """Context manager for Celery tasks that need a blocking session."""

    with SyncSessionFactory() as session:
        yield session
