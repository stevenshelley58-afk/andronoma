"""Database models that capture the pipeline lifecycle."""
from __future__ import annotations

import enum
from datetime import UTC, datetime
from typing import Dict

from sqlalchemy import JSON, Column, DateTime, Enum, Float, ForeignKey, String, Text
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base, relationship

from .config import get_settings


Base = declarative_base()
settings = get_settings()


class RunStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StageStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


def _utcnow() -> datetime:
    """Return a timezone-aware UTC timestamp."""

    return datetime.now(UTC)


class User(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True)
    email = Column(String(320), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    created_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    tokens = relationship("SessionToken", back_populates="user")


class SessionToken(Base):
    __tablename__ = "session_tokens"

    id = Column(UUID(as_uuid=True), primary_key=True)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    token = Column(String(128), nullable=False, unique=True, index=True)
    created_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    user = relationship("User", back_populates="tokens")


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id = Column(UUID(as_uuid=True), primary_key=True)
    owner_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    status = Column(Enum(RunStatus), default=RunStatus.PENDING, nullable=False)
    input_payload = Column(JSON, default=dict, nullable=False)
    budgets = Column(JSON, default=dict, nullable=False)
    telemetry = Column(MutableDict.as_mutable(JSON), default=dict, nullable=False)
    created_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    owner = relationship("User")
    stages = relationship("StageState", back_populates="run", cascade="all, delete-orphan")
    logs = relationship("RunLog", back_populates="run", cascade="all, delete-orphan")


class StageState(Base):
    __tablename__ = "stage_states"

    id = Column(UUID(as_uuid=True), primary_key=True)
    run_id = Column(UUID(as_uuid=True), ForeignKey("pipeline_runs.id"), nullable=False)
    name = Column(String(64), nullable=False)
    status = Column(Enum(StageStatus), default=StageStatus.PENDING, nullable=False)
    started_at = Column(DateTime(timezone=True))
    finished_at = Column(DateTime(timezone=True))
    telemetry = Column(MutableDict.as_mutable(JSON), default=dict, nullable=False)
    budget_spent = Column(Float, default=0.0, nullable=False)
    notes = Column(Text, default="")

    run = relationship("PipelineRun", back_populates="stages")


class RunLog(Base):
    __tablename__ = "run_logs"

    id = Column(UUID(as_uuid=True), primary_key=True)
    run_id = Column(UUID(as_uuid=True), ForeignKey("pipeline_runs.id"), nullable=False)
    created_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)
    level = Column(String(16), default="info", nullable=False)
    message = Column(Text, nullable=False)
    data = Column("metadata", JSON, default=dict, nullable=False)

    run = relationship("PipelineRun", back_populates="logs")


class AssetRecord(Base):
    __tablename__ = "asset_records"

    id = Column(UUID(as_uuid=True), primary_key=True)
    run_id = Column(UUID(as_uuid=True), ForeignKey("pipeline_runs.id"), nullable=False)
    stage = Column(String(64), nullable=False)
    asset_type = Column(String(32), nullable=False)
    storage_key = Column(String(512), nullable=False)
    extra = Column(JSON, default=dict, nullable=False)
    created_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    run = relationship("PipelineRun")


def default_budgets() -> Dict[str, float]:
    """Return a dictionary containing default per-stage budgets."""

    base = settings.budget_default
    return {
        "scrape": base * 0.1,
        "process": base * 0.1,
        "audiences": base * 0.2,
        "creatives": base * 0.2,
        "images": base * 0.2,
        "qa": base * 0.1,
        "export": base * 0.1,
    }
