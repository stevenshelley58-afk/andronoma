from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, validator

from shared.models import RunStatus, StageStatus


class PipelineConfig(BaseModel):
    name: str
    objectives: List[str] = Field(default_factory=list)
    target_markets: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class RunCreateRequest(BaseModel):
    config: PipelineConfig
    budgets: Dict[str, float] | None = None


class StageTelemetry(BaseModel):
    name: str
    status: StageStatus
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    telemetry: Dict[str, Any] = Field(default_factory=dict)
    notes: str = ""

    class Config:
        use_enum_values = True


class StageUpdateRequest(BaseModel):
    notes: str | None = None
    status: StageStatus | None = None

    @validator("notes")
    def notes_must_not_be_empty(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("Notes cannot be empty")
        return value


class RunResponse(BaseModel):
    id: UUID
    status: RunStatus
    input_payload: Dict[str, Any]
    budgets: Dict[str, float]
    telemetry: Dict[str, Any]
    created_at: datetime
    updated_at: datetime
    stages: List[StageTelemetry] = Field(default_factory=list)

    class Config:
        use_enum_values = True


class RunListResponse(BaseModel):
    runs: List[RunResponse]
