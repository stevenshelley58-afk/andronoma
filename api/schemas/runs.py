from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, validator

from shared.models import RunStatus, StageStatus
from shared.pipeline import PIPELINE_ORDER


class PipelineConfig(BaseModel):
    name: str
    objectives: List[str] = Field(default_factory=list)
    target_markets: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class RunCreateRequest(BaseModel):
    config: PipelineConfig
    budgets: Dict[str, float] | None = None


class RunBudgetUpdateRequest(BaseModel):
    budgets: Dict[str, float]

    @validator("budgets")
    def validate_budgets(cls, value: Dict[str, float]) -> Dict[str, float]:
        allowed_stages = set(PIPELINE_ORDER)
        for stage, amount in value.items():
            if stage not in allowed_stages:
                raise ValueError(f"Unknown stage '{stage}'")
            if amount < 0:
                raise ValueError("Budget allocations must be non-negative")
        return value


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
