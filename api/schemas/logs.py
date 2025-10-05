from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field


class RunLogEntry(BaseModel):
    id: UUID
    run_id: UUID
    created_at: datetime
    level: str
    message: str
    metadata: Dict[str, Any] = Field(default_factory=dict, alias="data")

    class Config:
        orm_mode = True
        allow_population_by_field_name = True


class RunLogListResponse(BaseModel):
    logs: List[RunLogEntry] = Field(default_factory=list)
    next_cursor: Optional[UUID] = None
