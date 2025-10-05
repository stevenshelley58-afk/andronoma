from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List
from uuid import UUID

from pydantic import BaseModel, Field


class AssetRecordResponse(BaseModel):
    id: UUID
    run_id: UUID
    stage: str
    asset_type: str
    storage_key: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime

    class Config:
        orm_mode = True


class AssetListResponse(BaseModel):
    assets: List[AssetRecordResponse] = Field(default_factory=list)
