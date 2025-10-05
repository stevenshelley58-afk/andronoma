from __future__ import annotations

import json
import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sse_starlette.sse import EventSourceResponse

from shared.logs import broker
from shared.models import PipelineRun

from ..dependencies import get_current_user, get_db

router = APIRouter(prefix="/runs", tags=["logs"])


@router.get("/{run_id}/logs/stream")
async def stream_logs(
    run_id: uuid.UUID,
    user=Depends(get_current_user),
    session: AsyncSession = Depends(get_db),
):
    run = await session.get(PipelineRun, run_id)
    if not run or run.owner_id != user.id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")

    async def event_generator():
        async for event in broker.stream(run_id):
            yield {
                "event": event.get("level", "info"),
                "data": json.dumps(event),
            }

    return EventSourceResponse(event_generator())
