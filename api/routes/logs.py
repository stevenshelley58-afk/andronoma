from __future__ import annotations

import json
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sse_starlette.sse import EventSourceResponse

from shared.logs import broker
from shared.models import PipelineRun, RunLog

from ..dependencies import get_current_user, get_db
from ..schemas.logs import RunLogEntry, RunLogListResponse

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


@router.get("/{run_id}/logs", response_model=RunLogListResponse)
async def list_logs(
    run_id: uuid.UUID,
    limit: int = Query(100, ge=1, le=500),
    after: uuid.UUID | None = Query(None),
    user=Depends(get_current_user),
    session: AsyncSession = Depends(get_db),
):
    run = await session.get(PipelineRun, run_id)
    if not run or run.owner_id != user.id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")

    query = select(RunLog).where(RunLog.run_id == run_id)

    if after is not None:
        cursor = await session.get(RunLog, after)
        if not cursor or cursor.run_id != run_id:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cursor not found")

        query = query.where(
            or_(
                RunLog.created_at > cursor.created_at,
                and_(
                    RunLog.created_at == cursor.created_at,
                    RunLog.id > cursor.id,
                ),
            )
        )

    query = query.order_by(RunLog.created_at.asc(), RunLog.id.asc()).limit(limit)
    result = await session.execute(query)
    logs = result.scalars().all()

    entries = [RunLogEntry.from_orm(row) for row in logs]
    next_cursor = logs[-1].id if logs and len(logs) == limit else None

    return RunLogListResponse(logs=entries, next_cursor=next_cursor)
