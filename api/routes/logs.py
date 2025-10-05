from __future__ import annotations

import json
import uuid

from fastapi import APIRouter, Depends
from sse_starlette.sse import EventSourceResponse

from shared.logs import broker

from ..dependencies import get_current_user

router = APIRouter(prefix="/runs", tags=["logs"])


@router.get("/{run_id}/logs/stream")
async def stream_logs(run_id: uuid.UUID, user=Depends(get_current_user)):
    async def event_generator():
        async for event in broker.stream(run_id):
            yield {
                "event": event.get("level", "info"),
                "data": json.dumps(event),
            }

    return EventSourceResponse(event_generator())
