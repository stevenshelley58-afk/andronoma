"""Log utilities that keep both database and SSE clients in sync."""
from __future__ import annotations

import asyncio
import uuid
from typing import Any, AsyncIterator, Awaitable, Dict

from sqlalchemy.orm import Session

from .models import RunLog


class LogStreamBroker:
    """In-memory broker that fans out log entries to SSE consumers."""

    def __init__(self) -> None:
        self._queues: Dict[uuid.UUID, "asyncio.Queue[Dict[str, Any]]"] = {}
        self._lock = asyncio.Lock()

    async def _get_queue(self, run_id: uuid.UUID) -> "asyncio.Queue[Dict[str, Any]]":
        async with self._lock:
            if run_id not in self._queues:
                self._queues[run_id] = asyncio.Queue()
            return self._queues[run_id]

    async def publish(self, run_id: uuid.UUID, payload: Dict[str, Any]) -> None:
        queue = await self._get_queue(run_id)
        await queue.put(payload)

    async def stream(self, run_id: uuid.UUID) -> AsyncIterator[Dict[str, Any]]:
        queue = await self._get_queue(run_id)
        while True:
            item = await queue.get()
            yield item


broker = LogStreamBroker()


def _run_async(coro: Awaitable[None]) -> None:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(coro)
    else:
        loop.create_task(coro)


def emit_log(
    session: Session,
    run_id: uuid.UUID,
    message: str,
    *,
    level: str = "info",
    metadata: Dict[str, Any] | None = None,
) -> RunLog:
    """Persist a log entry and notify SSE listeners."""

    metadata = metadata or {}
    entry = RunLog(
        id=uuid.uuid4(),
        run_id=run_id,
        message=message,
        level=level,
        metadata=metadata,
    )
    session.add(entry)
    session.commit()
    session.refresh(entry)

    _run_async(
        broker.publish(
            run_id,
            {
                "id": str(entry.id),
                "run_id": str(run_id),
                "message": entry.message,
                "level": entry.level,
                "metadata": entry.metadata,
                "created_at": entry.created_at.isoformat(),
            },
        )
    )
    return entry
