from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import AsyncIterator
from uuid import UUID, uuid4

if sys.version_info >= (3, 12):
    from typing import ForwardRef

    _orig_evaluate = ForwardRef._evaluate

    def _evaluate_with_guard(
        self: ForwardRef,
        globalns: dict | None,
        localns: dict | None,
        type_params=None,
        *,
        recursive_guard=None,
    ):  # type: ignore[override]
        if recursive_guard is None:
            recursive_guard = set()
        return _orig_evaluate(
            self,
            globalns,
            localns,
            type_params,
            recursive_guard=recursive_guard,
        )

    ForwardRef._evaluate = _evaluate_with_guard  # type: ignore[assignment]

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from api.main import app
from api.dependencies import get_current_user, get_db
from shared.models import Base, PipelineRun, RunLog, RunStatus, User


@dataclass
class SeededRun:
    owner_id: UUID
    run_id: UUID
    log_ids: list[UUID]


@pytest.fixture(scope="session")
def session_factory() -> async_sessionmaker[AsyncSession]:
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    async def prepare_schema() -> None:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    asyncio.run(prepare_schema())
    factory = async_sessionmaker(engine, expire_on_commit=False)

    yield factory

    asyncio.run(engine.dispose())


@pytest.fixture(scope="session")
def seeded_data(session_factory: async_sessionmaker[AsyncSession]) -> SeededRun:
    owner_id = uuid4()
    run_id = uuid4()
    log_ids = [uuid4(), uuid4(), uuid4()]
    created = datetime.utcnow()

    async def seed() -> None:
        async with session_factory() as session:
            user = User(
                id=owner_id,
                email="owner@example.com",
                password_hash="hash",
            )
            run = PipelineRun(
                id=run_id,
                owner_id=owner_id,
                status=RunStatus.RUNNING,
                input_payload={},
                budgets={},
                telemetry={},
            )
            logs = [
                RunLog(
                    id=log_ids[0],
                    run_id=run_id,
                    created_at=created,
                    level="info",
                    message="First log",
                    data={"index": 0},
                ),
                RunLog(
                    id=log_ids[1],
                    run_id=run_id,
                    created_at=created + timedelta(seconds=1),
                    level="warning",
                    message="Second log",
                    data={"index": 1},
                ),
                RunLog(
                    id=log_ids[2],
                    run_id=run_id,
                    created_at=created + timedelta(seconds=2),
                    level="error",
                    message="Third log",
                    data={"index": 2},
                ),
            ]
            session.add_all([user, run, *logs])
            await session.commit()

    asyncio.run(seed())
    return SeededRun(owner_id=owner_id, run_id=run_id, log_ids=log_ids)


@pytest.fixture
def client(session_factory: async_sessionmaker[AsyncSession]) -> TestClient:
    async def override_get_db() -> AsyncIterator[AsyncSession]:
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.pop(get_db, None)


def test_logs_history_happy_path(client: TestClient, seeded_data: SeededRun) -> None:
    owner = User(
        id=seeded_data.owner_id,
        email="owner@example.com",
        password_hash="hash",
    )
    app.dependency_overrides[get_current_user] = lambda: owner

    try:
        run_id = seeded_data.run_id
        log_ids = seeded_data.log_ids

        response = client.get(f"/runs/{run_id}/logs", params={"limit": 2})
        assert response.status_code == 200
        payload = response.json()

        assert [entry["id"] for entry in payload["logs"]] == [str(log_ids[0]), str(log_ids[1])]
        assert payload["next_cursor"] == str(log_ids[1])

        after = payload["logs"][-1]["id"]
        response = client.get(
            f"/runs/{run_id}/logs",
            params={"after": after, "limit": 2},
        )
        assert response.status_code == 200
        payload = response.json()

        assert [entry["id"] for entry in payload["logs"]] == [str(log_ids[2])]
        assert payload["next_cursor"] is None
    finally:
        app.dependency_overrides.pop(get_current_user, None)


def test_logs_history_requires_authentication(
    client: TestClient, seeded_data: SeededRun
) -> None:
    run_id = seeded_data.run_id
    response = client.get(f"/runs/{run_id}/logs")
    assert response.status_code == 401


def test_logs_history_not_owner(client: TestClient, seeded_data: SeededRun) -> None:
    other_user = User(id=uuid4(), email="other@example.com", password_hash="hash")
    app.dependency_overrides[get_current_user] = lambda: other_user

    try:
        run_id = seeded_data.run_id
        response = client.get(f"/runs/{run_id}/logs")
        assert response.status_code == 404
    finally:
        app.dependency_overrides.pop(get_current_user, None)
