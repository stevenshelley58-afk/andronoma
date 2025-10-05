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

from api.dependencies import get_current_user, get_db
from api.main import app
from shared.models import Base, PipelineRun, RunStatus, User


@dataclass
class SeededRun:
    owner_id: UUID
    run_id: UUID
    initial_updated_at: datetime


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
def seeded_run(session_factory: async_sessionmaker[AsyncSession]) -> SeededRun:
    owner_id = uuid4()
    run_id = uuid4()
    initial_updated_at = datetime.utcnow() - timedelta(hours=1)

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
                status=RunStatus.PENDING,
                input_payload={},
                budgets={"scrape": 10.0},
                telemetry={},
                created_at=initial_updated_at,
                updated_at=initial_updated_at,
            )
            session.add_all([user, run])
            await session.commit()

    asyncio.run(seed())
    return SeededRun(owner_id=owner_id, run_id=run_id, initial_updated_at=initial_updated_at)


@pytest.fixture
def client(session_factory: async_sessionmaker[AsyncSession]) -> TestClient:
    async def override_get_db() -> AsyncIterator[AsyncSession]:
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.pop(get_db, None)


def test_update_run_budgets_success(
    client: TestClient, session_factory: async_sessionmaker[AsyncSession], seeded_run: SeededRun
) -> None:
    owner = User(id=seeded_run.owner_id, email="owner@example.com", password_hash="hash")
    app.dependency_overrides[get_current_user] = lambda: owner

    try:
        new_budgets = {"scrape": 25.5, "process": 12.0}
        response = client.patch(
            f"/runs/{seeded_run.run_id}/budgets",
            json={"budgets": new_budgets},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["budgets"] == new_budgets

        async def fetch_run() -> PipelineRun | None:
            async with session_factory() as session:
                return await session.get(PipelineRun, seeded_run.run_id)

        refreshed_run = asyncio.run(fetch_run())
        assert refreshed_run is not None
        assert refreshed_run.budgets == new_budgets
        assert refreshed_run.updated_at > seeded_run.initial_updated_at
    finally:
        app.dependency_overrides.pop(get_current_user, None)


def test_update_run_budgets_rejects_negative_values(client: TestClient, seeded_run: SeededRun) -> None:
    owner = User(id=seeded_run.owner_id, email="owner@example.com", password_hash="hash")
    app.dependency_overrides[get_current_user] = lambda: owner

    try:
        response = client.patch(
            f"/runs/{seeded_run.run_id}/budgets",
            json={"budgets": {"scrape": -5}},
        )
        assert response.status_code == 422
    finally:
        app.dependency_overrides.pop(get_current_user, None)


def test_update_run_budgets_forbidden_for_other_user(
    client: TestClient, seeded_run: SeededRun
) -> None:
    other_user = User(id=uuid4(), email="intruder@example.com", password_hash="hash")
    app.dependency_overrides[get_current_user] = lambda: other_user

    try:
        response = client.patch(
            f"/runs/{seeded_run.run_id}/budgets",
            json={"budgets": {"scrape": 12}},
        )
        assert response.status_code == 403
    finally:
        app.dependency_overrides.pop(get_current_user, None)
