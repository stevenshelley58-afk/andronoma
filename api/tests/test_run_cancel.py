from __future__ import annotations

import asyncio
import sys
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
from sqlalchemy.orm import selectinload

from api.dependencies import get_current_user, get_db
from api.main import app
from shared.models import (
    Base,
    PipelineRun,
    RunStatus,
    StageState,
    StageStatus,
    User,
)


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


@pytest.fixture
def client(session_factory: async_sessionmaker[AsyncSession]) -> TestClient:
    async def override_get_db() -> AsyncIterator[AsyncSession]:
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.pop(get_db, None)


async def seed_run(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    run_id: UUID,
    status: RunStatus,
    stages: list[tuple[str, StageStatus, datetime | None, datetime | None]],
    owner_email: str | None = None,
) -> None:
    async with session_factory() as session:
        user = await session.get(User, owner_id)
        if user is None:
            email = owner_email or f"{owner_id}@example.com"
            user = User(id=owner_id, email=email, password_hash="hash")
            session.add(user)

        now = datetime.utcnow() - timedelta(hours=1)
        run = PipelineRun(
            id=run_id,
            owner_id=owner_id,
            status=status,
            input_payload={},
            budgets={"scrape": 10.0},
            telemetry={},
            created_at=now,
            updated_at=now,
        )
        session.add(run)
        session.add_all(
            [
                StageState(
                    id=uuid4(),
                    run_id=run_id,
                    name=name,
                    status=stage_status,
                    started_at=started,
                    finished_at=finished,
                    telemetry={},
                )
                for name, stage_status, started, finished in stages
            ]
        )
        await session.commit()


def test_cancel_run_success(
    client: TestClient,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    owner_id = uuid4()
    run_id = uuid4()
    owner_email = f"{owner_id}@example.com"

    stage_started = datetime.utcnow() - timedelta(minutes=30)

    asyncio.run(
        seed_run(
            session_factory,
            owner_id=owner_id,
            run_id=run_id,
            status=RunStatus.RUNNING,
            stages=[
                ("scrape", StageStatus.PENDING, None, None),
                ("process", StageStatus.RUNNING, stage_started, None),
                ("qa", StageStatus.COMPLETED, stage_started, datetime.utcnow()),
            ],
            owner_email=owner_email,
        )
    )

    owner = User(id=owner_id, email=owner_email, password_hash="hash")
    app.dependency_overrides[get_current_user] = lambda: owner

    try:
        response = client.post(f"/runs/{run_id}/cancel")
        assert response.status_code == 200
        payload = response.json()
        assert payload["status"] == RunStatus.CANCELLED.value

        stages = {stage["name"]: stage for stage in payload["stages"]}
        assert stages["scrape"]["status"] == StageStatus.SKIPPED.value
        assert stages["scrape"]["finished_at"] is not None
        assert stages["process"]["status"] == StageStatus.SKIPPED.value
        assert stages["process"]["finished_at"] is not None
        assert stages["qa"]["status"] == StageStatus.COMPLETED.value

        async def fetch_run() -> PipelineRun | None:
            async with session_factory() as session:
                return await session.get(
                    PipelineRun,
                    run_id,
                    options=(selectinload(PipelineRun.stages),),
                )

        refreshed_run = asyncio.run(fetch_run())
        assert refreshed_run is not None
        assert refreshed_run.status == RunStatus.CANCELLED
        stage_states = {stage.name: stage for stage in refreshed_run.stages}
        assert stage_states["scrape"].status == StageStatus.SKIPPED
        assert stage_states["scrape"].finished_at is not None
        assert stage_states["process"].status == StageStatus.SKIPPED
        assert stage_states["process"].finished_at is not None
        assert stage_states["qa"].status == StageStatus.COMPLETED
    finally:
        app.dependency_overrides.pop(get_current_user, None)


def test_cancel_run_forbidden(
    client: TestClient,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    owner_id = uuid4()
    run_id = uuid4()
    owner_email = f"{owner_id}@example.com"

    asyncio.run(
        seed_run(
            session_factory,
            owner_id=owner_id,
            run_id=run_id,
            status=RunStatus.PENDING,
            stages=[("scrape", StageStatus.PENDING, None, None)],
            owner_email=owner_email,
        )
    )

    intruder = User(id=uuid4(), email="intruder@example.com", password_hash="hash")
    app.dependency_overrides[get_current_user] = lambda: intruder

    try:
        response = client.post(f"/runs/{run_id}/cancel")
        assert response.status_code == 403
    finally:
        app.dependency_overrides.pop(get_current_user, None)


def test_cancel_run_invalid_status(
    client: TestClient,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    owner_id = uuid4()
    run_id = uuid4()
    owner_email = f"{owner_id}@example.com"

    asyncio.run(
        seed_run(
            session_factory,
            owner_id=owner_id,
            run_id=run_id,
            status=RunStatus.COMPLETED,
            stages=[("scrape", StageStatus.COMPLETED, datetime.utcnow(), datetime.utcnow())],
            owner_email=owner_email,
        )
    )

    owner = User(id=owner_id, email=owner_email, password_hash="hash")
    app.dependency_overrides[get_current_user] = lambda: owner

    try:
        response = client.post(f"/runs/{run_id}/cancel")
        assert response.status_code == 400
        assert response.json()["detail"] == "Run cannot be cancelled in its current status"
    finally:
        app.dependency_overrides.pop(get_current_user, None)

