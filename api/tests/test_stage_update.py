from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
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
from shared.models import Base, PipelineRun, RunStatus, StageState, StageStatus, User


@dataclass
class SeededStage:
    owner_id: UUID
    run_id: UUID
    stage_id: UUID
    stage_name: str


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
def seeded_stage(session_factory: async_sessionmaker[AsyncSession]) -> SeededStage:
    owner_id = uuid4()
    run_id = uuid4()
    stage_id = uuid4()
    stage_name = "qa"

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
                telemetry={
                    stage_name: {"initial": True},
                    "other": {"untouched": True},
                },
            )
            stage = StageState(
                id=stage_id,
                run_id=run_id,
                name=stage_name,
                status=StageStatus.PENDING,
                telemetry={"existing": True},
                budget_spent=1.0,
                notes="",
            )
            session.add_all([user, run, stage])
            await session.commit()

    asyncio.run(seed())
    return SeededStage(
        owner_id=owner_id,
        run_id=run_id,
        stage_id=stage_id,
        stage_name=stage_name,
    )


@pytest.fixture
def client(session_factory: async_sessionmaker[AsyncSession]) -> TestClient:
    async def override_get_db() -> AsyncIterator[AsyncSession]:
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.pop(get_db, None)


def test_update_stage_notes(client: TestClient, seeded_stage: SeededStage) -> None:
    owner = User(
        id=seeded_stage.owner_id,
        email="owner@example.com",
        password_hash="hash",
    )
    app.dependency_overrides[get_current_user] = lambda: owner

    try:
        response = client.patch(
            f"/runs/{seeded_stage.run_id}/stages/{seeded_stage.stage_name}",
            json={"notes": "Ready for QA"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["notes"] == "Ready for QA"
        assert payload["status"] == StageStatus.PENDING.value
    finally:
        app.dependency_overrides.pop(get_current_user, None)


def test_update_stage_status_to_skipped(
    client: TestClient, seeded_stage: SeededStage
) -> None:
    owner = User(
        id=seeded_stage.owner_id,
        email="owner@example.com",
        password_hash="hash",
    )
    app.dependency_overrides[get_current_user] = lambda: owner

    try:
        response = client.patch(
            f"/runs/{seeded_stage.run_id}/stages/{seeded_stage.stage_name}",
            json={"status": StageStatus.SKIPPED.value},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["status"] == StageStatus.SKIPPED.value
        assert payload["finished_at"] is not None
    finally:
        app.dependency_overrides.pop(get_current_user, None)


def test_update_stage_telemetry_and_budget(
    client: TestClient,
    seeded_stage: SeededStage,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    owner = User(
        id=seeded_stage.owner_id,
        email="owner@example.com",
        password_hash="hash",
    )
    app.dependency_overrides[get_current_user] = lambda: owner

    try:
        response = client.patch(
            f"/runs/{seeded_stage.run_id}/stages/{seeded_stage.stage_name}",
            json={
                "telemetry": {"new_metric": 42},
                "budget_spent": 12.5,
            },
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["telemetry"] == {"existing": True, "new_metric": 42}
        assert payload["budget_spent"] == 12.5

        async def fetch_state() -> tuple[StageState | None, PipelineRun | None]:
            async with session_factory() as session:
                stage_state = await session.get(StageState, seeded_stage.stage_id)
                run_state = await session.get(PipelineRun, seeded_stage.run_id)
                return stage_state, run_state

        stage_state, run_state = asyncio.run(fetch_state())
        assert stage_state is not None
        assert stage_state.telemetry == {"existing": True, "new_metric": 42}
        assert stage_state.budget_spent == 12.5
        assert run_state is not None
        stage_telemetry = run_state.telemetry.get(seeded_stage.stage_name, {})
        assert stage_telemetry == {"initial": True, "new_metric": 42}
        assert run_state.telemetry["other"] == {"untouched": True}
    finally:
        app.dependency_overrides.pop(get_current_user, None)


def test_update_stage_telemetry_creates_run_entry(
    client: TestClient,
    seeded_stage: SeededStage,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    owner = User(
        id=seeded_stage.owner_id,
        email="owner@example.com",
        password_hash="hash",
    )
    app.dependency_overrides[get_current_user] = lambda: owner

    new_run_id = uuid4()
    new_stage_id = uuid4()
    new_stage_name = "marketing"

    async def seed_additional_stage() -> None:
        async with session_factory() as session:
            run = PipelineRun(
                id=new_run_id,
                owner_id=seeded_stage.owner_id,
                status=RunStatus.RUNNING,
                input_payload={},
                budgets={},
                telemetry={},
            )
            stage = StageState(
                id=new_stage_id,
                run_id=new_run_id,
                name=new_stage_name,
                status=StageStatus.PENDING,
                telemetry=None,
                budget_spent=0.0,
                notes="",
            )
            session.add_all([run, stage])
            await session.commit()

    asyncio.run(seed_additional_stage())

    try:
        response = client.patch(
            f"/runs/{new_run_id}/stages/{new_stage_name}",
            json={"telemetry": {"fresh": "value"}},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["telemetry"] == {"fresh": "value"}
        assert payload["budget_spent"] == 0.0

        async def fetch_state() -> tuple[StageState | None, PipelineRun | None]:
            async with session_factory() as session:
                stage_state = await session.get(StageState, new_stage_id)
                run_state = await session.get(PipelineRun, new_run_id)
                return stage_state, run_state

        stage_state, run_state = asyncio.run(fetch_state())
        assert stage_state is not None
        assert stage_state.telemetry == {"fresh": "value"}
        assert run_state is not None
        assert run_state.telemetry == {new_stage_name: {"fresh": "value"}}
    finally:
        app.dependency_overrides.pop(get_current_user, None)


def test_update_stage_telemetry_replaces_non_mapping_run_entry(
    client: TestClient,
    seeded_stage: SeededStage,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    owner = User(
        id=seeded_stage.owner_id,
        email="owner@example.com",
        password_hash="hash",
    )
    app.dependency_overrides[get_current_user] = lambda: owner

    run_id = uuid4()
    stage_id = uuid4()
    stage_name = "ads"

    async def seed_stage_with_non_mapping() -> None:
        async with session_factory() as session:
            run = PipelineRun(
                id=run_id,
                owner_id=seeded_stage.owner_id,
                status=RunStatus.RUNNING,
                input_payload={},
                budgets={},
                telemetry={stage_name: "unexpected", "other": {"keep": True}},
            )
            stage = StageState(
                id=stage_id,
                run_id=run_id,
                name=stage_name,
                status=StageStatus.PENDING,
                telemetry={},
                budget_spent=0.0,
                notes="",
            )
            session.add_all([run, stage])
            await session.commit()

    asyncio.run(seed_stage_with_non_mapping())

    try:
        response = client.patch(
            f"/runs/{run_id}/stages/{stage_name}",
            json={"telemetry": {"metric": 1}},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["telemetry"] == {"metric": 1}

        async def fetch_state() -> tuple[StageState | None, PipelineRun | None]:
            async with session_factory() as session:
                stage_state = await session.get(StageState, stage_id)
                run_state = await session.get(PipelineRun, run_id)
                return stage_state, run_state

        stage_state, run_state = asyncio.run(fetch_state())
        assert stage_state is not None
        assert stage_state.telemetry == {"metric": 1}
        assert run_state is not None
        assert run_state.telemetry[stage_name] == {"metric": 1}
        assert run_state.telemetry["other"] == {"keep": True}
    finally:
        app.dependency_overrides.pop(get_current_user, None)


def test_update_stage_rejects_negative_budget(
    client: TestClient,
    seeded_stage: SeededStage,
) -> None:
    owner = User(
        id=seeded_stage.owner_id,
        email="owner@example.com",
        password_hash="hash",
    )
    app.dependency_overrides[get_current_user] = lambda: owner

    try:
        response = client.patch(
            f"/runs/{seeded_stage.run_id}/stages/{seeded_stage.stage_name}",
            json={"budget_spent": -1},
        )
        assert response.status_code == 422
    finally:
        app.dependency_overrides.pop(get_current_user, None)


def test_update_stage_rejects_unauthorized_user(
    client: TestClient, seeded_stage: SeededStage
) -> None:
    other_user = User(id=uuid4(), email="other@example.com", password_hash="hash")
    app.dependency_overrides[get_current_user] = lambda: other_user

    try:
        response = client.patch(
            f"/runs/{seeded_stage.run_id}/stages/{seeded_stage.stage_name}",
            json={"notes": "Attempted update"},
        )
        assert response.status_code == 403
    finally:
        app.dependency_overrides.pop(get_current_user, None)


def test_update_stage_missing_stage(client: TestClient, seeded_stage: SeededStage) -> None:
    owner = User(
        id=seeded_stage.owner_id,
        email="owner@example.com",
        password_hash="hash",
    )
    app.dependency_overrides[get_current_user] = lambda: owner

    try:
        response = client.patch(
            f"/runs/{seeded_stage.run_id}/stages/does-not-exist",
            json={"notes": "Missing"},
        )
        assert response.status_code == 404
    finally:
        app.dependency_overrides.pop(get_current_user, None)
