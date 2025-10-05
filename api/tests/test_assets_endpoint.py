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
from shared.models import AssetRecord, Base, PipelineRun, RunStatus, User


@dataclass
class SeededAssets:
    owner_id: UUID
    run_id: UUID
    asset_ids: list[UUID]


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
def seeded_assets(session_factory: async_sessionmaker[AsyncSession]) -> SeededAssets:
    owner_id = uuid4()
    run_id = uuid4()
    asset_ids = [uuid4(), uuid4(), uuid4()]
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
            assets = [
                AssetRecord(
                    id=asset_ids[0],
                    run_id=run_id,
                    stage="scrape",
                    asset_type="document",
                    storage_key="s3://bucket/doc-0",
                    extra={"index": 0, "quality": "draft"},
                    created_at=created,
                ),
                AssetRecord(
                    id=asset_ids[1],
                    run_id=run_id,
                    stage="process",
                    asset_type="dataset",
                    storage_key="s3://bucket/data-1",
                    extra={"index": 1, "quality": "refined"},
                    created_at=created + timedelta(seconds=1),
                ),
                AssetRecord(
                    id=asset_ids[2],
                    run_id=run_id,
                    stage="export",
                    asset_type="report",
                    storage_key="s3://bucket/report-2",
                    extra={"index": 2, "quality": "final"},
                    created_at=created + timedelta(seconds=2),
                ),
            ]
            session.add_all([user, run, *assets])
            await session.commit()

    asyncio.run(seed())
    return SeededAssets(owner_id=owner_id, run_id=run_id, asset_ids=asset_ids)


@pytest.fixture
def client(session_factory: async_sessionmaker[AsyncSession]) -> TestClient:
    async def override_get_db() -> AsyncIterator[AsyncSession]:
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.pop(get_db, None)


def test_owner_receives_assets(client: TestClient, seeded_assets: SeededAssets) -> None:
    owner = User(
        id=seeded_assets.owner_id,
        email="owner@example.com",
        password_hash="hash",
    )
    app.dependency_overrides[get_current_user] = lambda: owner

    try:
        response = client.get(f"/runs/{seeded_assets.run_id}/assets")
        assert response.status_code == 200
        payload = response.json()

        assets = payload["assets"]
        assert [asset["id"] for asset in assets] == [
            str(seeded_assets.asset_ids[2]),
            str(seeded_assets.asset_ids[1]),
            str(seeded_assets.asset_ids[0]),
        ]

        first = assets[0]
        assert first["stage"] == "export"
        assert first["asset_type"] == "report"
        assert first["storage_key"] == "s3://bucket/report-2"
        assert first["metadata"] == {"index": 2, "quality": "final"}
        assert first["created_at"] is not None
    finally:
        app.dependency_overrides.pop(get_current_user, None)


def test_non_owner_gets_not_found(client: TestClient, seeded_assets: SeededAssets) -> None:
    other_user = User(id=uuid4(), email="other@example.com", password_hash="hash")
    app.dependency_overrides[get_current_user] = lambda: other_user

    try:
        response = client.get(f"/runs/{seeded_assets.run_id}/assets")
        assert response.status_code == 404
    finally:
        app.dependency_overrides.pop(get_current_user, None)
