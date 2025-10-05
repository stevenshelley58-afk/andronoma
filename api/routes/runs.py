from __future__ import annotations

import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from shared.db import get_sync_session
from shared.models import (
    AssetRecord,
    PipelineRun,
    RunStatus,
    StageState,
    StageStatus,
    User,
    default_budgets,
)
from shared.pipeline import PIPELINE_ORDER

from ..dependencies import get_current_user, get_db
from ..schemas.assets import AssetListResponse, AssetRecordResponse
from ..schemas.runs import (
    RunBudgetUpdateRequest,
    RunCreateRequest,
    RunListResponse,
    RunResponse,
    StageTelemetry,
    StageUpdateRequest,
)
from ..services.pipeline import enqueue_pipeline, ensure_stage_records

router = APIRouter(prefix="/runs", tags=["runs"])


ALLOWED_STAGE_STATUS_TRANSITIONS: dict[StageStatus, set[StageStatus]] = {
    StageStatus.PENDING: {StageStatus.RUNNING, StageStatus.SKIPPED},
    StageStatus.RUNNING: {StageStatus.COMPLETED, StageStatus.FAILED, StageStatus.SKIPPED},
    StageStatus.FAILED: {StageStatus.RUNNING, StageStatus.SKIPPED},
    StageStatus.COMPLETED: set(),
    StageStatus.SKIPPED: set(),
}


def serialize_run(run: PipelineRun) -> RunResponse:
    return RunResponse(
        id=run.id,
        status=run.status,
        input_payload=run.input_payload,
        budgets=run.budgets,
        telemetry=run.telemetry,
        created_at=run.created_at,
        updated_at=run.updated_at,
        stages=[
            StageTelemetry(
                name=stage.name,
                status=stage.status,
                started_at=stage.started_at,
                finished_at=stage.finished_at,
                telemetry=stage.telemetry or {},
                budget_spent=stage.budget_spent,
                notes=stage.notes,
            )
            for stage in sorted(
                run.stages,
                key=lambda s: PIPELINE_ORDER.index(s.name) if s.name in PIPELINE_ORDER else len(PIPELINE_ORDER),
            )
        ],
    )


@router.get("", response_model=RunListResponse)
async def list_runs(
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RunListResponse:
    result = await session.execute(
        PipelineRun.__table__.select().where(PipelineRun.owner_id == current_user.id)
    )
    rows = result.fetchall()
    runs = []
    for row in rows:
        run = await session.get(PipelineRun, row.id)
        if run:
            runs.append(serialize_run(run))
    return RunListResponse(runs=runs)


@router.post("", response_model=RunResponse, status_code=status.HTTP_201_CREATED)
async def create_run(
    payload: RunCreateRequest,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RunResponse:
    run = PipelineRun(
        id=uuid.uuid4(),
        owner_id=current_user.id,
        status=RunStatus.PENDING,
        input_payload=payload.config.dict(),
        budgets=payload.budgets or default_budgets(),
        telemetry={},
    )
    session.add(run)
    await session.commit()
    await session.refresh(run)
    return serialize_run(run)


@router.patch("/{run_id}/budgets", response_model=RunResponse)
async def update_run_budgets(
    run_id: uuid.UUID,
    payload: RunBudgetUpdateRequest,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RunResponse:
    run = await session.get(PipelineRun, run_id)
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    if run.owner_id != current_user.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")

    now = datetime.utcnow()
    await session.execute(
        PipelineRun.__table__
        .update()
        .where(PipelineRun.id == run_id)
        .values(budgets=dict(payload.budgets), updated_at=now)
    )
    await session.commit()
    session.expire_all()

    result = await session.execute(
        select(PipelineRun)
        .options(selectinload(PipelineRun.stages))
        .where(PipelineRun.id == run_id)
    )
    refreshed_run = result.scalar_one_or_none()
    if not refreshed_run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    return serialize_run(refreshed_run)


@router.post("/{run_id}/start", response_model=RunResponse)
async def start_run(
    run_id: uuid.UUID,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RunResponse:
    run = await session.get(PipelineRun, run_id)
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    if run.owner_id != current_user.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")
    if run.status not in {RunStatus.PENDING, RunStatus.FAILED}:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Run already active")

    run.status = RunStatus.RUNNING
    run.updated_at = datetime.utcnow()
    await session.commit()
    await session.refresh(run)

    with get_sync_session() as sync_session:
        persistent_run = sync_session.get(PipelineRun, run.id)
        if persistent_run:
            ensure_stage_records(sync_session, persistent_run)
            enqueue_pipeline(persistent_run)

    return serialize_run(run)


@router.post("/{run_id}/cancel", response_model=RunResponse)
async def cancel_run(
    run_id: uuid.UUID,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RunResponse:
    run = await session.get(
        PipelineRun,
        run_id,
        options=(selectinload(PipelineRun.stages),),
    )
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    if run.owner_id != current_user.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")
    if run.status not in {RunStatus.PENDING, RunStatus.RUNNING}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Run cannot be cancelled in its current status",
        )

    now = datetime.utcnow()
    run.status = RunStatus.CANCELLED
    run.updated_at = now

    for stage in run.stages:
        if stage.status in {StageStatus.PENDING, StageStatus.RUNNING}:
            stage.status = StageStatus.SKIPPED
            stage.finished_at = now

    await session.commit()

    result = await session.execute(
        select(PipelineRun)
        .options(selectinload(PipelineRun.stages))
        .where(PipelineRun.id == run_id)
    )
    refreshed_run = result.scalar_one_or_none()
    if not refreshed_run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    return serialize_run(refreshed_run)


@router.get("/{run_id}", response_model=RunResponse)
async def get_run_detail(
    run_id: uuid.UUID,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RunResponse:
    result = await session.execute(
        select(PipelineRun)
        .options(selectinload(PipelineRun.stages))
        .where(PipelineRun.id == run_id)
    )
    run = result.scalar_one_or_none()
    if not run or run.owner_id != current_user.id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    return serialize_run(run)


@router.get("/{run_id}/assets", response_model=AssetListResponse)
async def list_run_assets(
    run_id: uuid.UUID,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> AssetListResponse:
    run = await session.get(PipelineRun, run_id)
    if not run or run.owner_id != current_user.id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")

    result = await session.execute(
        select(AssetRecord)
        .where(AssetRecord.run_id == run_id)
        .order_by(AssetRecord.created_at.desc(), AssetRecord.id.desc())
    )
    records = result.scalars().all()
    assets = [
        AssetRecordResponse(
            id=record.id,
            run_id=record.run_id,
            stage=record.stage,
            asset_type=record.asset_type,
            storage_key=record.storage_key,
            metadata=record.extra or {},
            created_at=record.created_at,
        )
        for record in records
    ]
    return AssetListResponse(assets=assets)


@router.patch("/{run_id}/stages/{stage_name}", response_model=StageTelemetry)
async def update_stage(
    run_id: uuid.UUID,
    stage_name: str,
    payload: StageUpdateRequest,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> StageTelemetry:
    run = await session.get(PipelineRun, run_id)
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    if run.owner_id != current_user.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")

    result = await session.execute(
        select(StageState).where(StageState.run_id == run.id, StageState.name == stage_name)
    )
    stage = result.scalar_one_or_none()
    if not stage:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Stage not found")

    updated = False

    if payload.notes is not None:
        stage.notes = payload.notes
        updated = True

    stage_status_changed = False
    new_stage_status: StageStatus | None = None

    if payload.status is not None:
        current_status = stage.status
        if payload.status != current_status:
            allowed = ALLOWED_STAGE_STATUS_TRANSITIONS.get(current_status, set())
            if payload.status not in allowed:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid status transition",
                )
            stage.status = payload.status
            now = datetime.utcnow()
            if payload.status == StageStatus.RUNNING and stage.started_at is None:
                stage.started_at = now
            if payload.status in {StageStatus.COMPLETED, StageStatus.FAILED, StageStatus.SKIPPED}:
                stage.finished_at = now
            stage_status_changed = True
            new_stage_status = payload.status
            updated = True

    if payload.telemetry is not None:
        if stage.telemetry is None:
            stage.telemetry = {}
        stage.telemetry.update(payload.telemetry)
        if run.telemetry is None:
            run.telemetry = {}
        stage_telemetry = run.telemetry.get(stage_name, {})
        stage_telemetry.update(payload.telemetry)
        run.telemetry[stage_name] = stage_telemetry
        updated = True

    if payload.budget_spent is not None:
        stage.budget_spent = payload.budget_spent
        updated = True

    if updated:
        now = datetime.utcnow()
        run.updated_at = now
        if stage_status_changed:
            await session.flush()
            if new_stage_status == StageStatus.FAILED:
                run.status = RunStatus.FAILED
            else:
                result = await session.execute(
                    select(StageState.status).where(StageState.run_id == run.id)
                )
                stage_statuses = set(result.scalars().all())
                if StageStatus.RUNNING in stage_statuses:
                    run.status = RunStatus.RUNNING
                elif stage_statuses and stage_statuses.issubset(
                    {StageStatus.COMPLETED, StageStatus.SKIPPED}
                ):
                    run.status = RunStatus.COMPLETED
        await session.commit()
        await session.refresh(stage)
        await session.refresh(run)
    else:
        await session.refresh(stage)

    return StageTelemetry(
        name=stage.name,
        status=stage.status,
        started_at=stage.started_at,
        finished_at=stage.finished_at,
        telemetry=stage.telemetry or {},
        budget_spent=stage.budget_spent,
        notes=stage.notes,
    )
