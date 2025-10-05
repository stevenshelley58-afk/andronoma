from __future__ import annotations

import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from shared.db import get_sync_session
from shared.models import PipelineRun, RunStatus, User, default_budgets
from shared.pipeline import PIPELINE_ORDER

from ..dependencies import get_current_user, get_db
from ..schemas.runs import RunCreateRequest, RunListResponse, RunResponse, StageTelemetry
from ..services.pipeline import enqueue_pipeline, ensure_stage_records

router = APIRouter(prefix="/runs", tags=["runs"])


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
                telemetry=stage.telemetry,
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


@router.get("/{run_id}", response_model=RunResponse)
async def get_run_detail(
    run_id: uuid.UUID,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RunResponse:
    run = await session.get(PipelineRun, run_id)
    if not run or run.owner_id != current_user.id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    await session.refresh(run)
    return serialize_run(run)
