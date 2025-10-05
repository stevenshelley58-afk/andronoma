from __future__ import annotations

import uuid
from typing import Dict, List

from celery import chain
from sqlalchemy.orm import Session

from shared.models import PipelineRun, RunStatus, StageState
from shared.pipeline import PIPELINE_ORDER

from workers.tasks import run_stage_task


def enqueue_pipeline(run: PipelineRun) -> str:
    """Create a Celery workflow that processes all pipeline stages sequentially."""

    signatures = [run_stage_task.s(str(run.id), stage_name) for stage_name in PIPELINE_ORDER]
    workflow = chain(*signatures)
    result = workflow.apply_async()
    return result.id


def ensure_stage_records(session: Session, run: PipelineRun) -> None:
    existing_names = {stage.name for stage in run.stages}
    for stage_name in PIPELINE_ORDER:
        if stage_name not in existing_names:
            stage = StageState(id=uuid.uuid4(), run_id=run.id, name=stage_name)
            session.add(stage)
    session.commit()
