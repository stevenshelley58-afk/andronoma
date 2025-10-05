"""Codex automation tasks for the standard feature/refactor pipeline."""
from __future__ import annotations

import uuid
from typing import Iterable, List

from celery import chain, shared_task
from celery.utils.log import get_task_logger
from sqlalchemy import select

from shared.db import get_sync_session
from shared.logs import emit_log
from shared.models import PipelineRun, RunStatus
from shared.pipeline import PIPELINE_ORDER
from workers.tasks import execute_pipeline_stage

logger = get_task_logger(__name__)


@shared_task(name="codex.pipeline.scrape")
def scrape_stage(run_id: str) -> str:
    """Execute the scrape stage for the provided run."""

    return execute_pipeline_stage(run_id, "scrape")


@shared_task(name="codex.pipeline.process")
def process_stage(run_id: str) -> str:
    """Execute the processing stage for the provided run."""

    return execute_pipeline_stage(run_id, "process")


@shared_task(name="codex.pipeline.audiences")
def audiences_stage(run_id: str) -> str:
    """Execute the audience generation stage for the provided run."""

    return execute_pipeline_stage(run_id, "audiences")


@shared_task(name="codex.pipeline.creatives")
def creatives_stage(run_id: str) -> str:
    """Execute the creative generation stage for the specified run."""

    return execute_pipeline_stage(run_id, "creatives")


@shared_task(name="codex.pipeline.images")
def images_stage(run_id: str) -> str:
    """Execute the image rendering stage for the given run."""

    return execute_pipeline_stage(run_id, "images")


@shared_task(name="codex.pipeline.qa")
def qa_stage(run_id: str) -> str:
    """Execute the QA stage for the provided run."""

    return execute_pipeline_stage(run_id, "qa")


@shared_task(name="codex.pipeline.export")
def export_stage(run_id: str) -> str:
    """Create export artifacts for the provided run."""

    return execute_pipeline_stage(run_id, "export")


STANDARD_SEQUENCE: List = [
    scrape_stage,
    process_stage,
    audiences_stage,
    creatives_stage,
    images_stage,
    qa_stage,
    export_stage,
]


def _immutable_signatures(run_id: str) -> Iterable:
    for task in STANDARD_SEQUENCE:
        yield task.si(run_id)


@shared_task(name="codex.standard.summary")
def summarize_standard_run(run_id: str) -> dict:
    """Log a completion summary once the standard pipeline sequence finishes."""

    run_uuid = uuid.UUID(run_id)
    stage_order = {name: index for index, name in enumerate(PIPELINE_ORDER)}

    with get_sync_session() as session:
        run = session.get(PipelineRun, run_uuid)
        if not run:
            raise ValueError(f"Run {run_id} not found")

        stages = sorted(
            run.stages,
            key=lambda state: stage_order.get(state.name, len(stage_order)),
        )
        stage_summaries = [
            {
                "name": state.name,
                "status": state.status.value,
                "started_at": state.started_at.isoformat() if state.started_at else None,
                "finished_at": state.finished_at.isoformat() if state.finished_at else None,
            }
            for state in stages
        ]

        payload = {
            "run_id": run_id,
            "status": run.status.value,
            "stages": stage_summaries,
        }
        emit_log(session, run.id, "Codex pipeline sequence completed", metadata=payload)
        return payload


@shared_task(name="codex.standard.schedule_build")
def schedule_standard_build(run_id: str) -> str:
    """Kick off the full feature/refactor batch for the given run."""

    workflow = chain(
        *_immutable_signatures(run_id),
        summarize_standard_run.si(run_id),
    )
    async_result = workflow.apply_async()
    logger.info(
        "Queued standard Codex build chain", extra={"run_id": run_id, "task_id": async_result.id}
    )
    return run_id


def _is_standard_batch(payload: dict) -> bool:
    batch = str(payload.get("codex_batch") or payload.get("batch") or "").strip().lower()
    explicit_hardening = payload.get("platform_hardening")
    if explicit_hardening is True:
        return False

    if not batch:
        return True

    return batch in {"feature", "refactor"}


@shared_task(name="codex.standard.nightly")
def launch_standard_nightly_builds() -> int:
    """Scan for pending runs and queue background builds overnight."""

    scheduled = 0
    with get_sync_session() as session:
        stmt = select(PipelineRun).where(PipelineRun.status == RunStatus.PENDING)
        for run in session.scalars(stmt):
            payload = dict(run.input_payload or {})
            if not _is_standard_batch(payload):
                continue

            schedule_standard_build.delay(str(run.id))
            run.status = RunStatus.RUNNING
            session.add(run)
            session.commit()
            emit_log(
                session,
                run.id,
                "Queued Codex build via nightly scheduler",
                metadata={"batch": payload.get("codex_batch") or payload.get("batch") or "standard"},
            )
            scheduled += 1

    logger.info("Nightly Codex scheduler queued %s runs", scheduled)
    return scheduled
