"""Codex automation tasks for the platform hardening batch."""
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
from workers.constants import PLATFORM_HARDENING_QUEUE
from workers.tasks import execute_pipeline_stage

logger = get_task_logger(__name__)


@shared_task(name="codex.scrape_stage_foundation")
def scrape_stage_foundation(run_id: str) -> str:
    """Execute the scrape stage foundation task for the provided run."""

    return execute_pipeline_stage(run_id, "scrape")


@shared_task(name="codex.processing_stage_implementation")
def processing_stage_implementation(run_id: str) -> str:
    """Run the processing stage implementation task for the provided run."""

    return execute_pipeline_stage(run_id, "process")


@shared_task(name="codex.audience_generation_stage")
def audience_generation_stage(run_id: str) -> str:
    """Generate the platform hardening audience stage outputs."""

    return execute_pipeline_stage(run_id, "audiences")


@shared_task(name="codex.creative_generation_stage")
def creative_generation_stage(run_id: str) -> str:
    """Execute the creative generation stage for the specified run."""

    return execute_pipeline_stage(run_id, "creatives")


@shared_task(name="codex.image_rendering_stage")
def image_rendering_stage(run_id: str) -> str:
    """Render platform hardening images for the given run."""

    return execute_pipeline_stage(run_id, "images")


@shared_task(name="codex.qa_stage")
def qa_stage(run_id: str) -> str:
    """Execute QA stage validation for the provided run."""

    return execute_pipeline_stage(run_id, "qa")


@shared_task(name="codex.export_stage")
def export_stage(run_id: str) -> str:
    """Create export artifacts for the provided run."""

    return execute_pipeline_stage(run_id, "export")


PLATFORM_HARDENING_SEQUENCE: List = [
    scrape_stage_foundation,
    processing_stage_implementation,
    audience_generation_stage,
    creative_generation_stage,
    image_rendering_stage,
    qa_stage,
    export_stage,
]


def _immutable_signatures(run_id: str) -> Iterable:
    for task in PLATFORM_HARDENING_SEQUENCE:
        yield task.si(run_id).set(
            queue=PLATFORM_HARDENING_QUEUE,
            routing_key=PLATFORM_HARDENING_QUEUE,
        )


@shared_task(name="codex.platform_hardening.summary")
def summarize_platform_hardening_run(run_id: str) -> dict:
    """Log a completion summary once the platform hardening sequence finishes."""

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
        emit_log(
            session,
            run.id,
            "Platform hardening sequence completed",
            metadata=payload,
        )
        return payload


@shared_task(name="codex.platform_hardening.schedule_build")
def schedule_platform_hardening_build(run_id: str) -> str:
    """Kick off the full platform hardening batch for the given run."""

    workflow = chain(
        *_immutable_signatures(run_id),
        summarize_platform_hardening_run.si(run_id).set(
            queue=PLATFORM_HARDENING_QUEUE,
            routing_key=PLATFORM_HARDENING_QUEUE,
        ),
    )
    async_result = workflow.apply_async()
    logger.info(
        "Queued platform hardening build chain", extra={"run_id": run_id, "task_id": async_result.id}
    )
    return run_id


def _is_platform_hardening(payload: dict) -> bool:
    batch = str(payload.get("codex_batch") or payload.get("batch") or "").strip().lower()
    explicit_flag = payload.get("platform_hardening")
    return explicit_flag is True or batch == "platform_hardening"


@shared_task(name="codex.platform_hardening.nightly")
def launch_platform_hardening_nightly_builds() -> int:
    """Scan for platform hardening runs and queue background builds overnight."""

    scheduled = 0
    with get_sync_session() as session:
        stmt = select(PipelineRun).where(PipelineRun.status == RunStatus.PENDING)
        for run in session.scalars(stmt):
            payload = dict(run.input_payload or {})
            if not _is_platform_hardening(payload):
                continue

            schedule_platform_hardening_build.delay(str(run.id))
            run.status = RunStatus.RUNNING
            session.add(run)
            session.commit()
            emit_log(
                session,
                run.id,
                "Queued platform hardening build via nightly scheduler",
                metadata={"batch": "platform_hardening"},
            )
            scheduled += 1

    logger.info("Nightly platform hardening scheduler queued %s runs", scheduled)
    return scheduled
