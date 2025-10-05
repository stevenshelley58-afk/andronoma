"""Celery tasks that execute each pipeline stage."""
from __future__ import annotations

import importlib
import uuid
from typing import Dict, Type

from celery import shared_task

from shared.db import get_sync_session
from shared.logs import emit_log
from shared.models import PipelineRun, RunStatus, StageState
from shared.stages.base import BaseStage, StageContext

STAGE_MODULES: Dict[str, str] = {
    "scrape": "scrape.service.ScrapeStage",
    "process": "nlp.pipeline.ProcessStage",
    "audiences": "nlp.pipeline.AudienceStage",
    "creatives": "gen.creatives.CreativeStage",
    "images": "image.generator.ImageStage",
    "qa": "qa.automation.checks.QAStage",
    "export": "export.manager.ExportStage",
}


def import_stage(stage_name: str) -> Type[BaseStage]:
    dotted = STAGE_MODULES[stage_name]
    module_name, class_name = dotted.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, class_name)


@shared_task(name="pipeline.run_stage")
def run_stage_task(run_id: str, stage_name: str) -> str:
    stage_cls = import_stage(stage_name)
    run_uuid = uuid.UUID(run_id)

    with get_sync_session() as session:
        run = session.get(PipelineRun, run_uuid)
        if not run:
            raise ValueError(f"Run {run_id} not found")

        session.refresh(run)
        state = next((s for s in run.stages if s.name == stage_name), None)
        if not state:
            state = StageState(id=uuid.uuid4(), run_id=run.id, name=stage_name)
            session.add(state)
            session.commit()
            session.refresh(state)
            session.refresh(run)

        context = StageContext(session=session, run=run)
        stage = stage_cls(context)

        try:
            stage.run()
        except Exception:  # pragma: no cover - defensive
            run.status = RunStatus.FAILED
            session.commit()
            raise

        if stage_name == "export":
            run.status = RunStatus.COMPLETED
        else:
            run.status = RunStatus.RUNNING
        session.commit()
        emit_log(session, run.id, f"Stage {stage_name} finished")

    return stage_name
