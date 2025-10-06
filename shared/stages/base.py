"""Common logic for pipeline stages."""
from __future__ import annotations

import abc
import datetime as dt
import uuid
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from ..logs import emit_log
from ..models import PipelineRun, StageState, StageStatus


class StageContext:
    """Container for the objects a stage needs to operate."""

    def __init__(self, session: Session, run: PipelineRun):
        self.session = session
        self.run = run


class BaseStage(abc.ABC):
    name: str

    def __init__(self, context: StageContext):
        self.context = context

    @abc.abstractmethod
    def execute(self) -> Dict[str, Any]:
        """Perform the stage-specific work and return telemetry details."""

    def ensure_budget(self, required: float) -> None:
        budgets = self.context.run.budgets or {}
        stage_budget = float(budgets.get(self.name, 0.0))
        if stage_budget < required:
            raise ValueError(
                f"Stage {self.name} exceeds allocated budget ({stage_budget} < {required})"
            )

    def update_state(
        self,
        status: StageStatus,
        telemetry: Optional[Dict[str, Any]] = None,
        notes: str | None = None,
    ) -> StageState:
        session = self.context.session
        state = next((s for s in self.context.run.stages if s.name == self.name), None)
        if not state:
            state = StageState(id=uuid.uuid4(), run_id=self.context.run.id, name=self.name)
            session.add(state)
            self.context.run.stages.append(state)
        now = dt.datetime.now(dt.UTC)
        if status == StageStatus.RUNNING:
            state.started_at = now
        if status in {StageStatus.COMPLETED, StageStatus.FAILED, StageStatus.SKIPPED}:
            state.finished_at = now
        state.status = status
        if telemetry:
            if not state.telemetry:
                state.telemetry = {}
            state.telemetry.update(telemetry)
        if notes:
            state.notes = notes
        session.commit()
        session.refresh(state)
        return state

    def run(self) -> StageState:
        session = self.context.session
        run = self.context.run

        emit_log(session, run.id, f"Starting stage: {self.name}")
        self.update_state(StageStatus.RUNNING)
        try:
            telemetry = self.execute()
        except Exception as exc:  # pragma: no cover - defensive
            self.update_state(StageStatus.FAILED, notes=str(exc))
            emit_log(
                session,
                run.id,
                f"Stage {self.name} failed",
                level="error",
                metadata={"error": str(exc)},
            )
            raise
        else:
            state = self.update_state(StageStatus.COMPLETED, telemetry=telemetry)
            if not run.telemetry:
                run.telemetry = {}
            run.telemetry[self.name] = telemetry
            session.commit()
            emit_log(
                session,
                run.id,
                f"Completed stage: {self.name}",
                metadata={"telemetry": telemetry},
            )
            return state
