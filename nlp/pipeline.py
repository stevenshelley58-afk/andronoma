"""NLP enrichment stage."""
from __future__ import annotations

from typing import Dict

from shared.stages.base import BaseStage


class ProcessStage(BaseStage):
    name = "process"

    def execute(self) -> Dict[str, float]:
        self.ensure_budget(20.0)
        return {"topics_extracted": 12, "sentiment_score": 0.8}


class AudienceStage(BaseStage):
    name = "audiences"

    def execute(self) -> Dict[str, int]:
        self.ensure_budget(30.0)
        return {"segments": 4, "personas": 3}
