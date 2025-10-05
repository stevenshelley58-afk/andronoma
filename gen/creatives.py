"""Creative generation stage."""
from __future__ import annotations

from typing import Dict

from shared.stages.base import BaseStage


class CreativeStage(BaseStage):
    name = "creatives"

    def execute(self) -> Dict[str, int]:
        self.ensure_budget(40.0)
        return {"copy_variations": 5, "cta_tests": 2}
