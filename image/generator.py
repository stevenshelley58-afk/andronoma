"""Image generation stage."""
from __future__ import annotations

from typing import Dict

from shared.stages.base import BaseStage


class ImageStage(BaseStage):
    name = "images"

    def execute(self) -> Dict[str, int]:
        self.ensure_budget(50.0)
        return {"image_variations": 6, "style_guides": 2}
