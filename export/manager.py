"""Export stage that packages final campaign assets."""
from __future__ import annotations

import io
import json
import uuid
from typing import Dict

from shared.models import AssetRecord
from shared.stages.base import BaseStage
from shared.storage import put_object


class ExportStage(BaseStage):
    name = "export"

    def execute(self) -> Dict[str, str]:
        self.ensure_budget(20.0)
        manifest = {
            "run_id": str(self.context.run.id),
            "stages": [stage.name for stage in self.context.run.stages],
        }
        payload = json.dumps(manifest, indent=2).encode("utf-8")
        uri: str
        try:
            uri = put_object(
                f"exports/{self.context.run.id}.json",
                io.BytesIO(payload),
                length=len(payload),
                content_type="application/json",
            )
        except Exception as exc:  # pragma: no cover - optional
            uri = f"minio-unavailable://{exc}"
        record = AssetRecord(
            id=uuid.uuid4(),
            run_id=self.context.run.id,
            stage=self.name,
            asset_type="manifest",
            storage_key=uri,
            extra={"size": len(payload)},
        )
        self.context.session.add(record)
        self.context.session.commit()
        return {"export_package": uri}
