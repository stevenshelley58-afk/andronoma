"""Scraping stage implementation."""
from __future__ import annotations

import io
import json
import uuid
from typing import Any, Dict

from shared.models import AssetRecord
from shared.stages.base import BaseStage
from shared.storage import put_object


class ScrapeStage(BaseStage):
    name = "scrape"

    def execute(self) -> Dict[str, Any]:
        self.ensure_budget(10.0)
        documents = [
            {"url": "https://example.com/article", "title": "Example Insight", "body": "Lorem ipsum"}
        ]
        payload = json.dumps(documents, indent=2).encode("utf-8")
        try:
            uri = put_object(
                f"scrape/{self.context.run.id}.json",
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
            asset_type="raw_documents",
            storage_key=uri,
            extra={"documents": len(documents)},
        )
        self.context.session.add(record)
        self.context.session.commit()
        return {"documents": len(documents), "sample_uri": uri}
