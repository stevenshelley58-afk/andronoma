"""NLP enrichment stage."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

from outputs.csv import write_records

from shared.stages.base import BaseStage

from .audiences import generate_audience_plan


class ProcessStage(BaseStage):
    name = "process"

    def execute(self) -> Dict[str, float]:
        self.ensure_budget(20.0)
        return {"topics_extracted": 12, "sentiment_score": 0.8}


class AudienceStage(BaseStage):
    name = "audiences"

    def execute(self) -> Dict[str, object]:
        self.ensure_budget(300.0)

        processed_insights = self.context.run.telemetry.get("process", {}) if self.context.run.telemetry else {}
        result = generate_audience_plan(processed_insights, target_count=120)

        output_dir = Path("outputs/audiences")

        master_path = write_records(
            output_dir / "audiences_master.csv",
            [record.as_row() for record in result.records],
        )

        dedupe_path = write_records(output_dir / "dedupe_report.csv", result.suppressed)

        quota_gap_records = [
            {
                "quota": quota,
                "required": requirement,
                "actual": result.quota_counts.get(quota, 0),
                "deficit": result.quota_gaps.get(quota, 0),
            }
            for quota, requirement in result.quota_requirements.items()
        ]
        gap_path = write_records(output_dir / "quota_gaps.csv", quota_gap_records)

        persona_path = output_dir / "persona_clusters.json"
        persona_path.parent.mkdir(parents=True, exist_ok=True)
        persona_path.write_text(json.dumps(result.persona_clusters, indent=2))

        blocker_freq_path = write_records(
            output_dir / "blocker_frequency.csv",
            [
                {"blocker": blocker, "count": count}
                for blocker, count in sorted(result.blocker_frequency.items())
            ],
        )

        assignments_path = output_dir / "audience_assignments.json"
        assignments_path.write_text(json.dumps(result.assignments, indent=2))

        telemetry: Dict[str, object] = {
            "total_audiences": len(result.records),
            "quota_counts": result.quota_counts,
            "quota_requirements": result.quota_requirements,
            "quota_gaps": result.quota_gaps,
            "blocker_binding": result.blocker_binding_summary,
            "suppressed_count": len(result.suppressed),
            "suppressed_rows": result.suppressed,
            "output_paths": {
                "master": str(master_path),
                "dedupe": str(dedupe_path),
                "quota_gaps": str(gap_path),
            },
            "supporting_assets": {
                "persona_clusters": str(persona_path),
                "blocker_frequency": str(blocker_freq_path),
                "audience_assignments": str(assignments_path),
            },
        }

        if any(gap > 0 for gap in result.quota_gaps.values()):
            session = self.context.session
            state = next((s for s in self.context.run.stages if s.name == self.name), None)
            if state:
                existing = dict(state.telemetry or {})
                existing.update(telemetry)
                state.telemetry = existing
            run = self.context.run
            if not run.telemetry:
                run.telemetry = {}
            run.telemetry[self.name] = telemetry
            session.commit()
            raise ValueError("Audience quotas not satisfied; see quota_gaps.csv for deficits")

        return telemetry
