"""Automated QA gate implementations."""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional

from shared.stages.base import BaseStage

from qa.result import CheckResult, CheckSeverity
from qa.telemetry import increment_failure_metric, snapshot_failure_counts
from qa.validators import (
    check_cta_presence,
    check_headline_length,
    check_promo_language,
    load_csv_records,
    validate_audience_quotas,
    validate_blocker_coverage,
    validate_budget_allocation,
    validate_duplicate_guard,
    validate_image_legibility,
    validate_naming_consistency,
    validate_signed_url_ttl,
)


class QAStage(BaseStage):
    name = "qa"

    def execute(self) -> Dict[str, object]:
        self.ensure_budget(15.0)

        run_id = str(self.context.run.id)
        audience_path = self._resolve_artifact("audiences", "audiences_master.csv")
        creative_path = self._resolve_artifact("creatives", "scroll_stoppers.csv")
        image_dir = self._resolve_artifact("creatives", "images", is_dir=True)

        results: List[CheckResult] = []

        audience_records = []
        creative_records = []
        image_paths = []

        results.append(
            self._presence_check("Audience CSV presence", "audience_presence", audience_path)
        )
        if audience_path:
            audience_records = load_csv_records(audience_path)

        results.append(
            self._presence_check("Creative CSV presence", "creative_presence", creative_path)
        )
        if creative_path:
            creative_records = load_csv_records(creative_path)

        if image_dir and image_dir.is_dir():
            image_paths = [
                path
                for path in image_dir.iterdir()
                if path.is_file() and path.suffix.lower() in {".png", ".jpg", ".jpeg"}
            ]

        if creative_records:
            results.extend(
                [
                    check_headline_length(creative_records),
                    check_cta_presence(creative_records),
                    check_promo_language(creative_records),
                    validate_duplicate_guard(creative_records),
                    validate_naming_consistency(creative_records, audience_records),
                ]
            )

        if audience_records:
            results.append(validate_audience_quotas(audience_records))

        if audience_records and creative_records:
            results.append(validate_blocker_coverage(audience_records, creative_records))

        results.append(validate_image_legibility(image_paths))

        budgets = self.context.run.budgets or {}
        results.append(validate_budget_allocation(budgets))

        export_telemetry = (self.context.run.telemetry or {}).get("export", {})
        if not isinstance(export_telemetry, dict):
            export_telemetry = {}
        results.append(validate_signed_url_ttl(export_telemetry))

        failure_counter: Counter[str] = Counter()
        for result in results:
            if result.is_failure():
                increment_failure_metric(result.kind)
                failure_counter[result.kind] += 1

        summary_counts = {
            "total": len(results),
            "passes": sum(1 for result in results if not result.is_failure()),
            "warnings": sum(1 for result in results if result.severity is CheckSeverity.WARNING),
            "blockers": sum(1 for result in results if result.is_blocker()),
        }

        notes_lines = []
        for result in results:
            if result.is_failure():
                line = f"{result.name}: {result.message}"
                if result.remediation:
                    line += f" → {result.remediation}"
                notes_lines.append(line)
        notes = (
            "QA checks passed with no blockers."
            if not notes_lines
            else "\n".join(notes_lines)
        )

        telemetry: Dict[str, object] = {
            "run_id": run_id,
            "sources": {
                "audiences_csv": str(audience_path) if audience_path else None,
                "creatives_csv": str(creative_path) if creative_path else None,
                "images_dir": str(image_dir) if image_dir else None,
            },
            "checks": [result.to_dict() for result in results],
            "counts": summary_counts,
            "failure_breakdown": dict(failure_counter),
            "global_failure_metrics": snapshot_failure_counts(),
            "notes": notes,
        }

        report_dir = Path("qa_reports") / run_id
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / "summary.json"
        report_path.write_text(json.dumps(telemetry, indent=2), encoding="utf-8")
        telemetry["report_path"] = str(report_path)

        self._update_stage_notes(notes)

        if summary_counts["blockers"]:
            self._persist_intermediate_telemetry(telemetry)
            raise RuntimeError(notes)

        return telemetry

    def _resolve_artifact(self, *parts: str, is_dir: bool = False) -> Optional[Path]:
        base = Path("outputs")
        if not parts:
            return None
        run_specific = base / parts[0] / str(self.context.run.id) / Path(*parts[1:])
        if run_specific.exists() and (not is_dir or run_specific.is_dir()):
            return run_specific
        fallback = base / Path(*parts)
        if fallback.exists() and (not is_dir or fallback.is_dir()):
            return fallback
        return None

    def _presence_check(self, name: str, kind: str, path: Optional[Path]) -> CheckResult:
        if path is None:
            return CheckResult(
                name=name,
                kind=kind,
                severity=CheckSeverity.BLOCKER,
                message=f"Missing required artifact: {kind}",
                remediation="Ensure upstream stages write the expected artifact before QA.",
            )
        return CheckResult(
            name=name,
            kind=kind,
            severity=CheckSeverity.PASS,
            message=f"Artifact located at {path}",
        )

    def _update_stage_notes(self, notes: str) -> None:
        session = self.context.session
        state = next((stage for stage in self.context.run.stages if stage.name == self.name), None)
        if not state:
            return
        state.notes = notes
        session.commit()
        session.refresh(state)

    def _persist_intermediate_telemetry(self, telemetry: Dict[str, object]) -> None:
        session = self.context.session
        state = next((stage for stage in self.context.run.stages if stage.name == self.name), None)
        if not state:
            return
        state.telemetry = telemetry
        session.commit()
        session.refresh(state)
