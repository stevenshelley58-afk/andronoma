"""CLI hook that reuses the QA validator library."""
from __future__ import annotations

import sys
from pathlib import Path

from qa.result import CheckResult, CheckSeverity
from qa.validators import (
    check_cta_presence,
    check_headline_length,
    check_promo_language,
    load_csv_records,
    validate_image_legibility,
)


def run_checks(root: Path | None = None) -> list[CheckResult]:
    """Execute creative readiness checks and return detailed results."""

    root = root or Path(".")
    csv_path = root / "outputs" / "creatives" / "scroll_stoppers.csv"
    image_dir = root / "outputs" / "creatives" / "images"

    results: list[CheckResult] = []
    if csv_path.exists():
        records = load_csv_records(csv_path)
        results.extend(
            [
                check_headline_length(records),
                check_cta_presence(records),
                check_promo_language(records),
            ]
        )
    else:
        results.append(
            CheckResult(
                name="Creative CSV presence",
                kind="creative_presence",
                severity=CheckSeverity.BLOCKER,
                message="outputs/creatives/scroll_stoppers.csv missing",
                remediation="Run the creative generation stage before executing QA.",
            )
        )

    image_files = []
    if image_dir.exists():
        image_files = [
            path
            for path in image_dir.iterdir()
            if path.suffix.lower() in {".png", ".jpg", ".jpeg"}
        ]
    results.append(validate_image_legibility(image_files))
    return results


def main() -> None:
    results = run_checks()
    failures = [result for result in results if result.is_failure()]
    if failures:
        print("FAIL ad_readiness_check")
        for result in results:
            prefix = (
                "BLOCKER" if result.severity is CheckSeverity.BLOCKER else result.severity.value.upper()
            )
            print(f"- {prefix}: {result.message}")
        sys.exit(1)
    print("PASS ad_readiness_check")


if __name__ == "__main__":
    main()
