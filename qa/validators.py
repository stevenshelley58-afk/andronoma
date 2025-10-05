"""Validators that power the QA automation stage."""
from __future__ import annotations

import csv
import re
from collections import Counter
from pathlib import Path
from typing import List, Mapping, Sequence

from .result import CheckResult, CheckSeverity


DISALLOWED_PROMO = ["% off", "discount", "sale", "limited offer"]
HEADLINE_MIN = 3
HEADLINE_MAX = 10
REQUIRED_AUDIENCE_COLUMNS = [
    "Audience Name",
    "Primary Motivation",
    "Top 2 Blockers",
]


def load_csv_records(path: Path) -> List[Mapping[str, str]]:
    """Load a CSV file into a list of stripped dictionaries."""

    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = []
        for row in reader:
            cleaned = {key: (value or "").strip() for key, value in row.items()}
            rows.append(cleaned)
        return rows


def check_headline_length(records: Sequence[Mapping[str, str]]) -> CheckResult:
    """Ensure all headlines have an acceptable word count."""

    violations = []
    for index, row in enumerate(records, start=2):
        headline = row.get("Headline", "")
        word_count = len([token for token in headline.split(" ") if token])
        if word_count < HEADLINE_MIN or word_count > HEADLINE_MAX:
            violations.append({"row": index, "word_count": word_count, "headline": headline})
    if violations:
        return CheckResult(
            name="Headline word count",
            kind="creative_headline_length",
            severity=CheckSeverity.BLOCKER,
            message=f"{len(violations)} headlines outside {HEADLINE_MIN}-{HEADLINE_MAX} word range",
            remediation="Revise highlighted headlines to meet the word-count constraints.",
            details={"violations": violations, "bounds": [HEADLINE_MIN, HEADLINE_MAX]},
        )
    return CheckResult(
        name="Headline word count",
        kind="creative_headline_length",
        severity=CheckSeverity.PASS,
        message="All headlines fall within the expected word-count range.",
    )


def check_cta_presence(records: Sequence[Mapping[str, str]]) -> CheckResult:
    """Ensure headlines communicate a clear call-to-action."""

    missing_cta = []
    pattern = re.compile(r"\b(Shop|See|Explore|Discover|Find|Get)\b.*", flags=re.IGNORECASE)
    for index, row in enumerate(records, start=2):
        joined = f"{row.get('Headline', '')} {row.get('Angle', '')}".strip()
        if not pattern.search(joined):
            missing_cta.append({"row": index, "headline": row.get("Headline", "")})
    if missing_cta:
        return CheckResult(
            name="CTA coverage",
            kind="creative_cta",
            severity=CheckSeverity.BLOCKER,
            message=f"{len(missing_cta)} creatives missing a clear CTA",
            remediation="Inject action-oriented phrasing (e.g. Shop, Discover, Get) into the affected copy.",
            details={"violations": missing_cta},
        )
    return CheckResult(
        name="CTA coverage",
        kind="creative_cta",
        severity=CheckSeverity.PASS,
        message="All creatives include a recognizable CTA cue.",
    )


def check_promo_language(records: Sequence[Mapping[str, str]]) -> CheckResult:
    """Guard against promotional phrasing that violates policy."""

    flagged = []
    for index, row in enumerate(records, start=2):
        head = row.get("Headline", "").lower()
        angle = row.get("Angle", "").lower()
        if any(token in head or token in angle for token in DISALLOWED_PROMO):
            flagged.append({"row": index, "headline": row.get("Headline", "")})
    if flagged:
        return CheckResult(
            name="Promo language",
            kind="creative_promo_language",
            severity=CheckSeverity.BLOCKER,
            message="Disallowed promo phrasing detected in creatives",
            remediation="Remove promotional discount claims from flagged rows before publishing.",
            details={"violations": flagged, "disallowed": DISALLOWED_PROMO},
        )
    return CheckResult(
        name="Promo language",
        kind="creative_promo_language",
        severity=CheckSeverity.PASS,
        message="No creatives contain disallowed promotional language.",
    )


def validate_audience_quotas(records: Sequence[Mapping[str, str]]) -> CheckResult:
    """Validate quota coverage for the generated audience table."""

    total = len(records)
    missing_columns = [col for col in REQUIRED_AUDIENCE_COLUMNS if records and col not in records[0]]
    incomplete_rows = []
    for index, row in enumerate(records, start=2):
        missing = [col for col in REQUIRED_AUDIENCE_COLUMNS if not row.get(col)]
        if missing:
            incomplete_rows.append({"row": index, "missing": missing})
    if total < 100:
        severity = CheckSeverity.BLOCKER
        message = f"Audience table below quota (found {total}, expected ≥ 100)"
        remediation = "Regenerate audiences to hit the minimum quota requirements."
    elif missing_columns:
        severity = CheckSeverity.BLOCKER
        message = "Audience table missing required columns"
        remediation = "Update pipeline mappings to output the full specification columns."
    elif incomplete_rows:
        severity = CheckSeverity.WARNING
        message = f"{len(incomplete_rows)} audience rows missing required attributes"
        remediation = "Fill in the highlighted columns to improve persona fidelity."
    else:
        severity = CheckSeverity.PASS
        message = "Audience quota requirements satisfied."
        remediation = ""
    return CheckResult(
        name="Audience quotas",
        kind="audience_quota",
        severity=severity,
        message=message,
        remediation=remediation,
        details={
            "total_records": total,
            "missing_columns": missing_columns,
            "incomplete_rows": incomplete_rows,
        },
    )


def validate_blocker_coverage(
    audiences: Sequence[Mapping[str, str]],
    creatives: Sequence[Mapping[str, str]],
) -> CheckResult:
    """Ensure blockers called out in personas have matching creative coverage."""

    audience_blockers: Counter[str] = Counter()
    for row in audiences:
        blockers = _split_multi_value(row.get("Top 2 Blockers", ""))
        audience_blockers.update(blockers)
    creative_blockers = {
        blocker.strip().lower()
        for row in creatives
        for blocker in _split_multi_value(row.get("Blocker", ""))
        if blocker
    }
    uncovered = [blocker for blocker in audience_blockers if blocker and blocker not in creative_blockers]
    if uncovered:
        return CheckResult(
            name="Blocker coverage",
            kind="blocker_coverage",
            severity=CheckSeverity.BLOCKER,
            message="Creatives missing coverage for audience blockers",
            remediation="Author creative variations that speak directly to the uncovered blockers.",
            details={"uncovered_blockers": sorted(uncovered)},
        )
    return CheckResult(
        name="Blocker coverage",
        kind="blocker_coverage",
        severity=CheckSeverity.PASS,
        message="All audience blockers have matching creative coverage.",
    )


def validate_naming_consistency(
    creatives: Sequence[Mapping[str, str]],
    audiences: Sequence[Mapping[str, str]] | None = None,
) -> CheckResult:
    """Confirm naming conventions stay aligned across artifacts."""

    audience_names = {row.get("Audience Name", "").strip() for row in audiences or []}
    mismatches = []
    for index, row in enumerate(creatives, start=2):
        fit = row.get("Audience Fit", "").strip()
        if audience_names and fit and fit not in audience_names:
            mismatches.append({"row": index, "audience_fit": fit})
    if mismatches:
        return CheckResult(
            name="Naming consistency",
            kind="naming_consistency",
            severity=CheckSeverity.WARNING,
            message="Audience Fit values in creatives do not align with audience names",
            remediation="Normalize creative targeting labels to match the audience table.",
            details={"mismatches": mismatches},
        )
    return CheckResult(
        name="Naming consistency",
        kind="naming_consistency",
        severity=CheckSeverity.PASS,
        message="Creative naming matches the audience roster.",
    )


def validate_duplicate_guard(records: Sequence[Mapping[str, str]]) -> CheckResult:
    """Detect duplicate headlines that could harm performance."""

    seen: Counter[str] = Counter()
    duplicates = []
    for index, row in enumerate(records, start=2):
        headline = row.get("Headline", "").strip()
        if not headline:
            continue
        seen[headline] += 1
        if seen[headline] > 1:
            duplicates.append({"row": index, "headline": headline})
    if duplicates:
        return CheckResult(
            name="Duplicate guard",
            kind="creative_duplicates",
            severity=CheckSeverity.BLOCKER,
            message="Duplicate headlines detected in scroll stoppers",
            remediation="Swap in fresh messaging to keep variations unique.",
            details={"duplicates": duplicates},
        )
    return CheckResult(
        name="Duplicate guard",
        kind="creative_duplicates",
        severity=CheckSeverity.PASS,
        message="No duplicate headlines detected.",
    )


def validate_image_legibility(images: Sequence[Path]) -> CheckResult:
    """Check that rendered image assets look usable on first pass."""

    if not images:
        return CheckResult(
            name="Image legibility",
            kind="image_legibility",
            severity=CheckSeverity.BLOCKER,
            message="No rendered images found",
            remediation="Re-run the image stage to produce the creative overlays.",
        )
    flagged = []
    for image in images:
        try:
            size = image.stat().st_size
        except FileNotFoundError:
            flagged.append({"file": image.name, "reason": "missing"})
            continue
        if size == 0:
            flagged.append({"file": image.name, "reason": "empty"})
        elif size < 4096:
            flagged.append({"file": image.name, "reason": "tiny", "bytes": size})
    if not flagged:
        return CheckResult(
            name="Image legibility",
            kind="image_legibility",
            severity=CheckSeverity.PASS,
            message="Images present with non-zero byte size checks.",
        )
    severity = CheckSeverity.BLOCKER if any(item["reason"] != "tiny" for item in flagged) else CheckSeverity.WARNING
    remediation = (
        "Regenerate missing/empty renders and ensure export quality thresholds."
        if severity is CheckSeverity.BLOCKER
        else "Review low-byte-size renders for potential legibility issues."
    )
    return CheckResult(
        name="Image legibility",
        kind="image_legibility",
        severity=severity,
        message="Image assets flagged for potential legibility issues",
        remediation=remediation,
        details={"flagged": flagged},
    )


def validate_budget_allocation(budgets: Mapping[str, float]) -> CheckResult:
    """Ensure pipeline budgets are configured with positive allocations."""

    if not budgets:
        return CheckResult(
            name="Budget configuration",
            kind="budget_configuration",
            severity=CheckSeverity.WARNING,
            message="No budgets configured for the run",
            remediation="Populate stage budgets on the run payload to enforce cost controls.",
        )
    negative = {stage: value for stage, value in budgets.items() if value is None or float(value) <= 0.0}
    required_stages = {"audiences", "creatives", "images", "qa"}
    missing = sorted(required_stages - set(budgets))
    if negative:
        return CheckResult(
            name="Budget configuration",
            kind="budget_configuration",
            severity=CheckSeverity.BLOCKER,
            message="Detected non-positive budget allocations",
            remediation="Update stage budgets to positive currency amounts before running the pipeline.",
            details={"invalid_budgets": negative},
        )
    if missing:
        return CheckResult(
            name="Budget configuration",
            kind="budget_configuration",
            severity=CheckSeverity.WARNING,
            message="Budget allocation missing for downstream stages",
            remediation="Provide budget entries for the listed stages to ensure guardrails.",
            details={"missing": missing},
        )
    return CheckResult(
        name="Budget configuration",
        kind="budget_configuration",
        severity=CheckSeverity.PASS,
        message="Budgets configured for key stages.",
    )


def validate_signed_url_ttl(telemetry: Mapping[str, object]) -> CheckResult:
    """Validate signed URL expiry metadata when available."""

    ttl = telemetry.get("signed_url_ttl_seconds") if telemetry else None
    expires_at = telemetry.get("signed_url_expires_at") if telemetry else None
    if ttl is None and expires_at is None:
        return CheckResult(
            name="Signed URL TTL",
            kind="signed_url_ttl",
            severity=CheckSeverity.WARNING,
            message="Signed URL metadata unavailable",
            remediation="Export stage should return `signed_url_ttl_seconds` for monitoring.",
        )
    if ttl is not None and isinstance(ttl, (int, float)) and ttl < 300:
        return CheckResult(
            name="Signed URL TTL",
            kind="signed_url_ttl",
            severity=CheckSeverity.WARNING,
            message=f"Signed URL TTL below recommended threshold ({ttl}s)",
            remediation="Extend TTL to keep assets downloadable for operators.",
            details={"ttl_seconds": ttl, "expires_at": expires_at},
        )
    return CheckResult(
        name="Signed URL TTL",
        kind="signed_url_ttl",
        severity=CheckSeverity.PASS,
        message="Signed URL TTL metadata present and within acceptable bounds.",
        details={"ttl_seconds": ttl, "expires_at": expires_at},
    )


def _split_multi_value(value: str) -> List[str]:
    tokens = [token.strip().lower() for token in re.split(r"[,/;]|\band\b", value) if token and token.strip()]
    return [token for token in tokens if token]

