"""Telemetry helpers for QA instrumentation."""
from __future__ import annotations

from collections import Counter
from typing import Dict

try:  # pragma: no cover - optional dependency
    from prometheus_client import Counter as PrometheusCounter
except Exception:  # pragma: no cover - fallback when unavailable
    PrometheusCounter = None  # type: ignore[assignment]

_fallback_counts: Counter[str] = Counter()

if PrometheusCounter is not None:  # pragma: no branch - import guard
    _failure_counter = PrometheusCounter(
        "andronoma_qa_failures_total",
        "Total QA validation failures partitioned by kind.",
        ["kind"],
    )
else:  # pragma: no cover - exercised when prometheus_client absent
    _failure_counter = None


def increment_failure_metric(kind: str, count: int = 1) -> None:
    """Increment the QA failure metric for the supplied kind."""

    if not kind:
        return
    _fallback_counts[kind] += count
    if _failure_counter is not None:  # pragma: no cover - simple delegation
        _failure_counter.labels(kind=kind).inc(count)


def snapshot_failure_counts() -> Dict[str, int]:
    """Return a snapshot of the failure counters accumulated in-process."""

    return dict(_fallback_counts)

