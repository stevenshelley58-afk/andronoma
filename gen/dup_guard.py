"""Duplicate guard utilities for creative generation."""
from __future__ import annotations

import difflib
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, MutableMapping


FIELDS = ("headline", "angle", "visual")


def _normalize(text: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", " ", text.lower())
    return " ".join(cleaned.split())


def _similarity(a: str, b: str) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a, b).ratio()


@dataclass
class DuplicateStats:
    """Aggregate statistics captured by the duplicate guard."""

    threshold: float
    evaluated: int = 0
    retained: int = 0
    suppressed: int = 0
    max_similarity_observed: float = 0.0
    collisions: List[Dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "threshold": self.threshold,
            "evaluated": self.evaluated,
            "retained": self.retained,
            "suppressed": self.suppressed,
            "max_similarity_observed": round(self.max_similarity_observed, 4),
            "collisions": self.collisions,
        }


class DuplicateGuard:
    """Similarity based guardrail to remove near-duplicate creative concepts."""

    def __init__(self, threshold: float = 0.94) -> None:
        self.threshold = threshold
        self._records: List[Dict[str, Any]] = []
        self._stats = DuplicateStats(threshold=threshold)

    def register(
        self,
        payload: Mapping[str, str],
        *,
        metadata: MutableMapping[str, Any] | None = None,
    ) -> bool:
        """Register a concept and return whether it should be kept.

        Args:
            payload: Mapping containing the creative fields to evaluate. The keys
                ``headline``, ``angle`` and ``visual`` are required.
            metadata: Optional context stored for diagnostics.

        Returns:
            ``True`` when the concept is accepted, ``False`` if it is considered a
            near-duplicate of an existing concept.
        """

        normalized = {field: _normalize(payload.get(field, "")) for field in FIELDS}
        raw_payload = {field: payload.get(field, "") for field in FIELDS}
        record_metadata: MutableMapping[str, Any] = metadata or {}

        self._stats.evaluated += 1

        for existing in self._records:
            existing_normalized = existing["normalized"]
            scores = {field: _similarity(normalized[field], existing_normalized[field]) for field in FIELDS}
            max_score = max(scores.values())
            self._stats.max_similarity_observed = max(self._stats.max_similarity_observed, max_score)
            if max_score >= self.threshold:
                self._stats.suppressed += 1
                collision_event = {
                    "candidate": {**raw_payload, "metadata": dict(record_metadata)},
                    "existing": existing["raw"],
                    "scores": {k: round(v, 4) for k, v in scores.items()},
                }
                self._stats.collisions.append(collision_event)
                return False

        self._stats.retained += 1
        stored = {
            "normalized": normalized,
            "raw": {**raw_payload, "metadata": dict(record_metadata)},
        }
        self._records.append(stored)
        return True

    def summary(self) -> Dict[str, Any]:
        """Return a JSON serialisable snapshot of the guard performance."""

        return self._stats.as_dict()


__all__ = ["DuplicateGuard", "DuplicateStats"]
