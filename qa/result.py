"""Shared result models for QA checks."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict


class CheckSeverity(str, Enum):
    """Severity levels produced by QA validators."""

    PASS = "pass"
    WARNING = "warning"
    BLOCKER = "blocker"


@dataclass(slots=True)
class CheckResult:
    """Container describing the outcome of a QA validation."""

    name: str
    kind: str
    severity: CheckSeverity
    message: str
    remediation: str = ""
    details: Dict[str, Any] = field(default_factory=dict)

    def is_failure(self) -> bool:
        """Return ``True`` when the check did not pass."""

        return self.severity is not CheckSeverity.PASS

    def is_blocker(self) -> bool:
        """Return ``True`` if the check represents a blocking issue."""

        return self.severity is CheckSeverity.BLOCKER

    def to_dict(self) -> Dict[str, Any]:
        """Serialize the result for telemetry emission."""

        payload: Dict[str, Any] = {
            "name": self.name,
            "kind": self.kind,
            "severity": self.severity.value,
            "message": self.message,
            "remediation": self.remediation,
        }
        if self.details:
            payload["details"] = self.details
        return payload

