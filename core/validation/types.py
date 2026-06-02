"""Validation result types."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ValidationSeverity(str, Enum):
    ERROR = "error"
    WARNING = "warning"


@dataclass
class ValidationIssue:
    checker: str
    message: str
    severity: ValidationSeverity = ValidationSeverity.ERROR
    file: str = ""
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "checker": self.checker,
            "message": self.message,
            "severity": self.severity.value,
            "file": self.file,
            "detail": self.detail[:500] if self.detail else "",
        }


@dataclass
class ValidationReport:
    attempt_id: str
    passed: bool
    issues: list[ValidationIssue] = field(default_factory=list)
    checks_run: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "attempt_id": self.attempt_id,
            "passed": self.passed,
            "issues": [i.to_dict() for i in self.issues],
            "checks_run": self.checks_run,
        }
