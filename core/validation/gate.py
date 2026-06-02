"""Orchestrates validation checks and repair loop metadata."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from core.artifact_parser import parse_code_artifact
from core.validation.dependency_validator import validate_dependencies
from core.validation.lint_runner import run_lint
from core.validation.syntax_checker import check_syntax
from core.validation.test_runner import run_tests
from core.validation.types import ValidationIssue, ValidationReport, ValidationSeverity


@dataclass
class QualityGate:
    """Runs syntax → lint → dependencies → tests on a code artifact."""

    require_tests: bool = False

    def validate_artifact(
        self,
        attempt_id: str,
        code_artifact: str,
        *,
        code_type: str = "",
    ) -> "ValidationReport":
        from core.validation.types import ValidationReport

        files = parse_code_artifact(code_artifact)
        checks_run: list[str] = []
        all_issues: list[ValidationIssue] = []

        if not files:
            return ValidationReport(
                attempt_id=attempt_id,
                passed=False,
                issues=[
                    ValidationIssue(
                        checker="artifact",
                        message="No parseable files in code artifact",
                    )
                ],
                checks_run=["artifact"],
            )

        for name, fn in (
            ("syntax", lambda: check_syntax(files)),
            ("lint", lambda: run_lint(files)),
            ("dependencies", lambda: validate_dependencies(files)),
            ("tests", lambda: run_tests(files, code_type=code_type)),
        ):
            checks_run.append(name)
            all_issues.extend(fn())

        errors = [i for i in all_issues if i.severity == ValidationSeverity.ERROR]
        passed = len(errors) == 0
        if self.require_tests and not any(i.checker == "tests" for i in all_issues):
            test_py = any(
                n.startswith("test_") and n.endswith(".py") for n in files
            )
            if test_py and passed:
                passed = False
                all_issues.append(
                    ValidationIssue(
                        checker="tests",
                        message="Test files present but tests did not run successfully",
                    )
                )

        return ValidationReport(
            attempt_id=attempt_id,
            passed=passed,
            issues=all_issues,
            checks_run=checks_run,
        )

    def validate_attempt(self, attempt: Any, *, code_type: str = "") -> ValidationReport:
        return self.validate_artifact(
            getattr(attempt, "attempt_id", "unknown"),
            getattr(attempt, "code_artifact", "") or "",
            code_type=code_type,
        )
