"""Non-LLM repair strategies for failed validation."""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional

from core.artifact_parser import parse_code_artifact, serialize_code_artifact
from core.validation.gate import QualityGate
from core.validation.types import ValidationReport


def repair_artifact(
    code_artifact: str,
    report: ValidationReport,
    *,
    fallback_generator: Optional[Callable[[str, str, Any], str]] = None,
    code_type: str = "",
    description: str = "",
    tool_stack: Any = None,
) -> tuple[str, str]:
    """
    Apply a repair strategy. Returns (new_artifact, strategy_name).
    """
    if fallback_generator and code_type and tool_stack:
        return fallback_generator(code_type, description, tool_stack), "fallback_generator"

    files = parse_code_artifact(code_artifact)
    if not files:
        return code_artifact, "none"

    error_files = {i.file for i in report.issues if i.file and i.severity.value == "error"}
    if error_files:
        for path in list(files.keys()):
            if path in error_files:
                del files[path]
        if files:
            return serialize_code_artifact(files), "drop_invalid_files"

    return code_artifact, "none"


async def validation_repair_loop(
    attempt: Any,
    gate: QualityGate,
    *,
    max_rounds: int = 2,
    code_type: str = "",
    description: str = "",
    tool_stack: Any = None,
    fallback_generator: Optional[Callable[[str, str, Any], str]] = None,
    on_repair: Optional[Callable[[str, str], None]] = None,
) -> tuple[Any, ValidationReport]:
    """
    builder → validator → repair → validator …
    Mutates attempt.code_artifact and attempt.success when repaired.
    """
    report = gate.validate_attempt(attempt, code_type=code_type)
    rounds = 0

    while not report.passed and rounds < max_rounds:
        new_artifact, strategy = repair_artifact(
            attempt.code_artifact,
            report,
            fallback_generator=fallback_generator,
            code_type=code_type,
            description=description,
            tool_stack=tool_stack,
        )
        if new_artifact == attempt.code_artifact and strategy == "none":
            break

        attempt.code_artifact = new_artifact
        if on_repair:
            on_repair(strategy, attempt.attempt_id)
        rounds += 1
        report = gate.validate_attempt(attempt, code_type=code_type)

    attempt.success = report.passed
    if not report.passed and attempt.error_message:
        attempt.error_message = f"Validation failed: {report.issues[0].message if report.issues else 'unknown'}"
    elif report.passed:
        attempt.error_message = ""
    return attempt, report
