"""Validate requirements.txt and package.json dependency declarations."""

from __future__ import annotations

import json
import re
from pathlib import PurePosixPath
from typing import Dict, List

from core.validation.types import ValidationIssue, ValidationSeverity

_REQ_LINE = re.compile(
    r"^[a-zA-Z0-9][a-zA-Z0-9._\-]*(\[[^\]]+\])?(\s*(==|>=|<=|~=|!=|>|<)\s*[^\s#]+)?\s*(#.*)?$"
)
_PKG_NAME = re.compile(r"^[a-zA-Z@][a-zA-Z0-9._\-/]*$")


def validate_dependencies(files: Dict[str, str]) -> List[ValidationIssue]:
    issues: List[ValidationIssue] = []
    for name, content in files.items():
        base = PurePosixPath(name).name.lower()
        if base == "requirements.txt":
            issues.extend(_check_requirements(name, content))
        elif base == "package.json":
            issues.extend(_check_package_json(name, content))
    return issues


def _check_requirements(filename: str, content: str) -> List[ValidationIssue]:
    issues: List[ValidationIssue] = []
    for i, line in enumerate(content.splitlines(), 1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or stripped.startswith("-"):
            continue
        if stripped.startswith("git+") or stripped.startswith("http"):
            continue
        if not _REQ_LINE.match(stripped):
            issues.append(
                ValidationIssue(
                    checker="dependencies",
                    file=filename,
                    message=f"Invalid requirements line {i}: {stripped[:80]}",
                    severity=ValidationSeverity.WARNING,
                )
            )
    return issues


def _check_package_json(filename: str, content: str) -> List[ValidationIssue]:
    issues: List[ValidationIssue] = []
    try:
        data = json.loads(content)
    except json.JSONDecodeError as exc:
        return [
            ValidationIssue(
                checker="dependencies",
                file=filename,
                message=f"package.json parse error: {exc.msg}",
            )
        ]

    for section in ("dependencies", "devDependencies"):
        deps = data.get(section) or {}
        if not isinstance(deps, dict):
            issues.append(
                ValidationIssue(
                    checker="dependencies",
                    file=filename,
                    message=f"{section} must be an object",
                )
            )
            continue
        for pkg, version in deps.items():
            if not _PKG_NAME.match(str(pkg)):
                issues.append(
                    ValidationIssue(
                        checker="dependencies",
                        file=filename,
                        message=f"Suspicious package name: {pkg}",
                        severity=ValidationSeverity.WARNING,
                    )
                )
            if version in ("", None):
                issues.append(
                    ValidationIssue(
                        checker="dependencies",
                        file=filename,
                        message=f"Empty version for {pkg}",
                    )
                )
    return issues
