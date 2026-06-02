"""Python/JS/HTML syntax checks using stdlib where possible."""

from __future__ import annotations

import ast
import json
import re
from pathlib import PurePosixPath
from typing import Dict, List

from core.validation.types import ValidationIssue, ValidationSeverity


def check_syntax(files: Dict[str, str]) -> List[ValidationIssue]:
    issues: List[ValidationIssue] = []
    for name, content in files.items():
        ext = PurePosixPath(name).suffix.lower()
        if ext == ".py":
            issues.extend(_check_python(name, content))
        elif ext == ".json":
            issues.extend(_check_json(name, content))
        elif ext in (".html", ".htm"):
            issues.extend(_check_html(name, content))
        elif ext in (".js", ".jsx", ".ts", ".tsx"):
            issues.extend(_check_js_basic(name, content))
    return issues


def _check_python(filename: str, content: str) -> List[ValidationIssue]:
    try:
        ast.parse(content, filename=filename)
        return []
    except SyntaxError as exc:
        return [
            ValidationIssue(
                checker="syntax",
                file=filename,
                message=f"Python syntax error: {exc.msg}",
                detail=f"line {exc.lineno}",
            )
        ]


def _check_json(filename: str, content: str) -> List[ValidationIssue]:
    try:
        json.loads(content)
        return []
    except json.JSONDecodeError as exc:
        return [
            ValidationIssue(
                checker="syntax",
                file=filename,
                message=f"Invalid JSON: {exc.msg}",
                detail=f"line {exc.lineno}",
            )
        ]


def _check_html(filename: str, content: str) -> List[ValidationIssue]:
    issues: List[ValidationIssue] = []
    if "<html" not in content.lower() and "<!doctype" not in content.lower():
        issues.append(
            ValidationIssue(
                checker="syntax",
                file=filename,
                message="HTML missing doctype or html root",
                severity=ValidationSeverity.WARNING,
            )
        )
    open_tags = len(re.findall(r"<([a-zA-Z][a-zA-Z0-9]*)", content))
    close_tags = len(re.findall(r"</([a-zA-Z][a-zA-Z0-9]*)", content))
    if open_tags > 0 and close_tags == 0 and "<body" in content.lower():
        issues.append(
            ValidationIssue(
                checker="syntax",
                file=filename,
                message="HTML may have unclosed tags",
                severity=ValidationSeverity.WARNING,
            )
        )
    return issues


def _check_js_basic(filename: str, content: str) -> List[ValidationIssue]:
    issues: List[ValidationIssue] = []
    if content.count("{") != content.count("}"):
        issues.append(
            ValidationIssue(
                checker="syntax",
                file=filename,
                message="Mismatched curly braces in JS/TS file",
            )
        )
    if content.count("(") != content.count(")"):
        issues.append(
            ValidationIssue(
                checker="syntax",
                file=filename,
                message="Mismatched parentheses in JS/TS file",
                severity=ValidationSeverity.WARNING,
            )
        )
    return issues
