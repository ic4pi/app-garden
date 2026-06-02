"""Compile-check Python sources."""

from __future__ import annotations

import py_compile
import tempfile
from pathlib import Path, PurePosixPath
from typing import Dict, List

from core.validation.types import ValidationIssue


def run_lint(files: Dict[str, str]) -> List[ValidationIssue]:
    issues: List[ValidationIssue] = []
    for name, content in files.items():
        if PurePosixPath(name).suffix.lower() != ".py":
            continue
        issues.extend(_py_compile_check(name, content))
    return issues


def _py_compile_check(filename: str, content: str) -> List[ValidationIssue]:
    try:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / Path(filename).name
            path.write_text(content, encoding="utf-8")
            py_compile.compile(str(path), doraise=True)
        return []
    except py_compile.PyCompileError as exc:
        return [
            ValidationIssue(
                checker="lint",
                file=filename,
                message="Python compile failed",
                detail=str(exc),
            )
        ]
    except Exception as exc:
        return [
            ValidationIssue(
                checker="lint",
                file=filename,
                message="Lint check error",
                detail=str(exc),
            )
        ]
