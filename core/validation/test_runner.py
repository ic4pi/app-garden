"""Optional pytest collect / npm dry-run when tooling is available."""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from pathlib import Path, PurePosixPath
from typing import Dict, List

from core.validation.types import ValidationIssue, ValidationSeverity


def run_tests(files: Dict[str, str], *, code_type: str = "") -> List[ValidationIssue]:
    issues: List[ValidationIssue] = []
    has_py = any(PurePosixPath(n).suffix.lower() == ".py" for n in files)
    has_pkg = "package.json" in {PurePosixPath(n).name for n in files}

    if has_py:
        issues.extend(_pytest_collect(files))
    if has_pkg:
        issues.extend(_npm_validate(files))
    return issues


def _pytest_collect(files: Dict[str, str]) -> List[ValidationIssue]:
    if not shutil.which("python3") and not shutil.which("python"):
        return []

    test_files = [
        n for n in files if PurePosixPath(n).name.startswith("test_") and n.endswith(".py")
    ]
    if not test_files:
        return []

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for name, content in files.items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")

        python = shutil.which("python3") or shutil.which("python")
        try:
            proc = subprocess.run(
                [python, "-m", "pytest", "--collect-only", "-q", str(root)],
                capture_output=True,
                text=True,
                timeout=30,
                cwd=root,
            )
            if proc.returncode != 0 and "No module named pytest" in (proc.stderr or ""):
                return [
                    ValidationIssue(
                        checker="tests",
                        message="Tests present but pytest not installed (skipped)",
                        severity=ValidationSeverity.WARNING,
                    )
                ]
            if proc.returncode != 0:
                return [
                    ValidationIssue(
                        checker="tests",
                        message="pytest collection failed",
                        detail=(proc.stderr or proc.stdout)[:800],
                    )
                ]
        except subprocess.TimeoutExpired:
            return [
                ValidationIssue(
                    checker="tests",
                    message="pytest collect timed out",
                    severity=ValidationSeverity.WARNING,
                )
            ]
        except FileNotFoundError:
            pass
    return []


def _npm_validate(files: Dict[str, str]) -> List[ValidationIssue]:
    if not shutil.which("npm"):
        return [
            ValidationIssue(
                checker="tests",
                message="package.json present but npm not available (skipped)",
                severity=ValidationSeverity.WARNING,
            )
        ]

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for name, content in files.items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")

        try:
            proc = subprocess.run(
                ["npm", "run", "build", "--if-present"],
                capture_output=True,
                text=True,
                timeout=120,
                cwd=root,
            )
            if proc.returncode != 0:
                detail = (proc.stderr or proc.stdout)[:800]
                if "Missing script" in detail or "ENOENT" in detail:
                    return []
                return [
                    ValidationIssue(
                        checker="tests",
                        message="npm build failed",
                        detail=detail,
                    )
                ]
        except subprocess.TimeoutExpired:
            return [
                ValidationIssue(
                    checker="tests",
                    message="npm build timed out",
                    severity=ValidationSeverity.WARNING,
                )
            ]
    return []
