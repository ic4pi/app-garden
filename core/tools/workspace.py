"""Per-attempt sandbox workspace on disk."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Dict, Optional

from core.artifact_parser import parse_code_artifact, serialize_code_artifact
from core.config import Config


class BuildWorkspace:
    """Isolated directory tree for one builder attempt."""

    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.src = self.root / "src"
        self.src.mkdir(parents=True, exist_ok=True)

    @classmethod
    def for_attempt(cls, build_id: str, attempt_id: str) -> "BuildWorkspace":
        base = Path(Config.WORKSPACES_DIR)
        return cls(base / build_id / attempt_id)

    def resolve_safe(self, relative_path: str) -> Path:
        """Resolve a path inside the workspace; reject traversal."""
        rel = relative_path.strip().lstrip("/").replace("\\", "/")
        if not rel or rel in (".", ".."):
            raise ValueError(f"Invalid path: {relative_path!r}")
        parts = Path(rel).parts
        if ".." in parts:
            raise ValueError(f"Path escapes workspace: {relative_path!r}")
        target = (self.src / rel).resolve()
        if not str(target).startswith(str(self.src.resolve())):
            raise ValueError(f"Path escapes workspace: {relative_path!r}")
        return target

    def write_files(self, files: Dict[str, str]) -> list[str]:
        written: list[str] = []
        for name, content in files.items():
            path = self.resolve_safe(name)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
            written.append(name)
        return written

    def read_tree(self) -> Dict[str, str]:
        out: Dict[str, str] = {}
        if not self.src.exists():
            return out
        for path in sorted(self.src.rglob("*")):
            if path.is_file():
                rel = path.relative_to(self.src).as_posix()
                try:
                    out[rel] = path.read_text(encoding="utf-8")
                except UnicodeDecodeError:
                    out[rel] = ""
        return out

    def to_artifact(self) -> str:
        return serialize_code_artifact(self.read_tree())

    def materialize_from_artifact(self, code_artifact: str) -> list[str]:
        files = parse_code_artifact(code_artifact)
        if not files:
            return []
        return self.write_files(files)

    def list_files(self, subpath: str = "") -> list[str]:
        base = self.resolve_safe(subpath) if subpath else self.src
        if not base.exists():
            return []
        if base.is_file():
            return [subpath or base.name]
        return [
            p.relative_to(self.src).as_posix()
            for p in sorted(base.rglob("*"))
            if p.is_file()
        ]

    def cleanup(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root, ignore_errors=True)

    @staticmethod
    def cleanup_build(build_id: str) -> None:
        root = Path(Config.WORKSPACES_DIR) / build_id
        if root.exists():
            shutil.rmtree(root, ignore_errors=True)
