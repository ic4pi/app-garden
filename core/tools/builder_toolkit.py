"""High-level toolkit used by builders to materialize and verify file trees."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List, Optional

from core.artifact_parser import parse_code_artifact
from core.tools.base import ToolExecutionContext, ToolResult
from core.tools.registry import ToolRegistry
from core.tools.workspace import BuildWorkspace


@dataclass
class MaterializeResult:
    ok: bool
    files_written: int = 0
    workspace_path: str = ""
    log: str = ""
    tool_report: str = ""
    actions: List[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "files_written": self.files_written,
            "workspace_path": self.workspace_path,
            "log": self.log,
            "tool_report": self.tool_report,
            "actions": self.actions,
        }


class BuilderToolkit:
    """
    Gives builders real-world power: write trees, run shell checks, lookup packages.
    """

    def __init__(
        self,
        workspace: BuildWorkspace,
        *,
        registry: Optional[ToolRegistry] = None,
        build_id: str = "",
        attempt_id: str = "",
        code_type: str = "",
    ) -> None:
        self.workspace = workspace
        self.registry = registry or ToolRegistry.default()
        self._ctx = ToolExecutionContext(
            build_id=build_id,
            attempt_id=attempt_id,
            workspace_root=workspace.root,
            code_type=code_type,
        )

    @classmethod
    def for_attempt(
        cls,
        build_id: str,
        attempt_id: str,
        *,
        code_type: str = "",
        registry: Optional[ToolRegistry] = None,
    ) -> "BuilderToolkit":
        ws = BuildWorkspace.for_attempt(build_id, attempt_id)
        return cls(
            ws,
            registry=registry,
            build_id=build_id,
            attempt_id=attempt_id,
            code_type=code_type,
        )

    async def materialize_from_artifact(self, code_artifact: str) -> MaterializeResult:
        """Parse LLM output and write a real file tree under workspace/src."""
        actions: list[dict[str, Any]] = []
        log_lines: list[str] = []
        report_lines: list[str] = ["=== Builder Toolkit ==="]

        files = parse_code_artifact(code_artifact)
        if not files:
            return MaterializeResult(
                ok=False,
                workspace_path=str(self.workspace.root),
                log="No parseable files in artifact\n",
                tool_report="No files materialized\n",
            )

        fs_result = await self.registry.run(
            self._ctx,
            "filesystem",
            action="write_tree",
            files=files,
        )
        actions.append(fs_result.to_dict())
        log_lines.append(f"[tool:filesystem] {fs_result.message}")
        report_lines.append(f"filesystem.write_tree: {fs_result.message}")

        if not fs_result.ok:
            return MaterializeResult(
                ok=False,
                files_written=0,
                workspace_path=str(self.workspace.root),
                log="\n".join(log_lines) + "\n",
                tool_report="\n".join(report_lines) + "\n",
                actions=actions,
            )

        written = fs_result.data.get("written") or list(files.keys())
        list_result = await self.registry.run(
            self._ctx, "filesystem", action="list", path=""
        )
        actions.append(list_result.to_dict())
        report_lines.append(f"workspace files: {list_result.data.get('files', [])}")

        await self._post_materialize_hooks(written, actions, log_lines, report_lines)

        return MaterializeResult(
            ok=True,
            files_written=len(written),
            workspace_path=str(self.workspace.root),
            log="\n".join(log_lines) + "\n",
            tool_report="\n".join(report_lines) + "\n",
            actions=actions,
        )

    async def _post_materialize_hooks(
        self,
        written: list,
        actions: list,
        log_lines: list,
        report_lines: list,
    ) -> None:
        """Run lightweight verification commands when relevant files exist."""
        tree = self.workspace.read_tree()
        names = {n.lower() for n in tree}

        if "requirements.txt" in names:
            lookup = await self._lookup_top_requirements(tree.get("requirements.txt", ""))
            for entry in lookup:
                actions.append(entry.to_dict())
                report_lines.append(f"package_lookup: {entry.message}")

            shell_result = await self.registry.run(
                self._ctx,
                "shell",
                command="pip install -r requirements.txt --dry-run",
            )
            actions.append(shell_result.to_dict())
            log_lines.append(f"[tool:shell] {shell_result.message}")
            report_lines.append(f"shell: {shell_result.message}")

        elif any(n.endswith(".py") for n in names):
            import shlex

            for pf in [n for n in tree if n.endswith(".py")][:5]:
                shell_result = await self.registry.run(
                    self._ctx,
                    "shell",
                    command=f"python3 -m py_compile {shlex.quote(pf)}",
                )
                actions.append(shell_result.to_dict())
                log_lines.append(f"[tool:shell] {pf}: {shell_result.message}")
                if not shell_result.ok:
                    break

        if "package.json" in names:
            shell_result = await self.registry.run(
                self._ctx,
                "shell",
                command="npm install --dry-run",
            )
            actions.append(shell_result.to_dict())
            log_lines.append(f"[tool:shell] npm: {shell_result.message}")

    async def _lookup_top_requirements(self, requirements_text: str) -> list[ToolResult]:
        results: list[ToolResult] = []
        for line in requirements_text.splitlines()[:3]:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or stripped.startswith("-"):
                continue
            pkg = stripped.split("==")[0].split(">=")[0].split("[")[0].strip()
            if pkg:
                results.append(
                    await self.registry.run(
                        self._ctx, "package_lookup", package=pkg
                    )
                )
        return results

    def artifact_from_workspace(self) -> str:
        """Rebuild code_artifact string from on-disk tree."""
        return self.workspace.to_artifact()
