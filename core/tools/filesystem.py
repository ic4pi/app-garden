"""Filesystem tool — read/write/list inside workspace sandbox."""

from __future__ import annotations

from typing import Any

from core.tools.base import BaseTool, ToolExecutionContext, ToolResult


class FilesystemTool(BaseTool):
    name = "filesystem"

    async def execute(self, ctx: ToolExecutionContext, **params: Any) -> ToolResult:
        action = (params.get("action") or "list").lower()
        workspace = ctx.workspace_root
        from core.tools.workspace import BuildWorkspace

        ws = BuildWorkspace(workspace)

        try:
            if action == "write":
                path = params.get("path", "")
                content = params.get("content", "")
                if not path:
                    return ToolResult(self.name, False, "path required")
                ws.resolve_safe(path).parent.mkdir(parents=True, exist_ok=True)
                ws.resolve_safe(path).write_text(content, encoding="utf-8")
                return ToolResult(
                    self.name,
                    True,
                    f"Wrote {path}",
                    data={"path": path, "bytes": len(content.encode("utf-8"))},
                )

            if action == "read":
                path = params.get("path", "")
                if not path:
                    return ToolResult(self.name, False, "path required")
                text = ws.resolve_safe(path).read_text(encoding="utf-8")
                return ToolResult(
                    self.name,
                    True,
                    f"Read {path}",
                    data={"path": path, "content": text[:8000]},
                )

            if action == "list":
                sub = params.get("path", "")
                files = ws.list_files(sub)
                return ToolResult(
                    self.name,
                    True,
                    f"{len(files)} file(s)",
                    data={"files": files},
                )

            if action == "mkdir":
                path = params.get("path", "")
                if not path:
                    return ToolResult(self.name, False, "path required")
                ws.resolve_safe(path).mkdir(parents=True, exist_ok=True)
                return ToolResult(self.name, True, f"Created directory {path}")

            if action == "write_tree":
                files = params.get("files") or {}
                if not isinstance(files, dict):
                    return ToolResult(self.name, False, "files must be a dict")
                written = ws.write_files({str(k): str(v) for k, v in files.items()})
                return ToolResult(
                    self.name,
                    True,
                    f"Wrote {len(written)} file(s)",
                    data={"written": written},
                )

            return ToolResult(self.name, False, f"Unknown action: {action}")
        except Exception as exc:
            return ToolResult(self.name, False, str(exc))
