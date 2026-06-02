"""Shell tool — run allowlisted commands inside workspace only."""

from __future__ import annotations

import asyncio
import re
import shlex
from typing import Any

from core.tools.base import BaseTool, ToolExecutionContext, ToolResult

_ALLOWED_PREFIXES = (
    "pip",
    "pip3",
    "python",
    "python3",
    "npm",
    "npx",
    "node",
    "pytest",
    "py_compile",
)

_BLOCKED_PATTERNS = re.compile(
    r"[;|&$`><]|\.\./|rm\s+-rf|curl\s+.*\|",
    re.IGNORECASE,
)


class ShellTool(BaseTool):
    name = "shell"

    def __init__(self, *, timeout_seconds: float = 60.0) -> None:
        self._timeout = timeout_seconds

    def _validate_command(self, command: str) -> str | None:
        if not command or not command.strip():
            return "Empty command"
        if _BLOCKED_PATTERNS.search(command):
            return "Command blocked by sandbox policy"
        try:
            parts = shlex.split(command)
        except ValueError as exc:
            return f"Invalid command: {exc}"
        if not parts:
            return "Empty command"
        base = parts[0].split("/")[-1].lower()
        if not any(base == p or base.startswith(p + ".") for p in _ALLOWED_PREFIXES):
            return f"Command not allowlisted: {parts[0]}"
        return None

    async def execute(self, ctx: ToolExecutionContext, **params: Any) -> ToolResult:
        command = params.get("command", "")
        err = self._validate_command(command)
        if err:
            return ToolResult(self.name, False, err)

        cwd = ctx.workspace_root / "src"
        cwd.mkdir(parents=True, exist_ok=True)

        try:
            proc = await asyncio.create_subprocess_shell(
                command,
                cwd=str(cwd),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(),
                    timeout=self._timeout,
                )
            except asyncio.TimeoutError:
                proc.kill()
                await proc.communicate()
                return ToolResult(self.name, False, "Command timed out")

            out = (stdout or b"").decode("utf-8", errors="replace")[:4000]
            err_out = (stderr or b"").decode("utf-8", errors="replace")[:2000]
            ok = proc.returncode == 0
            return ToolResult(
                self.name,
                ok,
                f"exit {proc.returncode}",
                data={
                    "command": command,
                    "stdout": out,
                    "stderr": err_out,
                    "returncode": proc.returncode,
                },
            )
        except Exception as exc:
            return ToolResult(self.name, False, str(exc))
