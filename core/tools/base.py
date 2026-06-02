"""Base tool contract for agent actions."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional


@dataclass
class ToolExecutionContext:
    """Sandbox scope for a single build attempt."""

    build_id: str
    attempt_id: str
    workspace_root: Path
    code_type: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolResult:
    tool: str
    ok: bool
    message: str = ""
    data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "tool": self.tool,
            "ok": self.ok,
            "message": self.message,
            "data": self.data,
        }


class BaseTool(ABC):
    name: str

    @abstractmethod
    async def execute(self, ctx: ToolExecutionContext, **params: Any) -> ToolResult:
        pass
