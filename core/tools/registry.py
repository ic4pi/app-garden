"""Tool registry — dispatch and swap tools per agent."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from core.tools.base import BaseTool, ToolExecutionContext, ToolResult
from core.tools.filesystem import FilesystemTool
from core.tools.package_lookup import PackageLookupTool
from core.tools.shell import ShellTool


@dataclass
class ToolRegistry:
    filesystem: BaseTool = field(default_factory=FilesystemTool)
    shell: BaseTool = field(default_factory=ShellTool)
    package_lookup: BaseTool = field(default_factory=PackageLookupTool)

    @classmethod
    def default(cls) -> "ToolRegistry":
        return cls()

    def get(self, name: str) -> Optional[BaseTool]:
        return {
            "filesystem": self.filesystem,
            "shell": self.shell,
            "package_lookup": self.package_lookup,
        }.get(name)

    async def run(
        self,
        ctx: ToolExecutionContext,
        tool_name: str,
        **params: Any,
    ) -> ToolResult:
        tool = self.get(tool_name)
        if not tool:
            return ToolResult(tool_name, False, f"Unknown tool: {tool_name}")
        return await tool.execute(ctx, **params)
