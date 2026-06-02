"""Package lookup tool — PyPI metadata for dependency decisions."""

from __future__ import annotations

import json
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen

from core.tools.base import BaseTool, ToolExecutionContext, ToolResult

_PYPI_JSON = "https://pypi.org/pypi/{package}/json"


class PackageLookupTool(BaseTool):
    name = "package_lookup"

    async def execute(self, ctx: ToolExecutionContext, **params: Any) -> ToolResult:
        package = (params.get("package") or params.get("name") or "").strip().lower()
        if not package:
            return ToolResult(self.name, False, "package name required")
        if not package.replace("-", "").replace("_", "").replace(".", "").isalnum():
            return ToolResult(self.name, False, "invalid package name")

        url = _PYPI_JSON.format(package=package)
        try:
            with urlopen(url, timeout=10) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except URLError as exc:
            return ToolResult(self.name, False, f"PyPI lookup failed: {exc.reason}")
        except Exception as exc:
            return ToolResult(self.name, False, str(exc))

        info = payload.get("info") or {}
        return ToolResult(
            self.name,
            True,
            info.get("summary") or package,
            data={
                "package": package,
                "version": info.get("version"),
                "summary": info.get("summary"),
                "requires_python": info.get("requires_python"),
                "home_page": info.get("home_page") or info.get("project_url"),
                "license": info.get("license"),
            },
        )
