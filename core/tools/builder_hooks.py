"""Integration hooks for Responsible/Creative builders."""

from __future__ import annotations

from typing import Any

from core.config import Config
from core.tools.builder_toolkit import BuilderToolkit, MaterializeResult


async def apply_builder_tools(
    attempt: Any,
    *,
    build_id: str,
    code_type: str = "",
    ctx: Any = None,
) -> Any:
    """
    Materialize code_artifact into a real workspace file tree.
    Mutates attempt in place (workspace_path, build_log, tool_usage_report, code_artifact).
    """
    if not Config.ENABLE_BUILDER_TOOLS:
        return attempt
    if not attempt.code_artifact or not attempt.success:
        return attempt

    toolkit = BuilderToolkit.for_attempt(
        build_id,
        attempt.attempt_id,
        code_type=code_type,
    )
    result = await toolkit.materialize_from_artifact(attempt.code_artifact)
    attempt.workspace_path = result.workspace_path
    if ctx is not None and hasattr(ctx, "workspaces"):
        ctx.workspaces.record(attempt.attempt_id, result.workspace_path)
    attempt.build_log += f"\n[toolkit] Materialized {result.files_written} file(s) → {result.workspace_path}\n"
    attempt.build_log += result.log
    attempt.tool_usage_report = (attempt.tool_usage_report or "") + "\n" + result.tool_report

    if result.ok and result.files_written > 0:
        attempt.code_artifact = toolkit.artifact_from_workspace()
    else:
        attempt.success = False
        attempt.error_message = attempt.error_message or "Failed to materialize file tree"

    return attempt
