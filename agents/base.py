"""Base agent contract — swap implementations without breaking the pipeline."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from core.pipeline_context import PipelineContext


class AgentRole(str, Enum):
    PLANNER = "planner"
    BUILDER = "builder"
    REVIEWER = "reviewer"
    RANKER = "ranker"
    REPAIR = "repair"
    NOVELTY = "novelty"
    LEADERBOARD = "leaderboard"


@dataclass
class AgentResult:
    role: AgentRole
    agent_id: str
    ok: bool
    payload: dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    completed: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "role": self.role.value,
            "agent_id": self.agent_id,
            "ok": self.ok,
            "payload": self.payload,
            "error": self.error,
            "completed": self.completed,
        }


class BaseAgent(ABC):
    """Every intelligence role implements run(ctx) and writes to PipelineContext."""

    role: AgentRole
    agent_id: str

    def should_skip(self, ctx: PipelineContext) -> bool:
        """Override for checkpoint-aware skip (default: never skip)."""
        return False

    def _log_result(self, ctx: PipelineContext, result: AgentResult) -> None:
        level = "info" if result.ok else "error"
        msg = f"{result.agent_id}: ok={result.ok}"
        if result.error:
            msg += f" — {result.error}"
        ctx.log(result.role.value, msg, level=level)

    @abstractmethod
    async def run(self, ctx: PipelineContext) -> AgentResult:
        """Read ctx, perform work, mutate ctx sections, return independent output."""
