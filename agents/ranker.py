"""Ranker agent — trait-vector ranking with fallback."""

from __future__ import annotations

from typing import Any

from agents.base import AgentResult, AgentRole, BaseAgent
from agents.outputs import RankerOutput
from core.pipeline_context import PipelineContext
from core.pipeline_fsm import PipelineState


class RankerAgent(BaseAgent):
    role = AgentRole.RANKER
    agent_id = "ranker"

    def __init__(
        self,
        primary_ranker: Any,
        fallback_ranker: Any,
    ) -> None:
        self._primary = primary_ranker
        self._fallback = fallback_ranker

    async def run(self, ctx: PipelineContext) -> AgentResult:
        try:
            if ctx.has_ckpt("ranked_builds") and not ctx.rankings.ranked_builds:
                ctx.rankings.ranked_builds = ctx._deserialize_ranked(
                    ctx.checkpoint["ranked_builds"]
                )
            elif not ctx.rankings.ranked_builds:
                ctx.pipe.begin_stage(
                    PipelineState.RANKING,
                    "ranker",
                    "Ranker: Scoring all builds...",
                    percent=70,
                )
                ctx.log("ranker", "Trait-vector ranking")
                try:
                    ctx.rankings.ranked_builds = await self._primary.rank_all(ctx)
                except Exception as exc:
                    ctx.record_error(
                        "Primary ranker failed, using fallback",
                        phase="ranker",
                        exc=exc,
                    )
                    ctx.rankings.ranked_builds = await self._fallback.rank_all(ctx)
                ctx.save_checkpoint(
                    PipelineState.RANKING,
                    ranked_builds=[r.model_dump() for r in ctx.rankings.ranked_builds],
                    percent=75,
                )

            if not ctx.resolve_winner():
                raise RuntimeError("Ranking failed - no winning attempt found")

            winner_id = (
                ctx.rankings.winning_attempt.attempt_id
                if ctx.rankings.winning_attempt
                else None
            )
            output = RankerOutput(
                ranked_count=len(ctx.rankings.ranked_builds),
                winner_attempt_id=winner_id,
            )
            result = AgentResult(
                role=self.role,
                agent_id=self.agent_id,
                ok=True,
                payload=output.to_dict(),
            )
        except Exception as exc:
            ctx.record_error(str(exc), phase="ranker", exc=exc)
            result = AgentResult(
                role=self.role,
                agent_id=self.agent_id,
                ok=False,
                error=str(exc),
            )
        self._log_result(ctx, result)
        return result
