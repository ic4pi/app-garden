"""Leaderboard agent — records build results to leaderboards."""

from __future__ import annotations

from typing import Any

from agents.base import AgentResult, AgentRole, BaseAgent
from agents.outputs import LeaderboardOutput
from core.pipeline_context import PipelineContext


class LeaderboardAgent(BaseAgent):
    role = AgentRole.LEADERBOARD
    agent_id = "leaderboard"

    def __init__(self, orchestrator: Any) -> None:
        self._orchestrator = orchestrator

    async def run(self, ctx: PipelineContext) -> AgentResult:
        try:
            ctx.pipe.phase("leaderboard", "Updating all leaderboards...", 95)
            
            # Ensure winner is resolved before recording
            if not ctx.rankings.winner and ctx.rankings.ranked_builds:
                ctx.resolve_winner()
            
            if not ctx.rankings.winning_attempt:
                ctx.log("leaderboard", "No winning attempt found - skipping leaderboard", level="warning")
                output = LeaderboardOutput(
                    leaderboard_updated=False,
                    winner_attempt_id=None,
                    winner_score=0.0,
                )
                result = AgentResult(
                    role=self.role,
                    agent_id=self.agent_id,
                    ok=True,
                    payload=output.to_dict(),
                )
                self._log_result(ctx, result)
                return result
            
            self._orchestrator.record_leaderboard(ctx)
            ctx.save_checkpoint(ctx.pipe.current_state, leaderboard_recorded=True, percent=96)

            winner = ctx.rankings.winner
            winning_attempt = ctx.rankings.winning_attempt
            output = LeaderboardOutput(
                leaderboard_updated=True,
                winner_attempt_id=winning_attempt.attempt_id if winning_attempt else None,
                winner_score=winner.total_score if winner else 0,
            )
            result = AgentResult(
                role=self.role,
                agent_id=self.agent_id,
                ok=True,
                payload=output.to_dict(),
            )
        except Exception as exc:
            ctx.record_error(str(exc), phase="leaderboard", exc=exc)
            result = AgentResult(
                role=self.role,
                agent_id=self.agent_id,
                ok=False,
                error=str(exc),
            )
        self._log_result(ctx, result)
        return result
