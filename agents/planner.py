"""Planner agent — tool stacks + factory strategy review."""

from __future__ import annotations

from typing import Any

from agents.base import AgentResult, AgentRole, BaseAgent
from agents.outputs import PlannerOutput
from core.pipeline_context import PipelineContext
from core.pipeline_fsm import PipelineState


class PlannerAgent(BaseAgent):
    role = AgentRole.PLANNER
    agent_id = "planner"

    def __init__(self, factory_builder: Any, factory_reviewer: Any) -> None:
        self._factory_builder = factory_builder
        self._factory_reviewer = factory_reviewer

    async def run(self, ctx: PipelineContext) -> AgentResult:
        try:
            if not ctx.has_ckpt("tool_combinations"):
                ctx.pipe.phase("factory_builder", "Factory: Generating tool stacks...", 10)
                ctx.log("factory_builder", "Generating tool stacks")
                self._factory_builder.plan(ctx)
                ctx.save_checkpoint(
                    PipelineState.PLANNING,
                    tool_combinations=[t.model_dump() for t in ctx.plan.tool_combinations],
                    percent=10,
                )
                # persist immediately to the DB to avoid lost plan artifacts
                try:
                    ctx.db.save_checkpoint(
                        ctx.build_id,
                        {"tool_combinations": [t.model_dump() for t in ctx.plan.tool_combinations]},
                    )
                except Exception:
                    pass

            if not ctx.has_ckpt("factory_review"):
                ctx.pipe.phase("factory_review", "Factory Reviewer: Evaluating strategy...", 15)
                ctx.log("factory_review", "Factory strategy review")
                review = await self._factory_reviewer.review_plan(ctx)
                if not isinstance(review, dict) or "overall_score" not in review:
                    review = {
                        "overall_score": 50,
                        "error": "Invalid factory review response",
                    }
                ctx.plan.factory_review = review
                ctx.plan.factory_score = review.get("overall_score", 70)
                ctx.save_checkpoint(
                    PipelineState.PLANNING,
                    factory_review=review,
                    factory_score=ctx.plan.factory_score,
                    percent=15,
                )
                # persist factory review immediately as a safety measure
                try:
                    ctx.db.save_checkpoint(
                        ctx.build_id,
                        {"factory_review": review},
                    )
                except Exception:
                    pass
            else:
                ctx.plan.factory_review = ctx.checkpoint.get("factory_review", ctx.plan.factory_review)
                ctx.plan.factory_score = ctx.checkpoint.get("factory_score", ctx.plan.factory_score)

            output = PlannerOutput(
                tool_combinations_count=len(ctx.plan.tool_combinations),
                factory_score=ctx.plan.factory_score,
                factory_review=ctx.plan.factory_review,
            )
            result = AgentResult(
                role=self.role,
                agent_id=self.agent_id,
                ok=True,
                payload=output.to_dict(),
            )
        except Exception as exc:
            ctx.record_error(str(exc), phase="planner", exc=exc)
            result = AgentResult(
                role=self.role,
                agent_id=self.agent_id,
                ok=False,
                error=str(exc),
            )
        self._log_result(ctx, result)
        return result
