"""Reviewer agent — builder reviews + triple app review merge."""

from __future__ import annotations

from typing import Any

from agents.base import AgentResult, AgentRole, BaseAgent
from agents.outputs import ReviewerOutput
from core.pipeline_context import PipelineContext
from core.pipeline_fsm import PipelineState


class ReviewerAgent(BaseAgent):
    role = AgentRole.REVIEWER
    agent_id = "reviewer"

    def __init__(self, orchestrator: Any) -> None:
        self._o = orchestrator

    async def run(self, ctx: PipelineContext) -> AgentResult:
        try:
            if not ctx.files.all_attempts:
                raise RuntimeError("No attempts to review — validation may have failed")

            if ctx.has_ckpt("builder_reviews") and not ctx.files.builder_reviews:
                ctx.files.builder_reviews = ctx.checkpoint["builder_reviews"]
            elif not ctx.files.builder_reviews:
                ctx.pipe.phase(
                    "builder_review",
                    "Builder Reviewer: Evaluating code quality...",
                    40,
                )
                ctx.files.builder_reviews = await self._o.builder_reviewer.review_all_builders(
                    ctx
                )
                ctx.save_checkpoint(
                    PipelineState.GENERATING,
                    builder_reviews=ctx.files.builder_reviews,
                    percent=45,
                )

            if ctx.has_ckpt("app_reviews") and not ctx.files.app_reviews:
                ctx.files.app_reviews = ctx._deserialize_reviews(ctx.checkpoint["app_reviews"])
            elif not ctx.files.app_reviews:
                ctx.pipe.begin_stage(
                    PipelineState.REVIEWING,
                    "app_review",
                    "App Reviewers: Evaluating final products...",
                    percent=55,
                )
                ctx.log("app_review", "Triple reviewer pass")
                ctx.files.app_reviews = await self._o.run_app_reviews(ctx)
                ctx.save_checkpoint(
                    PipelineState.REVIEWING,
                    app_reviews=[r.model_dump() for r in ctx.files.app_reviews],
                    percent=60,
                )

            output = ReviewerOutput(
                builder_reviews_count=len(ctx.files.builder_reviews),
                app_reviews_count=len(ctx.files.app_reviews),
            )
            result = AgentResult(
                role=self.role,
                agent_id=self.agent_id,
                ok=True,
                payload=output.to_dict(),
            )
        except Exception as exc:
            ctx.record_error(str(exc), phase="reviewer", exc=exc)
            result = AgentResult(
                role=self.role,
                agent_id=self.agent_id,
                ok=False,
                error=str(exc),
            )
        self._log_result(ctx, result)
        return result
