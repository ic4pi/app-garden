"""Builder agent — responsible + creative generation."""

from __future__ import annotations

from typing import Any

from agents.base import AgentResult, AgentRole, BaseAgent
from agents.outputs import BuilderOutput
from core.pipeline_context import PipelineContext
from core.pipeline_fsm import PipelineState


class BuilderAgent(BaseAgent):
    role = AgentRole.BUILDER
    agent_id = "builder"

    def __init__(self, orchestrator: Any) -> None:
        self._o = orchestrator

    def should_skip(self, ctx: PipelineContext) -> bool:
        return ctx.has_ckpt("all_attempts") and bool(ctx.files.all_attempts)

    async def run(self, ctx: PipelineContext) -> AgentResult:
        if self.should_skip(ctx):
            if ctx.has_ckpt("all_attempts"):
                ctx._apply_ckpt_to_sections()
            output = BuilderOutput(
                resp_count=len(ctx.files.resp_attempts),
                creative_count=len(ctx.files.creative_attempts),
                all_count=len(ctx.files.all_attempts),
                validated_count=sum(
                    1
                    for r in ctx.validation.by_attempt.values()
                    if r.get("passed")
                ),
            )
            return AgentResult(
                role=self.role,
                agent_id=self.agent_id,
                ok=True,
                payload=output.to_dict(),
            )

        try:
            ctx.pipe.begin_stage(
                PipelineState.GENERATING,
                "builders",
                "Builders: Generating apps...",
                percent=25,
            )
            ctx.log("builders", "Responsible + creative builders")
            resp, creative = await self._o.build_all_attempts(ctx)
            ctx.files.resp_attempts = resp
            ctx.files.creative_attempts = creative
            ctx.files.all_attempts = resp + creative
            ctx.save_checkpoint(
                PipelineState.GENERATING,
                resp_attempts=[a.model_dump() for a in ctx.files.resp_attempts],
                creative_attempts=[a.model_dump() for a in ctx.files.creative_attempts],
                all_attempts=[a.model_dump() for a in ctx.files.all_attempts],
                percent=35,
            )

            output = BuilderOutput(
                resp_count=len(ctx.files.resp_attempts),
                creative_count=len(ctx.files.creative_attempts),
                all_count=len(ctx.files.all_attempts),
            )
            result = AgentResult(
                role=self.role,
                agent_id=self.agent_id,
                ok=True,
                payload=output.to_dict(),
            )
        except Exception as exc:
            ctx.record_error(str(exc), phase="builder", exc=exc)
            result = AgentResult(
                role=self.role,
                agent_id=self.agent_id,
                ok=False,
                error=str(exc),
            )
        self._log_result(ctx, result)
        return result
