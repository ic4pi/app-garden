"""Novelty agent — creative iterations on winning build."""

from __future__ import annotations

from typing import Any

from agents.base import AgentResult, AgentRole, BaseAgent
from agents.outputs import NoveltyOutput
from core.pipeline_context import PipelineContext
from core.pipeline_fsm import PipelineState


class NoveltyAgent(BaseAgent):
    role = AgentRole.NOVELTY
    agent_id = "novelty"

    def __init__(self, novelty_builder: Any) -> None:
        self._novelty_builder = novelty_builder

    async def run(self, ctx: PipelineContext) -> AgentResult:
        try:
            if not ctx.has_ckpt("novelty_attempts"):
                ctx.pipe.phase("novelty", "Novelty Builder: Creative iterations...", 80)
                ctx.log("novelty", "Novelty iterations on winner")
                novelty_attempts = await self._novelty_builder.build_novelty(ctx)
                ctx.files.novelty_attempts = novelty_attempts
                
                # Try to get final code from novelty or fallback to winner
                successful = [a for a in novelty_attempts if a.success and getattr(a, 'code_artifact', None)]
                if successful:
                    ctx.files.final_code = successful[-1].code_artifact
                    ctx.log("novelty", f"Using successful novelty attempt: {successful[-1].attempt_id}")
                elif ctx.rankings.winning_attempt:
                    winner_code = getattr(ctx.rankings.winning_attempt, 'code_artifact', None) or getattr(ctx.rankings.winning_attempt, 'code', None)
                    if winner_code:
                        ctx.log("novelty", "No novelty attempts succeeded; using winner code as final output")
                        ctx.files.final_code = winner_code
                    else:
                        ctx.log("novelty", "Winner attempt has no code_artifact; falling back to all attempts")
                else:
                    ctx.log("novelty", "No winner attempt available; falling back to all attempts")

                if not ctx.files.final_code:
                    for att in ctx.files.all_attempts:
                        code = getattr(att, 'code_artifact', None) or getattr(att, 'code', None)
                        if code:
                            ctx.log("novelty", f"Final fallback: using code from {getattr(att, 'attempt_id', 'unknown')}")
                            ctx.files.final_code = code
                            break
                    else:
                        ranked = ctx.checkpoint.get("ranked_builds", [])
                        if ranked:
                            ctx.log("novelty", "No code in all_attempts; checking ranked_builds as last fallback")
                            for ranked_item in ranked:
                                if ranked_item.get("code_artifact"):
                                    ctx.files.final_code = ranked_item.get("code_artifact")
                                    break
                    
                    if not ctx.files.final_code:
                        all_att = ctx.checkpoint.get("all_attempts", [])
                        if all_att:
                            for att in all_att:
                                if att.get("code_artifact"):
                                    ctx.log("novelty", f"Final fallback: using code from {att.get('attempt_id')}")
                                    ctx.files.final_code = att.get("code_artifact")
                                    break
                    if not ctx.files.final_code:
                        ctx.log("novelty", "WARNING: No code found anywhere; final_code is None", level="warning")
                
                ctx.save_checkpoint(
                    PipelineState.RANKING,
                    novelty_attempts=[a.model_dump() for a in novelty_attempts],
                    final_code=ctx.files.final_code,
                    percent=85,
                )
            else:
                ctx.files.novelty_attempts = ctx._deserialize_novelty(ctx.checkpoint["novelty_attempts"])
                ctx.files.final_code = ctx.checkpoint.get("final_code")
                if not ctx.files.final_code:
                    # On resume, if final_code is None, try to recover it from ranked_builds
                    ranked = ctx.checkpoint.get("ranked_builds", [])
                    all_att = ctx.checkpoint.get("all_attempts", [])
                    if ranked:
                        ctx.log("novelty", "Resume: recovering final_code from ranked_builds")
                        for ranked_item in ranked:
                            if ranked_item.get("code_artifact"):
                                ctx.files.final_code = ranked_item.get("code_artifact")
                                break
                    if not ctx.files.final_code and all_att:
                        for att in all_att:
                            if att.get("code_artifact"):
                                ctx.log("novelty", f"Resume: recovering code from {att.get('attempt_id')}")
                                ctx.files.final_code = att.get("code_artifact")
                                break

            output = NoveltyOutput(
                novelty_attempts_count=len(ctx.files.novelty_attempts),
                successful_count=len([a for a in ctx.files.novelty_attempts if a.success]),
                final_code_length=len(ctx.files.final_code) if ctx.files.final_code else 0,
            )
            result = AgentResult(
                role=self.role,
                agent_id=self.agent_id,
                ok=True,
                payload=output.to_dict(),
            )
        except Exception as exc:
            ctx.record_error(str(exc), phase="novelty", exc=exc)
            result = AgentResult(
                role=self.role,
                agent_id=self.agent_id,
                ok=False,
                error=str(exc),
            )
        self._log_result(ctx, result)
        return result
