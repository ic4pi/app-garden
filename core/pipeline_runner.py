"""Deterministic pipeline execution — stages flow through AgentRegistry."""

from __future__ import annotations

import time
import uuid
from datetime import datetime
from typing import Any, Optional

from agents.registry import AgentRegistry, build_agent_registry
from core.pipeline_context import PipelineContext
from core.pipeline_fsm import PipelineState
from core.pipeline_stages import PipelineStage
from core.validation.gate import QualityGate


class PipelineRunner:
    """Runs the factory pipeline via swappable agents on a single PipelineContext."""

    def __init__(
        self,
        ctx: PipelineContext,
        *,
        agents: Optional[AgentRegistry] = None,
        gate: Optional[QualityGate] = None,
    ) -> None:
        self.ctx = ctx
        self.o = ctx.services.orchestrator
        self._gate = gate or QualityGate()
        self.agents = agents or build_agent_registry(self.o, gate=self._gate)

    async def execute(self) -> dict[str, Any]:
        ctx = self.ctx
        if ctx.resume:
            if not ctx.db.get_build(ctx.build_id):
                return {"status": "failed", "error": "Build not found"}
            progress = ctx.db.get_progress(ctx.build_id)
            ctx.pipe.phase(
                progress.get("resume_phase") or "factory_builder",
                "Resuming pipeline from checkpoint...",
                progress.get("percent", ctx.checkpoint.get("percent", 5)),
            )
        else:
            if not ctx.db.get_build(ctx.build_id):
                ctx.db.create_build(ctx.build_id, ctx.request.model_dump())
            ctx.save_checkpoint(
                PipelineState.QUEUED,
                request=ctx.request.model_dump(),
                start_time=time.time(),
            )
            ctx.start_time = time.time()
            ctx.pipe.begin_stage(
                PipelineState.PLANNING,
                "factory_builder",
                "Factory: Analyzing requirements...",
            )

        try:
            await self._stage_planning()
            await self._stage_builder()
            await self._stage_repair()
            await self._stage_reviewing()
            await self._stage_ranking()
            await self._stage_novelty()
            await self._stage_packaging()
            await self._stage_leaderboard()
            return await self._finalize_success()
        except Exception as exc:
            return self._finalize_failure(exc)

    async def execute_stage(self, stage: PipelineStage) -> dict[str, Any]:
        """Run a single pipeline stage (used by Celery worker pools)."""
        ctx = self.ctx
        ctx.set_stage(stage)
        if ctx.resume or ctx.db.get_build(ctx.build_id):
            if not ctx.checkpoints:
                ctx.hydrate_from_db()
        elif stage == PipelineStage.PLANNING:
            if not ctx.db.get_build(ctx.build_id):
                ctx.db.create_build(ctx.build_id, ctx.request.model_dump())
            ctx.save_checkpoint(
                PipelineState.QUEUED,
                request=ctx.request.model_dump(),
                start_time=time.time(),
            )
            ctx.start_time = time.time()

        try:
            if stage == PipelineStage.PLANNING:
                await self._stage_planning()
            elif stage == PipelineStage.BUILDER:
                await self._stage_builder()
            elif stage == PipelineStage.REPAIR:
                await self._stage_repair()
            elif stage == PipelineStage.REVIEWING:
                await self._stage_reviewing()
            elif stage == PipelineStage.RANKING:
                await self._stage_ranking()
            elif stage == PipelineStage.NOVELTY:
                await self._stage_novelty()
            elif stage == PipelineStage.PACKAGING:
                await self._stage_packaging()
            elif stage == PipelineStage.LEADERBOARD:
                await self._stage_leaderboard()
            elif stage == PipelineStage.FINALIZE:
                return await self._finalize_success()
            else:
                raise ValueError(f"Unknown pipeline stage: {stage}")
            progress = ctx.db.get_progress(ctx.build_id)
            return {"status": "ok", "stage": stage.value, "build_id": ctx.build_id, "progress": progress}
        except Exception as exc:
            failed = self._finalize_failure(exc)
            failed["stage"] = stage.value
            return failed

    async def _stage_planning(self) -> None:
        await self.agents.run_planning(self.ctx)

    async def _stage_builder(self) -> None:
        await self.agents.run_builder(self.ctx)

    async def _stage_repair(self) -> None:
        await self.agents.run_repair(self.ctx)

    async def _stage_generating(self) -> None:
        await self._stage_builder()
        await self._stage_repair()

    async def _stage_reviewing(self) -> None:
        await self.agents.run_reviewing(self.ctx)

    async def _stage_ranking(self) -> None:
        await self.agents.run_ranking(self.ctx)

    async def _stage_novelty(self) -> None:
        await self.agents.run_novelty(self.ctx)
        await self._validate_final_code()

    async def _validate_final_code(self) -> None:
        ctx = self.ctx
        if not ctx.files.final_code:
            ctx.validation.final_code_passed = False
            return
        report = self._gate.validate_artifact(
            "final_deliverable",
            ctx.files.final_code,
            code_type=ctx.request.code_type.value,
        )
        ctx.validation.final_code_passed = report.passed
        if not report.passed:
            ctx.log(
                "validation",
                f"Final code failed quality gate: {report.issues[0].message if report.issues else 'unknown'}",
                level="warning",
            )
            winner = ctx.rankings.winning_attempt
            if winner and getattr(winner, 'code_artifact', None):
                fallback_report = self._gate.validate_artifact(
                    "winner_fallback",
                    winner.code_artifact,
                    code_type=ctx.request.code_type.value,
                )
                if fallback_report.passed:
                    ctx.files.final_code = winner.code_artifact
                    ctx.validation.final_code_passed = True
                    ctx.log("validation", "Reverted final code to validated winner artifact")
        ctx.save_checkpoint(
            PipelineState.RANKING,
            final_code=ctx.files.final_code,
            validation={
                "by_attempt": ctx.validation.by_attempt,
                "all_passed": ctx.validation.all_passed,
                "final_code_passed": ctx.validation.final_code_passed,
            },
        )

    async def _stage_packaging(self) -> None:
        ctx = self.ctx
        if not ctx.validation.final_code_passed:
            ctx.fail("Final deliverable failed validation — cannot package")
            raise RuntimeError("Final deliverable failed validation")

        if ctx.has_ckpt("zip_path") and not ctx.files.zip_path:
            ctx.files.zip_path = ctx.checkpoint["zip_path"]
        elif not ctx.files.zip_path:
            winning_attempt = ctx.rankings.winning_attempt
            if not winning_attempt and ctx.rankings.ranked_builds:
                if ctx.resolve_winner():
                    winning_attempt = ctx.rankings.winning_attempt
                    ctx.log(
                        "packaging",
                        "Recovered winning attempt from ranked_builds during resume",
                    )
            if not winning_attempt:
                ctx.fail("Packaging failed: no winning attempt found")
                raise RuntimeError("No winning attempt to package")
            tool_stack = getattr(winning_attempt, 'tool_stack', None)
            if not tool_stack:
                ctx.fail("Packaging failed: winning attempt has no tool_stack")
                raise RuntimeError("No tool_stack to package")
            ctx.pipe.begin_stage(
                PipelineState.PACKAGING,
                "packaging",
                "Packaging final deliverable...",
                percent=90,
            )
            project_name = f"{ctx.request.code_type.value}_project"
            ctx.files.zip_path = ctx.download_manager.create_package(
                project_name,
                ctx.files.final_code,
                tool_stack,
                ctx.request,
            )
            ctx.log("packaging", f"Package written: {ctx.files.zip_path}")
            ctx.save_checkpoint(PipelineState.PACKAGING, zip_path=ctx.files.zip_path, percent=92)

    async def _stage_leaderboard(self) -> None:
        await self.agents.run_leaderboard(self.ctx)

    async def _finalize_success(self) -> dict[str, Any]:
        ctx = self.ctx
        if not ctx.validation.final_code_passed:
            ctx.fail("Build completed without validated final code")
            failed = {
                "status": "failed",
                "error": "Final code did not pass validation",
                "build_id": ctx.build_id,
            }
            ctx.db.save_results(ctx.build_id, failed)
            raise RuntimeError(failed["error"])

        winner = ctx.rankings.winner
        builder_scores = [br.get("overall_score", 50) for br in ctx.files.builder_reviews]
        avg_builder = sum(builder_scores) / len(builder_scores) if builder_scores else 50

        execution_score = getattr(winner, 'execution_score', 0.0)
        confidence_score = getattr(winner, 'confidence_score', 50.0)

        ctx.pipe.complete(
            f"Complete! Factory:{ctx.plan.factory_score} | Builder:{avg_builder:.0f} | App Execution:{execution_score:.0f} | Confidence:{confidence_score:.0f}"
        )

        results = self.o.build_results_payload(ctx, avg_builder_score=avg_builder)
        results["validation"] = {
            "by_attempt": ctx.validation.by_attempt,
            "all_passed": ctx.validation.all_passed,
            "final_code_passed": ctx.validation.final_code_passed,
        }
        results["agents"] = [r.to_dict() for r in getattr(ctx, "_agent_results", [])]
        ctx.db.save_results(ctx.build_id, results)
        ctx.save_checkpoint(PipelineState.COMPLETE, completed=True)
        return results

    def _finalize_failure(self, exc: Exception) -> dict[str, Any]:
        ctx = self.ctx
        err = str(exc)
        if err not in (
            "Ranking failed - no winning attempt found",
            "Final deliverable failed validation",
            "Final code did not pass validation",
            "No attempts passed validation",
        ):
            ctx.fail(err, exc=exc)
        failed = {
            "status": "failed",
            "error": err,
            "build_id": ctx.build_id,
            "errors": ctx.errors.entries,
            "validation": {
                "by_attempt": ctx.validation.by_attempt,
                "final_code_passed": ctx.validation.final_code_passed,
            },
        }
        ctx.db.save_results(ctx.build_id, failed)
        return failed
