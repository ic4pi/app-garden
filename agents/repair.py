"""Repair agent — LLM fallbacks + validation repair loop."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Callable, Optional

from agents.base import AgentResult, AgentRole, BaseAgent
from agents.outputs import RepairOutput
from core.pipeline_context import PipelineContext
from core.pipeline_fsm import PipelineState
from core.validation.gate import QualityGate
from core.validation.repair import validation_repair_loop


class RepairAgent(BaseAgent):
    role = AgentRole.REPAIR
    agent_id = "repair"

    def __init__(
        self,
        orchestrator: Any,
        *,
        gate: Optional[QualityGate] = None,
        max_validation_rounds: int = 2,
        fallback_generator: Optional[Callable[[str, str, Any], str]] = None,
    ) -> None:
        self._o = orchestrator
        self._gate = gate or QualityGate()
        self._max_rounds = max_validation_rounds
        self._fallback_generator = fallback_generator

    def should_skip(self, ctx: PipelineContext) -> bool:
        return ctx.has_ckpt("validated_attempts") and bool(ctx.checkpoint.get("all_attempts"))

    async def run(self, ctx: PipelineContext) -> AgentResult:
        if self.should_skip(ctx):
            ctx._apply_ckpt_to_sections()
            if ctx.checkpoint.get("validation"):
                ctx.validation.hydrate(ctx.checkpoint["validation"])
            output = RepairOutput(strategies=["skipped"])
            return AgentResult(
                role=self.role,
                agent_id=self.agent_id,
                ok=bool(ctx.files.all_attempts),
                payload=output.to_dict(),
            )

        strategies: list[str] = []
        fallback_count = 0
        validation_repairs = 0

        try:
            if ctx.has_ckpt("validation") and ctx.checkpoint.get("validation"):
                ctx.validation.hydrate(ctx.checkpoint["validation"])
            elif ctx.has_ckpt("validated_attempts"):
                pass

            # 1) LLM build fallbacks
            if not ctx.has_ckpt("repair_fallbacks_done"):
                before = len([a for a in ctx.files.all_attempts if a.success])
                ctx.files.all_attempts = await self._o.apply_build_fallbacks(ctx)
                after = len([a for a in ctx.files.all_attempts if a.success])
                if after > before:
                    fallback_count += after - before
                    strategies.append("llm_fallback")
                ctx.save_checkpoint(
                    PipelineState.GENERATING,
                    all_attempts=[a.model_dump() for a in ctx.files.all_attempts],
                    repair_fallbacks_done=True,
                    percent=38,
                )

            # 2) Validation loop per attempt
            ctx.pipe.phase("validation", "Quality gate: validating builds...", 42)
            ctx.log("validation", "Running syntax/lint/deps/tests")

            code_type = ctx.request.code_type.value
            description = ctx.request.description

            for attempt in ctx.files.all_attempts:
                if not attempt.code_artifact:
                    attempt.success = False
                    ctx.validation.by_attempt[attempt.attempt_id] = {
                        "passed": False,
                        "issues": [{"message": "Empty artifact"}],
                    }
                    continue

                def _on_repair(strategy: str, aid: str) -> None:
                    nonlocal validation_repairs
                    validation_repairs += 1
                    strategies.append(strategy)

                stack = attempt.tool_stack
                fb = self._fallback_generator
                if fb is None:
                    fb = _default_fallback

                _, report = await validation_repair_loop(
                    attempt,
                    self._gate,
                    max_rounds=self._max_rounds,
                    code_type=code_type,
                    description=description,
                    tool_stack=stack,
                    fallback_generator=fb,
                    on_repair=_on_repair,
                )
                ctx.validation.by_attempt[attempt.attempt_id] = report.to_dict()

            ctx.validation.all_passed = all(
                r.get("passed") for r in ctx.validation.by_attempt.values()
            ) if ctx.validation.by_attempt else False

            validated = [
                a
                for a in ctx.files.all_attempts
                if ctx.validation.by_attempt.get(a.attempt_id, {}).get("passed")
            ]

            if len(validated) < 2:
                validated = await self._ensure_minimum_validated(
                    ctx, validated, strategies
                )

            if validated:
                ctx.files.all_attempts = validated
            else:
                ctx.record_error(
                    "No attempts passed validation",
                    phase="validation",
                )

            ctx.save_checkpoint(
                PipelineState.GENERATING,
                all_attempts=[a.model_dump() for a in ctx.files.all_attempts],
                validation={
                    "by_attempt": ctx.validation.by_attempt,
                    "all_passed": ctx.validation.all_passed,
                },
                validated_attempts=True,
                percent=44,
            )

            output = RepairOutput(
                fallback_repairs=fallback_count,
                validation_repairs=validation_repairs,
                strategies=list(dict.fromkeys(strategies)),
            )
            result = AgentResult(
                role=self.role,
                agent_id=self.agent_id,
                ok=bool(validated),
                payload=output.to_dict(),
                error=None if validated else "No attempts passed validation",
            )
        except Exception as exc:
            ctx.record_error(str(exc), phase="repair", exc=exc)
            result = AgentResult(
                role=self.role,
                agent_id=self.agent_id,
                ok=False,
                error=str(exc),
            )
        self._log_result(ctx, result)
        return result

    async def _ensure_minimum_validated(
        self,
        ctx: PipelineContext,
        validated: list,
        strategies: list[str],
    ) -> list:
        """Ensure at least two attempts pass validation (ranker requirement)."""
        from core.models import BuildAttempt

        fb = self._fallback_generator or _default_fallback
        code_type = ctx.request.code_type.value
        description = ctx.request.description
        stacks = ctx.plan.tool_combinations
        seen_ids = {a.attempt_id for a in validated}
        need = max(0, 2 - len(validated))

        for i, stack in enumerate(stacks[:need]):
            code = fb(code_type, description, stack)
            attempt = BuildAttempt(
                attempt_id=f"validated_fallback_{uuid.uuid4().hex[:8]}",
                attempt_number=len(validated) + i + 1,
                tool_stack=stack,
                model_used="Validation Fallback",
                code_artifact=code,
                build_log=f"[{datetime.now().isoformat()}] Validation minimum fallback\n",
                tool_usage_report="Validation Fallback",
                build_time_seconds=0.5,
                success=False,
                error_message="",
                timestamp=datetime.now().isoformat(),
            )
            _, report = await validation_repair_loop(
                attempt,
                self._gate,
                max_rounds=1,
                code_type=code_type,
                description=description,
                tool_stack=stack,
            )
            ctx.validation.by_attempt[attempt.attempt_id] = report.to_dict()
            if report.passed and attempt.attempt_id not in seen_ids:
                validated.append(attempt)
                seen_ids.add(attempt.attempt_id)
                strategies.append("validation_minimum_fallback")

        return validated


def _default_fallback(code_type: str, description: str, stack: Any) -> str:
    from core.pipeline_domain import FallbackCodeGenerator

    return FallbackCodeGenerator.generate(code_type, description, stack)
