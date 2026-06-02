"""Load/save PipelineContext from DB only — no global pipeline state."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional

from core.database import AppDatabase, get_database
from core.pipeline_context import PipelineContext
from core.pipeline_fsm import PipelineStateMachine
from core.pipeline_stages import PipelineStage


@dataclass(frozen=True)
class PipelineServices:
    """Stateless capabilities (LLM, packaging). Not used as pipeline state."""

    orchestrator: Any
    download_manager: Any
    leaderboard: Any
    stack_factory: Optional[Callable[[list], list]] = None
    attempt_factory: Optional[Callable[[list], list]] = None
    ranked_factory: Optional[Callable[[list], list]] = None
    review_factory: Optional[Callable[[list], list]] = None
    novelty_factory: Optional[Callable[[list], list]] = None


def _services_from_orchestrator(orchestrator: Any) -> PipelineServices:
    from core.models import NoveltyAttempt, ReviewReport

    return PipelineServices(
        orchestrator=orchestrator,
        download_manager=orchestrator.download_manager,
        leaderboard=orchestrator.leaderboard,
        stack_factory=orchestrator._stacks_from_checkpoint,
        attempt_factory=orchestrator._attempts_from_checkpoint,
        ranked_factory=orchestrator._ranked_from_checkpoint,
        review_factory=lambda data: [ReviewReport(**r) for r in data],
        novelty_factory=lambda data: [NoveltyAttempt(**a) for a in data],
    )


class PipelineContextStore:
    """Single entry point for constructing context from persisted state."""

    def __init__(self, db: Optional[AppDatabase] = None) -> None:
        self.db = db or get_database()

    def load(
        self,
        build_id: str,
        stage: PipelineStage,
        services: PipelineServices,
    ) -> PipelineContext:
        """Hydrate full context from DB; set active stage."""
        build = self.db.get_build(build_id)
        if not build:
            raise ValueError(f"Build not found: {build_id}")

        ctx = PipelineContext(
            build_id=build_id,
            db=self.db,
            fsm=PipelineStateMachine(self.db, build_id),
            services=services,
            stage=stage.value,
            prompt=dict(build.get("request") or {}),
        )
        ctx.hydrate_from_db()
        ctx.set_stage(stage)
        return ctx

    def persist_checkpoints(self, ctx: PipelineContext) -> None:
        """Flush in-memory checkpoint dict to DB (agents should use ctx.save_checkpoint)."""
        if ctx.checkpoints:
            self.db.save_checkpoint(ctx.build_id, ctx.checkpoints)
