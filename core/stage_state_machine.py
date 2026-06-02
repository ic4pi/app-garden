"""Canonical pipeline stage resolution — single source for 'what runs next'."""

from __future__ import annotations

import logging
from typing import Optional

from core.database import AppDatabase, get_database
from core.pipeline_fsm import (
    InvalidPipelineTransition,
    PipelineState,
    PipelineStateMachine,
    TERMINAL_STATES,
    can_transition,
)
from core.pipeline_stages import STAGE_ORDER, PipelineStage, stage_for_resume
from core.stage_execution import verify_stage_done

logger = logging.getLogger("app_garden.stage_state_machine")

# Worker stage → major FSM state (for legal recovery transitions)
STAGE_TO_PIPELINE_STATE: dict[PipelineStage, PipelineState] = {
    PipelineStage.PLANNING: PipelineState.PLANNING,
    PipelineStage.BUILDER: PipelineState.GENERATING,
    PipelineStage.REPAIR: PipelineState.GENERATING,
    PipelineStage.REVIEWING: PipelineState.REVIEWING,
    PipelineStage.RANKING: PipelineState.RANKING,
    PipelineStage.NOVELTY: PipelineState.RANKING,
    PipelineStage.PACKAGING: PipelineState.PACKAGING,
    PipelineStage.LEADERBOARD: PipelineState.PACKAGING,
    PipelineStage.FINALIZE: PipelineState.COMPLETE,
}

STAGE_DEFAULT_PHASE: dict[PipelineStage, str] = {
    PipelineStage.PLANNING: "factory_builder",
    PipelineStage.BUILDER: "builders",
    PipelineStage.REPAIR: "validation",
    PipelineStage.REVIEWING: "app_review",
    PipelineStage.RANKING: "ranker",
    PipelineStage.NOVELTY: "novelty",
    PipelineStage.PACKAGING: "packaging",
    PipelineStage.LEADERBOARD: "leaderboard",
    PipelineStage.FINALIZE: "complete",
}


def pipeline_state_for_stage(stage: PipelineStage) -> PipelineState:
    return STAGE_TO_PIPELINE_STATE[stage]


def _stage_index(stage: PipelineStage) -> int:
    return STAGE_ORDER.index(stage)


def _later_stage(a: PipelineStage, b: PipelineStage) -> PipelineStage:
    """Return the stage further along STAGE_ORDER (never regress artifact truth)."""
    return a if _stage_index(a) >= _stage_index(b) else b


def canonical_next_stage(
    db: AppDatabase,
    build_id: str,
    *,
    release_expired_locks: bool = True,
) -> Optional[PipelineStage]:
    """
    Return exactly one next stage to execute, or None if pipeline is complete.

    Rules (deterministic):
    1. Walk STAGE_ORDER in order.
    2. First stage that is not verify_stage_done() wins.
    3. Never skip ahead based on pipeline_status alone.
    """
    if release_expired_locks:
        db.release_expired_stage_locks()

    build = db.get_build(build_id)
    if not build:
        return None
    if build.get("status") in ("complete", "failed"):
        return None

    for stage in STAGE_ORDER:
        if not verify_stage_done(db, build_id, stage):
            return stage
    return None


def resolve_resume_stage(db: AppDatabase, build_id: str) -> Optional[PipelineStage]:
    """
    Pick the worker stage to (re)run from checkpoints, then refine with resume_phase.

    Checkpoint order (canonical_next_stage) is authoritative. resume_phase only
    prevents re-running an earlier stage when artifacts for later work already exist.
    """
    canonical = canonical_next_stage(db, build_id, release_expired_locks=False)
    if canonical is None:
        return None

    progress = db.get_progress(build_id)
    pipeline_status = progress.get("pipeline_status", "queued")
    resume_phase = progress.get("resume_phase")

    hinted = stage_for_resume(pipeline_status, resume_phase=resume_phase)
    if hinted is None:
        return canonical

    if pipeline_status == "interrupted":
        return _later_stage(canonical, hinted)

    # Stale active row (e.g. generating) while an earlier stage is incomplete
    if _stage_index(hinted) > _stage_index(canonical):
        return canonical
    return canonical


def prepare_recovery_transition(
    db: AppDatabase,
    build_id: str,
    target_stage: PipelineStage,
) -> None:
    """
    Move the build FSM to a legal state before enqueueing or running a stage.

    When the DB still shows an in-flight linear state (e.g. generating) but the
    next work is an earlier stage, transition through interrupted first, then
    forward to the target (explicitly allowed by can_transition).
    """
    progress = db.get_progress(build_id)
    if not progress or progress.get("pipeline_status") == "unknown":
        return

    fsm = PipelineStateMachine(db, build_id)
    from_state = fsm.current_state
    to_state = pipeline_state_for_stage(target_stage)
    phase = progress.get("resume_phase") or STAGE_DEFAULT_PHASE[target_stage]
    pct = float(progress.get("percent") or 0.0)

    if from_state == to_state:
        return

    if can_transition(from_state.value, to_state.value):
        fsm.enter(
            to_state,
            phase,
            f"Recovery: resume at {target_stage.value}",
            pct,
            event="recovery",
        )
        return

    if from_state not in TERMINAL_STATES and from_state != PipelineState.INTERRUPTED:
        if can_transition(from_state.value, PipelineState.INTERRUPTED.value):
            fsm.enter(
                PipelineState.INTERRUPTED,
                phase,
                "Recovery: normalize pipeline state before resume",
                pct,
                event="recovery_interrupt",
            )
            from_state = PipelineState.INTERRUPTED

    if not can_transition(from_state.value, to_state.value):
        raise InvalidPipelineTransition(
            f"Cannot recover build {build_id}: {from_state.value} → {to_state.value} "
            f"(target stage {target_stage.value})"
        )

    fsm.enter(
        to_state,
        phase,
        f"Recovery: resume at {target_stage.value}",
        pct,
        event="recovery",
    )
    logger.info(
        "Recovery transition for %s: %s → %s (stage=%s)",
        build_id,
        progress.get("pipeline_status"),
        to_state.value,
        target_stage.value,
    )


def is_pipeline_complete(db: AppDatabase, build_id: str) -> bool:
    return canonical_next_stage(db, build_id) is None
