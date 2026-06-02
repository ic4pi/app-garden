"""Stage completion criteria and idempotent execution (DB-backed)."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

from core.database import AppDatabase
from core.pipeline_stages import PipelineStage

STAGE_REQUIRED_CHECKPOINT_KEYS: dict[PipelineStage, tuple[str, ...]] = {
    PipelineStage.PLANNING: ("tool_combinations", "factory_review"),
    PipelineStage.BUILDER: ("all_attempts",),
    PipelineStage.REPAIR: ("validated_attempts", "validation"),
    PipelineStage.REVIEWING: ("app_reviews",),
    PipelineStage.RANKING: ("ranked_builds",),
    PipelineStage.NOVELTY: ("novelty_attempts", "final_code"),
    PipelineStage.PACKAGING: ("zip_path",),
    PipelineStage.LEADERBOARD: ("leaderboard_recorded",),
    PipelineStage.FINALIZE: (),
}


def stage_done_marker(stage: PipelineStage) -> str:
    return f"_stage_{stage.value}_done"


class StageBeginResult(str, Enum):
    PROCEED = "proceed"
    ALREADY_DONE = "already_done"
    LOCKED = "locked"
    DISPATCH_DUPLICATE = "dispatch_duplicate"


@dataclass
class StageGuardOutcome:
    result: StageBeginResult
    lock_token: Optional[str] = None


def checkpoint_satisfies_stage(checkpoint: dict[str, Any], stage: PipelineStage) -> bool:
    if stage == PipelineStage.FINALIZE:
        return bool(checkpoint.get("completed")) and bool(
            checkpoint.get(stage_done_marker(stage))
        )

    if stage == PipelineStage.NOVELTY:
        if not checkpoint.get(stage_done_marker(stage)):
            return False
        if not checkpoint.get("final_code"):
            return False
        return True

    required = STAGE_REQUIRED_CHECKPOINT_KEYS.get(stage, ())
    if not required:
        return False
    if not checkpoint.get(stage_done_marker(stage)):
        return False
    for key in required:
        if key not in checkpoint:
            return False
        if checkpoint[key] in (None, [], {}, "", False):
            return False
    return True


def verify_stage_done(db: AppDatabase, build_id: str, stage: PipelineStage) -> bool:
    """Strict idempotency: checkpoint marker + artifact truth are the source of truth."""
    checkpoint = db.get_checkpoint(build_id)

    if stage == PipelineStage.FINALIZE:
        build = db.get_build(build_id)
        if build and build.get("status") == "complete":
            return True
        return bool(checkpoint.get("completed")) and bool(
            checkpoint.get(stage_done_marker(stage))
        )

    return checkpoint_satisfies_stage(checkpoint, stage)


def first_incomplete_stage(db: AppDatabase, build_id: str) -> Optional[PipelineStage]:
    from core.pipeline_stages import STAGE_ORDER

    for stage in STAGE_ORDER:
        if not verify_stage_done(db, build_id, stage):
            return stage
    return None
