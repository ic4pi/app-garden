"""Pipeline stage identifiers and Celery queue routing."""

from __future__ import annotations

from enum import Enum


class PipelineStage(str, Enum):
    PLANNING = "planning"
    BUILDER = "builder"
    REPAIR = "repair"
    REVIEWING = "reviewing"
    RANKING = "ranking"
    NOVELTY = "novelty"
    PACKAGING = "packaging"
    LEADERBOARD = "leaderboard"
    FINALIZE = "finalize"


# Worker pool queues (Celery -Q names)
QUEUE_PLANNER = "planner"
QUEUE_BUILDER = "builder"
QUEUE_REPAIR = "repair"
QUEUE_REVIEWER = "reviewer"
QUEUE_RANKER = "ranker"

STAGE_TO_QUEUE: dict[PipelineStage, str] = {
    PipelineStage.PLANNING: QUEUE_PLANNER,
    PipelineStage.BUILDER: QUEUE_BUILDER,
    PipelineStage.REPAIR: QUEUE_REPAIR,
    PipelineStage.REVIEWING: QUEUE_REVIEWER,
    PipelineStage.RANKING: QUEUE_RANKER,
    PipelineStage.NOVELTY: QUEUE_RANKER,
    PipelineStage.PACKAGING: QUEUE_RANKER,
    PipelineStage.LEADERBOARD: QUEUE_RANKER,
    PipelineStage.FINALIZE: QUEUE_RANKER,
}

# Order for dispatch after a stage completes successfully
STAGE_ORDER: tuple[PipelineStage, ...] = (
    PipelineStage.PLANNING,
    PipelineStage.BUILDER,
    PipelineStage.REPAIR,
    PipelineStage.REVIEWING,
    PipelineStage.RANKING,
    PipelineStage.NOVELTY,
    PipelineStage.PACKAGING,
    PipelineStage.LEADERBOARD,
    PipelineStage.FINALIZE,
)


def next_stage(current: PipelineStage) -> PipelineStage | None:
    try:
        idx = STAGE_ORDER.index(current)
    except ValueError:
        return None
    if idx + 1 >= len(STAGE_ORDER):
        return None
    return STAGE_ORDER[idx + 1]


# Map DB pipeline_status → worker stage hint (not used for interrupted — use checkpoints)
STATUS_TO_STAGE: dict[str, PipelineStage] = {
    "queued": PipelineStage.PLANNING,
    "planning": PipelineStage.PLANNING,
    "generating": PipelineStage.BUILDER,
    "reviewing": PipelineStage.REVIEWING,
    "ranking": PipelineStage.RANKING,
    "packaging": PipelineStage.PACKAGING,
}

RESUME_PHASE_TO_STAGE: dict[str, PipelineStage] = {
    "factory_builder": PipelineStage.PLANNING,
    "factory_review": PipelineStage.PLANNING,
    "builders": PipelineStage.BUILDER,
    "responsible_builder": PipelineStage.BUILDER,
    "creative_builder": PipelineStage.BUILDER,
    "builder_review": PipelineStage.REVIEWING,
    "validation": PipelineStage.REPAIR,
    "app_review": PipelineStage.REVIEWING,
    "reviewer": PipelineStage.REVIEWING,
    "ranker": PipelineStage.RANKING,
    "novelty": PipelineStage.NOVELTY,
    "packaging": PipelineStage.PACKAGING,
    "leaderboard": PipelineStage.LEADERBOARD,
}


def stage_for_resume(
    pipeline_status: str,
    *,
    resume_phase: str | None = None,
) -> PipelineStage | None:
    """Hint stage from resume_phase or active pipeline_status (never guess for interrupted)."""
    if resume_phase and resume_phase in RESUME_PHASE_TO_STAGE:
        return RESUME_PHASE_TO_STAGE[resume_phase]
    if pipeline_status == "interrupted":
        return None
    return STATUS_TO_STAGE.get(pipeline_status)
