"""
Deterministic pipeline state machine — the orchestration spine.

Lifecycle (happy path):
  queued → planning → generating → reviewing → ranking → packaging → complete

Terminal / recovery:
  failed (from any active state), interrupted (server restart)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

from core.database import AppDatabase, RESUMABLE_STATUSES

logger = logging.getLogger("app_garden.pipeline_fsm")


class PipelineState(str, Enum):
    QUEUED = "queued"
    PLANNING = "planning"
    GENERATING = "generating"
    REVIEWING = "reviewing"
    RANKING = "ranking"
    PACKAGING = "packaging"
    COMPLETE = "complete"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


# Canonical factory line — index defines legal forward progression
LINEAR_PIPELINE: tuple[PipelineState, ...] = (
    PipelineState.QUEUED,
    PipelineState.PLANNING,
    PipelineState.GENERATING,
    PipelineState.REVIEWING,
    PipelineState.RANKING,
    PipelineState.PACKAGING,
    PipelineState.COMPLETE,
)

TERMINAL_STATES = frozenset(
    {PipelineState.COMPLETE, PipelineState.FAILED, PipelineState.INTERRUPTED}
)

# Sub-phases mapped to their parent pipeline state
PHASE_TO_STATE: dict[str, PipelineState] = {
    "queued": PipelineState.QUEUED,
    "factory_builder": PipelineState.PLANNING,
    "factory_review": PipelineState.PLANNING,
    "builders": PipelineState.GENERATING,
    "responsible_builder": PipelineState.GENERATING,
    "creative_builder": PipelineState.GENERATING,
    "builder_review": PipelineState.GENERATING,
    "validation": PipelineState.GENERATING,
    "app_review": PipelineState.REVIEWING,
    "reviewer": PipelineState.REVIEWING,
    "ranker": PipelineState.RANKING,
    "novelty": PipelineState.RANKING,
    "packaging": PipelineState.PACKAGING,
    "leaderboard": PipelineState.PACKAGING,
    "complete": PipelineState.COMPLETE,
    "failed": PipelineState.FAILED,
    "resume": PipelineState.QUEUED,
}


@dataclass(frozen=True)
class StageDefinition:
    """One pipeline state and its checkpoint artifact keys."""

    state: PipelineState
    phases: tuple[str, ...]
    checkpoint_keys: tuple[str, ...]
    percent_enter: float
    percent_exit: float


STAGE_DEFINITIONS: tuple[StageDefinition, ...] = (
    StageDefinition(
        PipelineState.PLANNING,
        ("factory_builder", "factory_review"),
        ("tool_combinations", "factory_review", "factory_score"),
        5.0,
        20.0,
    ),
    StageDefinition(
        PipelineState.GENERATING,
        ("builders", "builder_review"),
        ("resp_attempts", "creative_attempts", "all_attempts", "builder_reviews"),
        20.0,
        50.0,
    ),
    StageDefinition(
        PipelineState.REVIEWING,
        ("app_review", "reviewer"),
        ("app_reviews",),
        50.0,
        65.0,
    ),
    StageDefinition(
        PipelineState.RANKING,
        ("ranker", "novelty"),
        ("ranked_builds", "novelty_attempts", "final_code"),
        65.0,
        88.0,
    ),
    StageDefinition(
        PipelineState.PACKAGING,
        ("packaging", "leaderboard"),
        ("zip_path",),
        88.0,
        99.0,
    ),
)

STAGE_BY_STATE: dict[PipelineState, StageDefinition] = {
    s.state: s for s in STAGE_DEFINITIONS
}


def state_for_phase(phase: str) -> PipelineState:
    return PHASE_TO_STATE.get(phase, PipelineState.GENERATING)


def can_transition(from_status: str, to_status: str) -> bool:
    """Validate deterministic transitions."""
    try:
        from_s = PipelineState(from_status)
        to_s = PipelineState(to_status)
    except ValueError:
        return False

    if to_s == PipelineState.FAILED:
        return from_s not in TERMINAL_STATES

    if to_s == PipelineState.INTERRUPTED:
        return from_s not in TERMINAL_STATES

    # Resume after server restart: re-enter any active factory stage (not a dead end)
    if from_s == PipelineState.INTERRUPTED and to_s in LINEAR_PIPELINE:
        return to_s != PipelineState.QUEUED

    if from_s in TERMINAL_STATES:
        return False

    if from_s not in LINEAR_PIPELINE or to_s not in LINEAR_PIPELINE:
        return False

    return LINEAR_PIPELINE.index(to_s) >= LINEAR_PIPELINE.index(from_s)


class PipelineStateMachine:
    """
    Single-build orchestration spine. All status changes go through here so
    every transition is validated, logged, and persisted.
    """

    def __init__(self, db: AppDatabase, build_id: str) -> None:
        self.db = db
        self.build_id = build_id
        progress = db.get_progress(build_id)
        raw = progress.get("pipeline_status", PipelineState.QUEUED.value)
        try:
            self._current = PipelineState(raw)
        except ValueError:
            self._current = PipelineState.QUEUED

    @property
    def current_state(self) -> PipelineState:
        return self._current

    def enter(
        self,
        state: PipelineState | str,
        phase: str,
        message: str,
        percent: float,
        *,
        event: str = "enter",
    ) -> None:
        """Transition to a pipeline state (validated) and update live progress."""
        to_state = PipelineState(state) if isinstance(state, str) else state
        from_status = self._current.value

        to_value = to_state.value
        if from_status != to_value and not can_transition(from_status, to_value):
            raise InvalidPipelineTransition(
                f"Illegal transition {from_status} → {to_value} for build {self.build_id}"
            )

        if from_status != to_value:
            self.db.record_transition(
                self.build_id,
                from_status=from_status,
                to_status=to_value,
                phase=phase,
                event=event,
                message=message,
                percent=percent,
            )
            self._current = to_state

        self.db.apply_pipeline_state(
            self.build_id,
            pipeline_status=to_value,
            phase=phase,
            message=message,
            percent=percent,
        )
        self.db.append_log(
            self.build_id,
            phase,
            message,
            level="info" if to_state != PipelineState.FAILED else "error",
        )
        self.db.save_checkpoint(
            self.build_id,
            {
                "percent": percent,
                "resume_phase": phase,
                "last_pipeline_state": to_value,
            },
        )

    def phase(self, phase: str, message: str, percent: float) -> None:
        """Update sub-phase within the current state (no state change)."""
        state = state_for_phase(phase)
        if state != self._current and state != PipelineState.FAILED:
            self.enter(state, phase, message, percent, event="phase")
            return

        self.db.record_transition(
            self.build_id,
            from_status=self._current.value,
            to_status=self._current.value,
            phase=phase,
            event="phase",
            message=message,
            percent=percent,
        )
        self.db.apply_pipeline_state(
            self.build_id,
            pipeline_status=self._current,
            phase=phase,
            message=message,
            percent=percent,
        )
        self.db.append_log(self.build_id, phase, message)
        self.db.save_checkpoint(
            self.build_id,
            {
                "percent": percent,
                "resume_phase": phase,
            },
        )

    def stage_checkpoint(self, stage: PipelineState | str, **data: Any) -> None:
        """Persist stage artifacts and log checkpoint completion."""
        stage_state = PipelineState(stage) if isinstance(stage, str) else stage
        stage_def = STAGE_BY_STATE.get(stage_state)
        keys = list(data.keys())
        payload = {**data, f"_stage_{stage_state.value}_done": True}

        self.db.save_checkpoint(self.build_id, payload)
        self.db.record_transition(
            self.build_id,
            from_status=self._current.value,
            to_status=self._current.value,
            phase=stage_def.phases[-1] if stage_def else self._current.value,
            event="checkpoint",
            message=f"Checkpoint saved: {', '.join(keys)}",
            percent=data.get("percent"),
            checkpoint_keys=keys,
        )
        self.db.append_log(
            self.build_id,
            stage_state.value,
            f"Stage checkpoint [{stage_state.value}]: {', '.join(keys)}",
            level="info",
        )
        logger.debug(
            "Build %s checkpoint @ %s: %s",
            self.build_id,
            stage_state.value,
            keys,
        )

    def begin_stage(
        self,
        state: PipelineState | str,
        phase: str,
        message: str,
        *,
        percent: Optional[float] = None,
    ) -> None:
        """Enter a major pipeline stage (state transition)."""
        stage_state = PipelineState(state) if isinstance(state, str) else state
        stage_def = STAGE_BY_STATE.get(stage_state)
        pct = percent if percent is not None else (stage_def.percent_enter if stage_def else 0.0)
        self.enter(stage_state, phase, message, pct, event="stage_begin")

    def fail(self, error: str, *, phase: str = "failed", traceback_text: str = "") -> None:
        """Capture failure and move to terminal failed state."""
        detail = traceback_text or error
        from_status = self._current.value
        self.db.update_build(self.build_id, status="failed", error=detail, completed=True)
        self.db.record_transition(
            self.build_id,
            from_status=from_status,
            to_status=PipelineState.FAILED.value,
            phase=phase,
            event="fail",
            message=error[:500],
            percent=100.0,
            error_text=detail,
        )
        self.db.apply_pipeline_state(
            self.build_id,
            pipeline_status=PipelineState.FAILED.value,
            phase=phase,
            message=error[:500],
            percent=100.0,
        )
        self.db.append_log(self.build_id, phase, error, level="error")
        self.db.save_checkpoint(
            self.build_id,
            {
                "failed": True,
                "error": error,
                "traceback": traceback_text,
            },
        )
        self._current = PipelineState.FAILED

    def complete(self, message: str, *, phase: str = "complete") -> None:
        """Terminal success transition."""
        self.enter(PipelineState.COMPLETE, phase, message, 100.0, event="complete")
        self.db.update_build(self.build_id, status="complete", completed=True)

    def lifecycle(self) -> list[dict[str, Any]]:
        """Full auditable transition history for this build."""
        return self.db.get_lifecycle(self.build_id)

    def snapshot(self) -> dict[str, Any]:
        """Current FSM position plus lifecycle summary."""
        progress = self.db.get_progress(self.build_id)
        transitions = self.lifecycle()
        return {
            "build_id": self.build_id,
            "current_state": self._current.value,
            "progress": progress,
            "transition_count": len(transitions),
            "lifecycle": transitions,
            "linear_order": [s.value for s in LINEAR_PIPELINE],
        }


class InvalidPipelineTransition(Exception):
    """Raised when orchestration violates the deterministic state graph."""
