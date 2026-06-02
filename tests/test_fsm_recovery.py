"""FSM recovery transitions and resume stage resolution."""

import os
import uuid

os.environ["USE_CELERY"] = "false"

from core.database import get_database  # noqa: E402

get_database().init_db()
from core.pipeline_fsm import (  # noqa: E402
    InvalidPipelineTransition,
    PipelineState,
    PipelineStateMachine,
    can_transition,
)
from core.pipeline_stages import PipelineStage, stage_for_resume  # noqa: E402
from core.stage_execution import stage_done_marker  # noqa: E402
from core.stage_state_machine import (  # noqa: E402
    prepare_recovery_transition,
    resolve_resume_stage,
)


def _new_build_id() -> str:
    return f"fsm_{uuid.uuid4().hex[:8]}"


def test_interrupted_without_phase_uses_checkpoints_not_planning():
    db = get_database()
    build_id = _new_build_id()
    db.create_build(
        build_id,
        {
            "code_type": "website",
            "description": "FSM recovery test build",
        },
    )
    db.save_checkpoint(
        build_id,
        {
            "tool_combinations": [{"name": "stack1"}],
            "factory_review": {"overall_score": 80},
            stage_done_marker(PipelineStage.PLANNING): True,
        },
    )
    db.complete_stage_run(build_id, PipelineStage.PLANNING.value)
    db.apply_pipeline_state(
        build_id,
        pipeline_status=PipelineState.INTERRUPTED.value,
        phase="builders",
        message="Interrupted during generating",
        percent=25.0,
    )

    assert stage_for_resume("interrupted", resume_phase=None) is None
    assert resolve_resume_stage(db, build_id) == PipelineStage.BUILDER


def test_interrupted_resume_phase_never_regresses_past_checkpoints():
    db = get_database()
    build_id = _new_build_id()
    db.create_build(
        build_id,
        {
            "code_type": "website",
            "description": "FSM phase hint test",
        },
    )
    db.save_checkpoint(
        build_id,
        {
            "tool_combinations": [{"name": "stack1"}],
            "factory_review": {"overall_score": 80},
            stage_done_marker(PipelineStage.PLANNING): True,
        },
    )
    db.complete_stage_run(build_id, PipelineStage.PLANNING.value)
    db.apply_pipeline_state(
        build_id,
        pipeline_status=PipelineState.INTERRUPTED.value,
        phase="factory_builder",
        message="Interrupted",
        percent=10.0,
    )

    assert stage_for_resume("interrupted", resume_phase="factory_builder") == PipelineStage.PLANNING
    assert resolve_resume_stage(db, build_id) == PipelineStage.BUILDER


def test_prepare_recovery_generating_to_planning_via_interrupted():
    db = get_database()
    build_id = _new_build_id()
    db.create_build(
        build_id,
        {
            "code_type": "website",
            "description": "Stale generating recovery",
        },
    )
    db.apply_pipeline_state(
        build_id,
        pipeline_status=PipelineState.GENERATING.value,
        phase="builders",
        message="Stale run",
        percent=20.0,
    )

    prepare_recovery_transition(db, build_id, PipelineStage.PLANNING)
    progress = db.get_progress(build_id)
    assert progress["pipeline_status"] == PipelineState.PLANNING.value

    fsm = PipelineStateMachine(db, build_id)
    fsm.enter(
        PipelineState.PLANNING,
        "factory_builder",
        "Planner running",
        10.0,
        event="phase",
    )


def test_can_transition_interrupted_to_planning_explicit():
    assert can_transition(PipelineState.INTERRUPTED.value, PipelineState.PLANNING.value)
    assert not can_transition(PipelineState.GENERATING.value, PipelineState.PLANNING.value)


def test_new_build_prepare_queued_to_planning():
    db = get_database()
    build_id = _new_build_id()
    db.create_build(
        build_id,
        {
            "code_type": "website",
            "description": "New build FSM",
        },
    )
    prepare_recovery_transition(db, build_id, PipelineStage.PLANNING)
    assert db.get_progress(build_id)["pipeline_status"] == PipelineState.PLANNING.value


def _run_all() -> None:
    test_interrupted_without_phase_uses_checkpoints_not_planning()
    test_interrupted_resume_phase_never_regresses_past_checkpoints()
    test_prepare_recovery_generating_to_planning_via_interrupted()
    test_can_transition_interrupted_to_planning_explicit()
    test_new_build_prepare_queued_to_planning()
    print("test_fsm_recovery: OK")


if __name__ == "__main__":
    _run_all()
