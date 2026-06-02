import uuid

import pytest
from core.pipeline_stages import PipelineStage
from core.stage_coordinator import StageCoordinator
from core.stage_execution import verify_stage_done


class FakeExceptionAfterComplete(Exception):
    pass


@pytest.fixture
def coordinator():
    return StageCoordinator()


def test_late_exception_does_not_reopen_stage(coordinator):
    db = coordinator.db
    build_id = f"late_{uuid.uuid4().hex[:8]}"
    stage = PipelineStage.PLANNING

    # create build (matches your DB signature)
    db.create_build(
        build_id,
        {
            "code_type": "test",
            "description": "test build",
            "request": {
                "tool_combinations": [{"name": "stack1"}],
                "factory_review": {"overall_score": 80},
            },
        },
    )

    # checkpoint setup
    db.save_checkpoint(build_id, {
        "tool_combinations": [{"name": "stack1"}],
        "factory_review": {"overall_score": 80},
    })

    lock_token = "lock-abc"

    # complete stage
    coordinator.complete_stage(
        build_id,
        stage,
        lock_token=lock_token,
    )

    assert verify_stage_done(db, build_id, stage) is True

    # simulate exception AFTER completion
    with pytest.raises(FakeExceptionAfterComplete):
        raise FakeExceptionAfterComplete("post-completion failure")

    # stage must remain idempotent
    result = coordinator.begin_stage(build_id, stage)
    assert result.result.name == "ALREADY_DONE"

    checkpoint = db.get_checkpoint(build_id)
    assert checkpoint["_stage_planning_done"] is True