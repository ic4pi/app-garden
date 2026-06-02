"""Tests for stage locks and idempotency (no Celery/Redis)."""

import asyncio
import os
import uuid
from datetime import datetime, timezone

os.environ["USE_CELERY"] = "false"

from core.config import Config
from core.database import get_database

get_database().init_db()
from core.pipeline_stages import PipelineStage
from core.stage_execution import (
    checkpoint_satisfies_stage,
    first_incomplete_stage,
    verify_stage_done,
)
from core.stage_coordinator import StageCoordinator, StageBeginResult, run_stage_guarded


def test_checkpoint_satisfies_planning():
    ckpt = {
        "tool_combinations": [{}],
        "factory_review": {"score": 1},
        "_stage_planning_done": True,
    }
    assert checkpoint_satisfies_stage(ckpt, PipelineStage.PLANNING)


def test_stage_lock_exclusive():
    db = get_database()
    build_id = f"lock_{uuid.uuid4().hex[:8]}"
    db.create_build(
        build_id,
        {"code_type": "website", "description": "lock test build x"},
    )
    coord = StageCoordinator(db)
    a = coord.begin_stage(build_id, PipelineStage.PLANNING, worker_id="w1")
    assert a.result == StageBeginResult.PROCEED
    b = coord.begin_stage(build_id, PipelineStage.PLANNING, worker_id="w2")
    assert b.result == StageBeginResult.LOCKED
    db.save_checkpoint(
        build_id,
        {
            "tool_combinations": [{"name": "s"}],
            "factory_review": {"overall_score": 1},
            "_stage_planning_done": True,
        },
    )
    db.complete_stage_run(build_id, PipelineStage.PLANNING.value, lock_token=a.lock_token)
    assert verify_stage_done(db, build_id, PipelineStage.PLANNING)
    c = coord.begin_stage(build_id, PipelineStage.PLANNING, worker_id="w3")
    assert c.result == StageBeginResult.ALREADY_DONE


def test_begin_stage_skips_failed_stage():
    db = get_database()
    build_id = f"failed_{uuid.uuid4().hex[:8]}"
    db.create_build(
        build_id,
        {"code_type": "website", "description": "failed terminal stage test"},
    )
    db.fail_stage_run(build_id, PipelineStage.PLANNING.value, error="fatal error")
    coord = StageCoordinator(db)
    result = coord.begin_stage(build_id, PipelineStage.PLANNING, worker_id="w1")
    assert result.result == StageBeginResult.ALREADY_DONE


def test_first_incomplete_after_planning():
    db = get_database()
    build_id = f"inc_{uuid.uuid4().hex[:8]}"
    db.create_build(
        build_id,
        {"code_type": "website", "description": "incomplete stage test x"},
    )
    db.save_checkpoint(
        build_id,
        {
            "tool_combinations": [{"name": "s"}],
            "factory_review": {"overall_score": 1},
        },
    )
    db.save_checkpoint(
        build_id,
        {"_stage_planning_done": True},
    )
    db.complete_stage_run(build_id, PipelineStage.PLANNING.value)
    assert first_incomplete_stage(db, build_id) == PipelineStage.BUILDER


def test_try_acquire_stage_lock_allows_expired_running_reclaim():
    db = get_database()
    build_id = f"lock_{uuid.uuid4().hex[:8]}"
    db.create_build(
        build_id,
        {"code_type": "website", "description": "lock reclaim test"},
    )
    ok, token1 = db.try_acquire_stage_lock(build_id, PipelineStage.PLANNING.value, "w1", ttl_seconds=1)
    assert ok and token1

    past = datetime(2000, 1, 1, tzinfo=timezone.utc).isoformat()
    with db._connect() as conn:
        conn.execute(
            "UPDATE pipeline_stage_runs SET expires_at = ? WHERE build_id = ? AND stage = ?",
            (past, build_id, PipelineStage.PLANNING.value),
        )

    ok2, token2 = db.try_acquire_stage_lock(build_id, PipelineStage.PLANNING.value, "w2", ttl_seconds=3600)
    assert ok2 and token2 and token2 != token1


def test_recover_expired_stage_run_requeues_pending():
    db = get_database()
    build_id = f"recover_{uuid.uuid4().hex[:8]}"
    db.create_build(
        build_id,
        {"code_type": "website", "description": "expire recover test"},
    )
    ok, token = db.try_acquire_stage_lock(build_id, PipelineStage.PLANNING.value, "w1", ttl_seconds=1)
    assert ok and token

    past = datetime(2000, 1, 1, tzinfo=timezone.utc).isoformat()
    with db._connect() as conn:
        conn.execute(
            "UPDATE pipeline_stage_runs SET expires_at = ? WHERE build_id = ? AND stage = ?",
            (past, build_id, PipelineStage.PLANNING.value),
        )

    coordinator = StageCoordinator(db)
    recovered = coordinator.recover_stuck_builds(limit=10)
    assert build_id in recovered
    stage_row = db.get_stage_run(build_id, PipelineStage.PLANNING.value)
    assert stage_row and stage_row["status"] == "pending"


def test_recover_expired_stage_run_fails_after_max_attempts():
    db = get_database()
    build_id = f"recover_{uuid.uuid4().hex[:8]}"
    db.create_build(
        build_id,
        {"code_type": "website", "description": "max attempt fail test"},
    )
    max_attempts = int(getattr(Config, "STAGE_MAX_ATTEMPTS", 3))
    ok, token = db.try_acquire_stage_lock(build_id, PipelineStage.PLANNING.value, "w1", ttl_seconds=1)
    assert ok and token

    past = datetime(2000, 1, 1, tzinfo=timezone.utc).isoformat()
    with db._connect() as conn:
        conn.execute(
            "UPDATE pipeline_stage_runs SET expires_at = ?, attempt = ? WHERE build_id = ? AND stage = ?",
            (past, max_attempts, build_id, PipelineStage.PLANNING.value),
        )

    coordinator = StageCoordinator(db)
    recovered = coordinator.recover_stuck_builds(limit=10)
    assert build_id not in recovered
    stage_row = db.get_stage_run(build_id, PipelineStage.PLANNING.value)
    assert stage_row and stage_row["status"] == "failed"


def test_verify_stage_done_from_checkpoint_without_stage_run():
    db = get_database()
    build_id = f"ckpt_{uuid.uuid4().hex[:8]}"
    db.create_build(
        build_id,
        {"code_type": "website", "description": "checkpoint only test"},
    )
    db.save_checkpoint(
        build_id,
        {
            "tool_combinations": [{"name": "s"}],
            "factory_review": {"overall_score": 90},
            "_stage_planning_done": True,
        },
    )
    assert verify_stage_done(db, build_id, PipelineStage.PLANNING)


def test_complete_stage_with_checkpoint_persists_planning_artifacts():
    db = get_database()
    build_id = f"atomic_{uuid.uuid4().hex[:8]}"
    db.create_build(
        build_id,
        {"code_type": "website", "description": "atomic planning test"},
    )
    db.save_checkpoint(
        build_id,
        {
            "tool_combinations": [{"name": "s"}],
            "factory_review": {"overall_score": 90},
        },
    )

    db.complete_stage_with_checkpoint(build_id, PipelineStage.PLANNING.value, lock_token="lock-123")

    checkpoint = db.get_checkpoint(build_id)
    assert checkpoint["_stage_planning_done"] is True
    assert checkpoint["tool_combinations"] == [{"name": "s"}]
    assert checkpoint["factory_review"] == {"overall_score": 90}

    stage_row = db.get_stage_run(build_id, PipelineStage.PLANNING.value)
    assert stage_row and stage_row["status"] == "completed"


def test_planning_stage_falls_back_to_default_factory_review(monkeypatch):
    db = get_database()
    build_id = f"fallback_{uuid.uuid4().hex[:8]}"
    db.create_build(
        build_id,
        {
            "code_type": "website",
            "description": "planning fallback test build request",
        },
    )

    async def fake_review_plan(self, ctx):
        return {}

    monkeypatch.setattr(
        "core.pipeline_domain.FactoryReviewer.review_plan",
        fake_review_plan,
    )

    result = asyncio.run(run_stage_guarded(build_id, PipelineStage.PLANNING, worker_id="inline-test"))

    assert result["status"] == "ok"
    checkpoint = db.get_checkpoint(build_id)
    assert checkpoint["_stage_planning_done"] is True
    assert checkpoint["factory_review"]["overall_score"] == 50
    stage_row = db.get_stage_run(build_id, PipelineStage.PLANNING.value)
    assert stage_row and stage_row["status"] == "completed"


def test_fail_build_terminally_marks_build():
    db = get_database()
    build_id = f"fail_{uuid.uuid4().hex[:8]}"
    db.create_build(
        build_id,
        {"code_type": "website", "description": "fail build test"},
    )
    coord = StageCoordinator(db)
    coord.fail_build(
        build_id,
        PipelineStage.PLANNING,
        lock_token=None,
        error="fatal stage error",
    )
    assert db.get_build(build_id)["status"] == "failed"
    progress = db.get_progress(build_id)
    assert progress["pipeline_status"] == "failed"
    stage_row = db.get_stage_run(build_id, PipelineStage.PLANNING.value)
    assert stage_row and stage_row["status"] == "failed"


def test_fail_build_skipped_for_completed_stage():
    db = get_database()
    build_id = f"skip_{uuid.uuid4().hex[:8]}"
    db.create_build(
        build_id,
        {"code_type": "website", "description": "completed stage skip fail"},
    )
    db.save_checkpoint(
        build_id,
        {
            "tool_combinations": [{"name": "s"}],
            "factory_review": {"overall_score": 1},
            "_stage_planning_done": True,
        },
    )
    db.complete_stage_run(build_id, PipelineStage.PLANNING.value)
    coord = StageCoordinator(db)
    coord.fail_build(
        build_id,
        PipelineStage.PLANNING,
        lock_token=None,
        error="should not overwrite completed stage",
    )
    assert db.get_build(build_id)["status"] != "failed"
    stage_row = db.get_stage_run(build_id, PipelineStage.PLANNING.value)
    assert stage_row and stage_row["status"] == "completed"


def test_run_stage_guarded_ignores_late_exception_after_completion(monkeypatch):
    db = get_database()
    build_id = f"late_{uuid.uuid4().hex[:8]}"
    db.create_build(
        build_id,
        {"code_type": "website", "description": "late exception after completion test"},
    )

    async def fake_run_pipeline_stage(build_id_arg, stage_arg):
        return {"status": "ok", "stage": stage_arg.value, "build_id": build_id_arg}

    monkeypatch.setattr("core.stage_coordinator.run_pipeline_stage", fake_run_pipeline_stage)

    original_complete_stage = StageCoordinator.complete_stage

    def complete_stage_then_raise(self, build_id_arg, stage_arg, *, lock_token=None):
        original_complete_stage(self, build_id_arg, stage_arg, lock_token=lock_token)
        raise RuntimeError("late cleanup failure")

    monkeypatch.setattr(StageCoordinator, "complete_stage", complete_stage_then_raise)

    result = asyncio.run(run_stage_guarded(build_id, PipelineStage.PLANNING, worker_id="inline-test"))

    assert result["status"] == "ok"
    assert result.get("skipped") is True
    assert result["reason"] == "already_done"

    stage_row = db.get_stage_run(build_id, PipelineStage.PLANNING.value)
    assert stage_row and stage_row["status"] == "completed"
    assert db.get_build(build_id)["status"] != "failed"
    progress = db.get_progress(build_id)
    assert progress["pipeline_status"] != "failed"
