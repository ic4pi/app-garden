"""Tests for pipeline stage routing and build enqueue (no Redis required)."""

import os
import uuid

os.environ["USE_CELERY"] = "false"

from core.pipeline_stages import (  # noqa: E402
    PipelineStage,
    next_stage,
    stage_for_resume,
    STAGE_ORDER,
)


def test_stage_order_covers_worker_types():
    assert PipelineStage.PLANNING in STAGE_ORDER
    assert PipelineStage.BUILDER in STAGE_ORDER
    assert PipelineStage.REPAIR in STAGE_ORDER
    assert PipelineStage.REVIEWING in STAGE_ORDER
    assert PipelineStage.FINALIZE in STAGE_ORDER


def test_next_stage_chain():
    assert next_stage(PipelineStage.PLANNING) == PipelineStage.BUILDER
    assert next_stage(PipelineStage.BUILDER) == PipelineStage.REPAIR
    assert next_stage(PipelineStage.REPAIR) == PipelineStage.REVIEWING
    assert next_stage(PipelineStage.REVIEWING) == PipelineStage.RANKING
    assert next_stage(PipelineStage.FINALIZE) is None


def test_stage_for_resume_from_phase():
    assert stage_for_resume("generating", resume_phase="builders") == PipelineStage.BUILDER
    assert stage_for_resume("generating", resume_phase="validation") == PipelineStage.REPAIR
    assert stage_for_resume("reviewing", resume_phase="app_review") == PipelineStage.REVIEWING


def test_stage_for_resume_interrupted_without_phase_is_none():
    assert stage_for_resume("interrupted", resume_phase=None) is None
    assert stage_for_resume("interrupted", resume_phase="builders") == PipelineStage.BUILDER


def test_start_new_build_creates_row(monkeypatch):
    from core.build_queue import start_new_build
    from core.database import get_database

    monkeypatch.setattr(
        "core.build_queue._run_inline_dispatch",
        lambda build_id, stage: f"mock-{build_id}",
    )

    build_id = f"test_{uuid.uuid4().hex[:8]}"
    start_new_build(
        {
            "code_type": "website",
            "description": "Test queue enqueue for worker pipeline",
            "preferred_frameworks": [],
        },
        build_id=build_id,
    )
    row = get_database().get_build(build_id)
    assert row is not None
    assert row["status"] == "queued"
