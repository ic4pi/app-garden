"""Dispatch deduplication tests."""

import os
import uuid

os.environ["USE_CELERY"] = "false"

from core.database import get_database
from core.pipeline_stages import PipelineStage

get_database().init_db()


def test_dispatch_dedup_blocks_duplicate():
    db = get_database()
    build_id = f"dedup_{uuid.uuid4().hex[:8]}"
    db.create_build(
        build_id,
        {"code_type": "website", "description": "dispatch dedup test build"},
    )
    stage = PipelineStage.PLANNING.value
    assert db.try_claim_dispatch(build_id, stage) is True
    assert db.try_claim_dispatch(build_id, stage) is False
    db.clear_dispatch(build_id, stage)
    assert db.try_claim_dispatch(build_id, stage) is True
