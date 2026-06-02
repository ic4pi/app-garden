"""Celery tasks — guarded stage execution, recovery, and periodic stuck-build sweep."""

from __future__ import annotations

import logging

from worker.celery_app import celery_app

from core.config import Config
from core.pipeline_stages import PipelineStage
from core.stage_coordinator import (
    auto_resume_on_startup,
    recover_stuck_builds,
    run_stage_guarded,
    try_startup_recovery_lock,
)
from core.worker_runtime import ensure_kernel, run_async

logger = logging.getLogger("app_garden.tasks")


@celery_app.task(bind=True, name="worker.tasks.run_stage", max_retries=3)
def run_stage(self, build_id: str, stage_name: str) -> dict:
    """Execute one pipeline stage under build/stage lock with idempotent guards."""
    ensure_kernel()
    stage = PipelineStage(stage_name)
    worker_id = self.request.id or "celery"
    logger.info("Running stage %s for build %s (task=%s)", stage_name, build_id, worker_id)

    result = run_async(
        run_stage_guarded(build_id, stage, worker_id=worker_id)
    )

    if result.get("status") == "locked":
        raise self.retry(countdown=int(getattr(Config, "STAGE_LOCK_RETRY_SECONDS", 45)))

    if result.get("status") == "failed":
        logger.error(
            "Build %s failed at stage %s: %s",
            build_id,
            stage_name,
            result.get("error"),
        )
    elif result.get("skipped"):
        logger.info("Build %s stage %s skipped (already done)", build_id, stage_name)
    elif stage == PipelineStage.FINALIZE:
        logger.info("Build %s completed", build_id)

    return result


@celery_app.task(bind=True, name="worker.tasks.dispatch_build", max_retries=1)
def dispatch_build(self, build_id: str) -> dict:
    """Entry task: resolve resume point and enqueue the first incomplete stage."""
    from core.stage_coordinator import dispatch_build_recovery

    ensure_kernel()
    task_id = dispatch_build_recovery(build_id)
    return {"build_id": build_id, "enqueued": task_id}


@celery_app.task(name="worker.tasks.recover_stuck_builds")
def recover_stuck_builds_task() -> dict:
    """Periodic: requeue missing stages for builds stuck past the threshold."""
    ensure_kernel()
    dispatched = recover_stuck_builds(
        limit=int(getattr(Config, "RECOVERY_BATCH_LIMIT", 50)),
    )
    return {"dispatched": dispatched, "count": len(dispatched)}


@celery_app.task(name="worker.tasks.startup_auto_resume")
def startup_auto_resume_task() -> dict:
    """On worker boot: scan interrupted/stale builds and re-dispatch."""
    ensure_kernel()
    if not try_startup_recovery_lock():
        return {"skipped": True, "reason": "another_worker_handled_startup"}
    return auto_resume_on_startup(
        limit=int(getattr(Config, "RECOVERY_BATCH_LIMIT", 100)),
    )
