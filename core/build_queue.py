"""Enqueue pipeline work to Celery (or in-process fallback for local dev)."""

from __future__ import annotations

import logging
import uuid
from typing import Any, Optional

from core.config import Config
from core.database import get_database
from core.pipeline_stages import PipelineStage, STAGE_TO_QUEUE, next_stage

logger = logging.getLogger("app_garden.build_queue")

TASK_RUN_STAGE = "worker.tasks.run_stage"
TASK_DISPATCH = "worker.tasks.dispatch_build"

_broker_ping_ok: Optional[bool] = None


def _is_broker_reachable() -> bool:
    """Ping Redis broker once per process; avoids 500s when Celery is configured but Redis is down."""
    global _broker_ping_ok
    if _broker_ping_ok is not None:
        return _broker_ping_ok
    url = Config.CELERY_BROKER_URL
    if not url or not url.startswith("redis"):
        _broker_ping_ok = False
        return False
    try:
        import redis

        client = redis.from_url(url, socket_connect_timeout=1)
        client.ping()
        _broker_ping_ok = True
    except Exception as exc:
        logger.warning("Redis broker unavailable (%s); using inline pipeline", exc)
        _broker_ping_ok = False
    return _broker_ping_ok


def _celery_enabled() -> bool:
    return Config.USE_CELERY and bool(Config.CELERY_BROKER_URL) and _is_broker_reachable()


def _get_celery_app():
    from worker.celery_app import celery_app

    return celery_app


def enqueue_stage(build_id: str, stage: PipelineStage) -> Optional[str]:
    """Send one pipeline stage to its worker queue (dedup-safe)."""
    db = get_database()
    if not db.try_claim_dispatch(build_id, stage.value):
        logger.info(
            "Dispatch dedup: skip duplicate %s for build %s",
            stage.value,
            build_id,
        )
        return None

    if not _celery_enabled():
        db.clear_dispatch(build_id, stage.value)
        return None

    queue = STAGE_TO_QUEUE[stage]
    try:
        result = _get_celery_app().send_task(
            TASK_RUN_STAGE,
            args=[build_id, stage.value],
            queue=queue,
            routing_key=queue,
        )
    except Exception as exc:
        global _broker_ping_ok
        _broker_ping_ok = False
        db.clear_dispatch(build_id, stage.value)
        logger.warning(
            "Celery enqueue failed for %s/%s (%s); caller should use inline fallback",
            build_id,
            stage.value,
            exc,
        )
        return None
    task_id = result.id
    db.register_dispatch_task(build_id, stage.value, task_id)
    logger.info("Enqueued %s for build %s (task=%s)", stage.value, build_id, task_id)
    return task_id


def enqueue_next_stage(build_id: str, completed: PipelineStage) -> Optional[str]:
    nxt = next_stage(completed)
    if nxt is None:
        return None
    return enqueue_stage(build_id, nxt)


def dispatch_build(build_id: str, *, force_stage: Optional[PipelineStage] = None) -> str:
    """
    Start or resume a build by enqueueing the appropriate first stage.
    Returns task id or a local marker when Celery is disabled.
    """
    from core.stage_coordinator import dispatch_build_recovery

    db = get_database()

    if force_stage is not None:
        from core.stage_state_machine import prepare_recovery_transition

        db.release_expired_stage_locks()
        prepare_recovery_transition(db, build_id, force_stage)
        task_id = enqueue_stage(build_id, force_stage)
        if task_id:
            return task_id
        return _run_inline_dispatch(build_id, force_stage)

    task_id = dispatch_build_recovery(build_id)
    if task_id:
        return task_id

    from core.stage_state_machine import resolve_resume_stage

    stage = resolve_resume_stage(db, build_id)
    if stage is None:
        return f"inline-complete-{build_id}"
    return _run_inline_dispatch(build_id, stage)


def _run_inline_dispatch(build_id: str, stage: PipelineStage) -> str:
    """In-process fallback when Redis/Celery unavailable (non-blocking on API event loop)."""
    import asyncio

    from core.worker_runtime import run_async

    marker = f"inline-{uuid.uuid4().hex[:8]}"
    logger.warning(
        "Celery disabled — running pipeline inline from stage %s (build=%s)",
        stage.value,
        build_id,
    )

    from core.stage_coordinator import run_stage_guarded
    from core.stage_state_machine import canonical_next_stage

    async def _run_chain() -> None:
        while True:
            current = canonical_next_stage(get_database(), build_id)
            if current is None:
                break
            result = await run_stage_guarded(build_id, current, worker_id="inline")
            if result.get("status") in ("failed", "locked"):
                break

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_run_chain())
    except RuntimeError:
        import threading

        threading.Thread(
            target=lambda: run_async(_run_chain()),
            name=f"inline-pipeline-{build_id}",
            daemon=True,
        ).start()
    return marker


def start_new_build(request_payload: dict[str, Any], build_id: Optional[str] = None) -> str:
    """Create DB row and enqueue planning stage."""
    build_id = build_id or f"pipeline_{uuid.uuid4().hex[:8]}"
    db = get_database()
    if not db.get_build(build_id):
        db.create_build(build_id, request_payload)
    # NOTE: do not dispatch automatically here; leave the build in 'queued'
    # so the caller (API or worker) can decide when to start the pipeline.
    return build_id
