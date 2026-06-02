"""Build/stage locking, guarded execution, recovery — deterministic state machine."""

from __future__ import annotations

import logging
import socket
import uuid
from typing import Any, Optional

from core.build_queue import _celery_enabled, enqueue_stage
from core.config import Config
from core.database import AppDatabase, get_database
from core.pipeline_stages import PipelineStage
from core.stage_execution import (
    StageBeginResult,
    StageGuardOutcome,
    stage_done_marker,
    verify_stage_done,
)
from core.stage_state_machine import canonical_next_stage, prepare_recovery_transition
from core.worker_runtime import run_async, run_pipeline_stage

logger = logging.getLogger("app_garden.stage_coordinator")


def _worker_id() -> str:
    return f"{socket.gethostname()}:{uuid.uuid4().hex[:8]}"


def _ttl_seconds() -> int:
    return int(getattr(Config, "STAGE_LOCK_TTL_SECONDS", 3600))


def _stuck_minutes() -> int:
    return int(getattr(Config, "STUCK_BUILD_THRESHOLD_MINUTES", 30))


class StageCoordinator:
    def __init__(self, db: Optional[AppDatabase] = None) -> None:
        self.db = db or get_database()

    def _stage_or_build_terminal(self, build_id: str, stage: PipelineStage) -> bool:
        build = self.db.get_build(build_id)
        if build and build.get("status") in ("complete", "failed"):
            return True

        if verify_stage_done(self.db, build_id, stage):
            return True

        row = self.db.get_stage_run(build_id, stage.value)
        if row and row.get("status") == "running":
            return False

        return False

    def begin_stage(
        self,
        build_id: str,
        stage: PipelineStage,
        *,
        worker_id: Optional[str] = None,
    ) -> StageGuardOutcome:
        wid = worker_id or _worker_id()

        if self._stage_or_build_terminal(build_id, stage):
            progress = self.db.get_progress(build_id)
            if progress.get("pipeline_status") == "interrupted":
                try:
                    prepare_recovery_transition(self.db, build_id, stage)
                except Exception:
                    pass
            return StageGuardOutcome(StageBeginResult.ALREADY_DONE)

        row = self.db.get_stage_run(build_id, stage.value)
        if row and row.get("status") in ("completed", "failed"):
            if not verify_stage_done(self.db, build_id, stage):
                self.db.reset_stage_run(build_id, stage.value)

        acquired, token = self.db.try_acquire_stage_lock(
            build_id,
            stage.value,
            wid,
            ttl_seconds=_ttl_seconds(),
        )
        if not acquired:
            row = self.db.get_stage_run(build_id, stage.value)
            if row and row.get("status") in ("completed", "failed"):
                return StageGuardOutcome(StageBeginResult.ALREADY_DONE)
            return StageGuardOutcome(StageBeginResult.LOCKED)

        self.db.consume_dispatch(build_id, stage.value)
        return StageGuardOutcome(StageBeginResult.PROCEED, lock_token=token)

    def complete_stage(
        self,
        build_id: str,
        stage: PipelineStage,
        *,
        lock_token: Optional[str],
    ) -> None:
        # Persist checkpoint marker and stage completion atomically so that
        # no late exception or retry can observe a partially-completed state.
        try:
            self.db.complete_stage_with_checkpoint(build_id, stage.value, lock_token=lock_token)
        except AttributeError:
            # backwards-compat: if the DB implementation doesn't provide the
            # atomic helper, fall back to the previous behavior.
            if not verify_stage_done(self.db, build_id, stage):
                self.db.save_checkpoint(build_id, {stage_done_marker(stage): True})
            self.db.complete_stage_run(build_id, stage.value, lock_token=lock_token)

    def fail_stage(
        self,
        build_id: str,
        stage: PipelineStage,
        *,
        lock_token: Optional[str],
        error: str = "",
    ) -> None:
        if self._stage_or_build_terminal(build_id, stage):
            logger.info(
                "Skipping failure for terminal stage/build %s/%s",
                build_id,
                stage.value,
            )
            return
        self.db.fail_stage_run(build_id, stage.value, lock_token=lock_token, error=error)
        self.db.clear_dispatch(build_id, stage.value)

    def fail_build(
        self,
        build_id: str,
        stage: PipelineStage,
        *,
        lock_token: Optional[str],
        error: str = "",
    ) -> None:
        if self._stage_or_build_terminal(build_id, stage):
            logger.info(
                "Skipping build failure for terminal stage/build %s/%s",
                build_id,
                stage.value,
            )
            return
        self.fail_stage(build_id, stage, lock_token=lock_token, error=error)
        self.db.update_build(build_id, status="failed", error=error, completed=True)
        self.db.apply_pipeline_state(
            build_id,
            pipeline_status="failed",
            phase=stage.value,
            message=error[:500],
            percent=100.0,
        )
        self.db.append_log(build_id, stage.value, f"Build failed: {error}", level="error")

    def recover_stuck_builds(self, *, limit: int = 50) -> list[str]:
        return recover_stuck_builds(limit=limit)


def _enqueue_next(build_id: str) -> Optional[str]:
    nxt = canonical_next_stage(get_database(), build_id)
    if nxt is None:
        return None
    from core.build_queue import enqueue_stage as _eq

    return _eq(build_id, nxt)


def _recover_expired_stage_runs(*, limit: int = 50) -> list[str]:
    db = get_database()
    recovered: list[str] = []
    expired_rows = db.list_expired_running_stage_runs(limit=limit)
    max_attempts = int(getattr(Config, "STAGE_MAX_ATTEMPTS", 3))

    for row in expired_rows:
        build_id = row["build_id"]
        stage = PipelineStage(row["stage"])
        if row["attempt"] >= max_attempts:
            db.fail_stage_run(
                build_id,
                stage.value,
                lock_token=None,
                error="Stage retry limit exceeded during recovery",
            )
            continue

        if db.reset_expired_stage_run(build_id, stage.value):
            if _celery_enabled():
                task_id = enqueue_stage(build_id, stage)
                if task_id:
                    recovered.append(build_id)
            else:
                recovered.append(build_id)
    return recovered


async def run_stage_guarded(
    build_id: str,
    stage: PipelineStage,
    *,
    worker_id: Optional[str] = None,
) -> dict[str, Any]:
    """Lock → verify idempotency → execute → persist → chain exactly one next stage."""
    coord = StageCoordinator()

    begin = coord.begin_stage(build_id, stage, worker_id=worker_id)
    if begin.result == StageBeginResult.ALREADY_DONE:
        logger.info("Stage %s already done for %s", stage.value, build_id)
        if _celery_enabled():
            _enqueue_next(build_id)
        return {
            "status": "ok",
            "skipped": True,
            "reason": "already_done",
            "stage": stage.value,
            "build_id": build_id,
        }

    if begin.result == StageBeginResult.LOCKED:
        return {
            "status": "locked",
            "stage": stage.value,
            "build_id": build_id,
        }

    lock_token = begin.lock_token
    try:
        result = await run_pipeline_stage(build_id, stage)
        if result.get("status") == "failed":
            if coord._stage_or_build_terminal(build_id, stage):
                logger.info(
                    "Ignoring late failure for terminal stage/build %s/%s",
                    build_id,
                    stage.value,
                )
                return {
                    "status": "ok",
                    "skipped": True,
                    "reason": "already_done",
                    "stage": stage.value,
                    "build_id": build_id,
                }
            coord.fail_build(
                build_id,
                stage,
                lock_token=lock_token,
                error=str(result.get("error", "")) or "Stage execution failed",
            )
            return result

        coord.complete_stage(build_id, stage, lock_token=lock_token)

        if not verify_stage_done(coord.db, build_id, stage):
            err = f"Stage {stage.value} missing completion marker or artifacts"
            coord.fail_build(build_id, stage, lock_token=lock_token, error=err)
            return {"status": "failed", "error": err, "build_id": build_id, "stage": stage.value}

        if stage != PipelineStage.FINALIZE and _celery_enabled():
            _enqueue_next(build_id)
        return result
    except Exception as exc:
        if coord._stage_or_build_terminal(build_id, stage):
            logger.info(
                "Ignoring late exception for terminal stage/build %s/%s",
                build_id,
                stage.value,
            )
            return {
                "status": "ok",
                "skipped": True,
                "reason": "already_done",
                "stage": stage.value,
                "build_id": build_id,
            }
        coord.fail_build(
            build_id,
            stage,
            lock_token=lock_token,
            error=str(exc) or "Unhandled stage exception",
        )
        return {
            "status": "failed",
            "error": str(exc),
            "build_id": build_id,
            "stage": stage.value,
        }


def dispatch_build_recovery(build_id: str, *, force: bool = False) -> Optional[str]:
    """Resolve resume stage, apply FSM recovery transition, enqueue one stage."""
    from core.stage_state_machine import prepare_recovery_transition, resolve_resume_stage

    db = get_database()
    build = db.get_build(build_id)
    if not build:
        return None
    if build.get("status") in ("complete", "failed") and not force:
        return None

    stage = resolve_resume_stage(db, build_id)
    if stage is None:
        return None

    prepare_recovery_transition(db, build_id, stage)

    row = db.get_stage_run(build_id, stage.value)
    if row and row.get("status") == "running":
        from datetime import datetime, timezone

        expires = row.get("expires_at")
        if expires and expires > datetime.now(timezone.utc).isoformat():
            return None

    from core.build_queue import enqueue_stage as _eq

    return _eq(build_id, stage)


def recover_stuck_builds(*, limit: int = 50) -> list[str]:
    requeued: list[str] = []
    requeued += _recover_expired_stage_runs(limit=limit)

    db = get_database()
    stale = db.list_stale_active_builds(stuck_minutes=_stuck_minutes(), limit=limit)
    for row in stale:
        build_id = row["build_id"]
        if verify_stage_done(db, build_id, PipelineStage.FINALIZE):
            continue
        task_id = dispatch_build_recovery(build_id)
        if task_id:
            requeued.append(build_id)
    return list(dict.fromkeys(requeued))


def auto_resume_on_startup(*, limit: int = 100) -> dict[str, Any]:
    if not getattr(Config, "AUTO_RESUME_ON_STARTUP", True):
        return {"skipped": True, "reason": "disabled"}

    db = get_database()
    dispatched: list[str] = []
    for row in db.list_resumable_builds(limit=limit):
        task_id = dispatch_build_recovery(row["id"])
        if task_id:
            dispatched.append(row["id"])

    stuck = recover_stuck_builds(limit=limit)
    return {
        "dispatched": list(dict.fromkeys(dispatched + stuck)),
        "stuck_recovered": stuck,
    }


def try_startup_recovery_lock() -> bool:
    if Config.USE_CELERY and Config.CELERY_BROKER_URL:
        try:
            import redis

            client = redis.from_url(Config.CELERY_BROKER_URL)
            return bool(
                client.set(
                    "app_garden:recovery:startup",
                    _worker_id(),
                    nx=True,
                    ex=int(getattr(Config, "STARTUP_RECOVERY_LOCK_SECONDS", 120)),
                )
            )
        except Exception as exc:
            logger.warning("Redis startup lock unavailable: %s", exc)
    return get_database().try_acquire_cluster_lock("startup_recovery", ttl_seconds=120)
