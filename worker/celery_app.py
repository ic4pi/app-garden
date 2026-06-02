"""Celery application — routes stages to planner/builder/repair/reviewer/ranker queues."""

from __future__ import annotations

import os

from celery import Celery
from celery.signals import worker_ready
from kombu import Queue

from core.config import Config
from core.pipeline_stages import (
    QUEUE_BUILDER,
    QUEUE_PLANNER,
    QUEUE_RANKER,
    QUEUE_REPAIR,
    QUEUE_REVIEWER,
)

broker = Config.CELERY_BROKER_URL
backend = os.getenv("CELERY_RESULT_BACKEND", Config.CELERY_RESULT_BACKEND)

celery_app = Celery("app_garden", broker=broker, backend=backend, include=["worker.tasks"])

_recovery_seconds = int(
    __import__("os").getenv("RECOVERY_INTERVAL_SECONDS", "300")
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_default_queue=QUEUE_PLANNER,
    task_queues=(
        Queue(QUEUE_PLANNER, routing_key=QUEUE_PLANNER),
        Queue(QUEUE_BUILDER, routing_key=QUEUE_BUILDER),
        Queue(QUEUE_REPAIR, routing_key=QUEUE_REPAIR),
        Queue(QUEUE_REVIEWER, routing_key=QUEUE_REVIEWER),
        Queue(QUEUE_RANKER, routing_key=QUEUE_RANKER),
    ),
    task_routes={
        "worker.tasks.run_stage": {
            "queue": QUEUE_PLANNER,
        },
        "worker.tasks.dispatch_build": {
            "queue": QUEUE_PLANNER,
        },
        "worker.tasks.recover_stuck_builds": {
            "queue": QUEUE_PLANNER,
        },
        "worker.tasks.startup_auto_resume": {
            "queue": QUEUE_PLANNER,
        },
    },
    beat_schedule={
        "recover-stuck-builds": {
            "task": "worker.tasks.recover_stuck_builds",
            "schedule": float(_recovery_seconds),
        },
    },
)

# Queues are selected per send_task(..., queue=STAGE_TO_QUEUE[stage])


@celery_app.on_after_configure.connect
def _schedule_startup_resume(sender, **kwargs):
    """Enqueue one-shot auto-resume when Celery starts."""
    sender.send_task("worker.tasks.startup_auto_resume")


@worker_ready.connect
def _on_worker_ready(sender, **kwargs):
    logger = __import__("logging").getLogger("app_garden.celery")
    logger.info("Celery worker ready: %s", sender.hostname)
