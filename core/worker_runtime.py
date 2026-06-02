"""Runtime helpers for Celery workers — context loaded from DB only."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from core.app_state import kernel_startup
from core.database import get_database
from core.pipeline_runner import PipelineRunner
from core.pipeline_stages import PipelineStage
from core.pipeline_store import PipelineContextStore, _services_from_orchestrator

logger = logging.getLogger("app_garden.worker_runtime")

_kernel_ready = False


def ensure_kernel() -> None:
    global _kernel_ready
    if not _kernel_ready:
        kernel_startup()
        _kernel_ready = True


def run_async(coro: Any) -> Any:
    return asyncio.run(coro)


def create_orchestrator() -> Any:
    """Fresh orchestrator per task (LLM client only — no pipeline state)."""
    from core.orchestrator import PipelineOrchestrator

    return PipelineOrchestrator()


def build_runner(build_id: str, stage: PipelineStage) -> PipelineRunner:
    """Construct runner from DB-hydrated PipelineContext only."""
    orchestrator = create_orchestrator()
    services = _services_from_orchestrator(orchestrator)
    store = PipelineContextStore(get_database())
    ctx = store.load(build_id, stage, services)
    agents = orchestrator.agent_registry()
    return PipelineRunner(ctx, agents=agents)


async def run_pipeline_stage(build_id: str, stage: PipelineStage) -> dict[str, Any]:
    ensure_kernel()
    runner = build_runner(build_id, stage)
    return await runner.execute_stage(stage)
