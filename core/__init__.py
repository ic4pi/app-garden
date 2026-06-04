"""App Garden system kernel — config, state, persistence, and lifecycle.

Uses lazy imports so that lightweight consumers (e.g. Celery workers)
can import core.config without pulling in pydantic, orchestrator, etc.
"""

from core.config import Config, SETTINGS_PATH, is_secret_key, mask_secrets, redact_for_log


def __getattr__(name):
    """Lazy-load heavy submodules only when accessed."""
    _lazy = {
        "AppState": "core.app_state",
        "kernel_startup": "core.app_state",
        "kernel_shutdown": "core.app_state",
        "AppDatabase": "core.database",
        "get_database": "core.database",
        "PipelineContext": "core.pipeline_context",
        "PipelineReviews": "core.pipeline_context",
        "PipelineContextStore": "core.pipeline_store",
        "PipelineServices": "core.pipeline_store",
        "canonical_next_stage": "core.stage_state_machine",
        "PipelineState": "core.pipeline_fsm",
        "PipelineStateMachine": "core.pipeline_fsm",
        "LINEAR_PIPELINE": "core.pipeline_fsm",
        "InvalidPipelineTransition": "core.pipeline_fsm",
        "QualityGate": "core.validation",
        "ValidationReport": "core.validation",
        "BuilderToolkit": "core.tools",
        "ToolRegistry": "core.tools",
        "BuildWorkspace": "core.tools",
        "PipelineOrchestrator": "core.orchestrator",
        "BuildRequest": "core.models",
        "BuildAttempt": "core.models",
        "ReviewReport": "core.models",
        "NoveltyAttempt": "core.models",
    }
    if name in _lazy:
        import importlib
        mod = importlib.import_module(_lazy[name])
        value = getattr(mod, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module 'core' has no attribute {name!r}")


__all__ = [
    "Config",
    "SETTINGS_PATH",
    "is_secret_key",
    "mask_secrets",
    "redact_for_log",
    "AppState",
    "kernel_startup",
    "kernel_shutdown",
    "AppDatabase",
    "get_database",
    "PipelineState",
    "PipelineStateMachine",
    "LINEAR_PIPELINE",
    "InvalidPipelineTransition",
    "PipelineContext",
    "PipelineReviews",
    "PipelineContextStore",
    "PipelineServices",
    "canonical_next_stage",
    "QualityGate",
    "ValidationReport",
    "ToolRegistry",
    "BuildWorkspace",
    "BuilderToolkit",
    "PipelineOrchestrator",
    "BuildRequest",
    "BuildAttempt",
    "ReviewReport",
    "NoveltyAttempt",
]
