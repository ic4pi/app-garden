"""Pipeline orchestrator — importable from workers without loading the FastAPI app."""

from core.pipeline_domain import PipelineOrchestrator

__all__ = ["PipelineOrchestrator"]
