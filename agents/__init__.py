"""Multi-brain agent layer — standardized run(context) per role."""

from agents.base import AgentResult, AgentRole, BaseAgent
from agents.registry import AgentRegistry, build_agent_registry

__all__ = [
    "AgentRole",
    "AgentResult",
    "BaseAgent",
    "AgentRegistry",
    "build_agent_registry",
]
