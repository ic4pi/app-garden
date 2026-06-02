"""Agent registry — swap any role without changing PipelineRunner."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from agents.base import AgentResult, BaseAgent
from agents.builder import BuilderAgent
from agents.leaderboard import LeaderboardAgent
from agents.novelty import NoveltyAgent
from agents.planner import PlannerAgent
from agents.ranker import RankerAgent
from agents.repair import RepairAgent
from agents.reviewer import ReviewerAgent
from core.pipeline_context import PipelineContext
from core.validation.gate import QualityGate


@dataclass
class AgentRegistry:
    planner: BaseAgent
    builder: BaseAgent
    repair: BaseAgent
    reviewer: BaseAgent
    ranker: BaseAgent
    novelty: BaseAgent
    leaderboard: BaseAgent

    async def run_stage(
        self,
        ctx: PipelineContext,
        agent: BaseAgent,
        *,
        required: bool = True,
    ) -> AgentResult:
        if agent.should_skip(ctx):
            result = AgentResult(
                role=agent.role,
                agent_id=agent.agent_id,
                ok=True,
                payload={"skipped": True},
            )
        else:
            result = await agent.run(ctx)
        if not hasattr(ctx, "_agent_results"):
            ctx._agent_results = []
        ctx._agent_results.append(result)
        if required and not result.ok:
            raise RuntimeError(result.error or f"{agent.agent_id} failed")
        return result

    async def run_planning(self, ctx: PipelineContext) -> AgentResult:
        return await self.run_stage(ctx, self.planner)

    async def run_builder(self, ctx: PipelineContext) -> AgentResult:
        return await self.run_stage(ctx, self.builder)

    async def run_repair(self, ctx: PipelineContext) -> AgentResult:
        return await self.run_stage(ctx, self.repair)

    async def run_generating(self, ctx: PipelineContext) -> list[AgentResult]:
        builder_result = await self.run_builder(ctx)
        repair_result = await self.run_repair(ctx)
        return [builder_result, repair_result]

    async def run_reviewing(self, ctx: PipelineContext) -> AgentResult:
        return await self.run_stage(ctx, self.reviewer)

    async def run_ranking(self, ctx: PipelineContext) -> AgentResult:
        return await self.run_stage(ctx, self.ranker)

    async def run_novelty(self, ctx: PipelineContext) -> AgentResult:
        return await self.run_stage(ctx, self.novelty)

    async def run_leaderboard(self, ctx: PipelineContext) -> AgentResult:
        return await self.run_stage(ctx, self.leaderboard)


def build_agent_registry(
    orchestrator: Any,
    *,
    gate: Optional[QualityGate] = None,
    planner: Optional[BaseAgent] = None,
    builder: Optional[BaseAgent] = None,
    repair: Optional[BaseAgent] = None,
    reviewer: Optional[BaseAgent] = None,
    ranker: Optional[BaseAgent] = None,
    novelty: Optional[BaseAgent] = None,
    leaderboard: Optional[BaseAgent] = None,
) -> AgentRegistry:
    """Factory for default agents; pass overrides to swap implementations."""
    g = gate or QualityGate()
    return AgentRegistry(
        planner=planner
        or PlannerAgent(orchestrator.factory_builder, orchestrator.factory_reviewer),
        builder=builder or BuilderAgent(orchestrator),
        repair=repair or RepairAgent(orchestrator, gate=g),
        reviewer=reviewer or ReviewerAgent(orchestrator),
        ranker=ranker
        or RankerAgent(orchestrator.primary_ranker, orchestrator.fallback_ranker),
        novelty=novelty or NoveltyAgent(orchestrator.novelty_builder),
        leaderboard=leaderboard or LeaderboardAgent(orchestrator),
    )
