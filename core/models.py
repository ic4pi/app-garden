"""Shared Pydantic models for pipeline builds (importable without FastAPI)."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator

class CodeType(str, Enum):
    WEBSITE = "website"
    WEB_APP = "web_app"
    API_BACKEND = "api_backend"
    CLI_TOOL = "cli_tool"
    DATA_PIPELINE = "data_pipeline"
    GAME = "game"
    MOBILE_APP = "mobile_app"
    CHATBOT = "chatbot"
    DASHBOARD = "dashboard"
    E_COMMERCE = "e_commerce"
    PORTFOLIO = "portfolio"
    BLOG = "blog"
    CUSTOM = "custom"

class ToolStack(BaseModel):
    name: str
    frontend: List[str] = Field(default_factory=list)
    backend: List[str] = Field(default_factory=list)
    database: List[str] = Field(default_factory=list)
    styling: List[str] = Field(default_factory=list)
    utilities: List[str] = Field(default_factory=list)
    deployment: List[str] = Field(default_factory=list)
    justification: str = ""
    novelty_score: int = Field(0, ge=0, le=100)

class BuildAttempt(BaseModel):
    attempt_id: str
    attempt_number: int
    tool_stack: ToolStack
    model_used: str
    code_artifact: str = ""
    workspace_path: str = ""
    build_log: str = ""
    tool_usage_report: str = ""
    build_time_seconds: float = 0.0
    success: bool = False
    error_message: str = ""
    timestamp: str = ""

class ReviewDimension(BaseModel):
    dimension: str
    score: int = Field(0, ge=0, le=100)
    analysis: str = ""
    suggestions: List[str] = Field(default_factory=list)

class ReviewReport(BaseModel):
    attempt_id: str
    overall_score: Optional[int] = None  # legacy aggregate; not authoritative
    dimensions: List[ReviewDimension] = Field(default_factory=list)
    comparative_notes: str = ""
    what_works_better: str = ""
    improvement_suggestions: List[str] = Field(default_factory=list)
    potential_failure_points: List[str] = Field(default_factory=list)
    reviewer_model: str = ""
    timestamp: str = ""

class RankedBuild(BaseModel):
    attempt_id: str
    attempt_number: int
    tool_stack_name: str
    functionality_score: int = Field(0, ge=0, le=100)
    code_quality_score: int = Field(0, ge=0, le=100)
    tool_optimization_score: int = Field(0, ge=0, le=100)
    novelty_score: int = Field(0, ge=0, le=100)
    documentation_score: int = Field(0, ge=0, le=100)
    total_score: float = 0.0
    justification: str = ""
    rank: int = 0
    ranker_model: str = ""

class NoveltyAttempt(BaseModel):
    attempt_id: str
    iteration: int
    winning_config: ToolStack
    code_artifact: str = ""
    build_log: str = ""
    creativity_notes: str = ""
    build_time_seconds: float = 0.0
    success: bool = False
    timestamp: str = ""

class LeaderboardEntry(BaseModel):
    entry_id: str
    project_name: str
    code_type: str
    score: float
    novelty_rating: int
    tool_stack: str
    build_time_seconds: float
    user_rating: Optional[int] = None
    created_at: str
    download_path: Optional[str] = None
    model_used: str = ""
    # Trait vector fields (canonical evaluation data)
    trait_vector: Optional[Dict] = None
    dominant_traits: List[str] = Field(default_factory=list)
    weak_traits: List[str] = Field(default_factory=list)
    builder_traits: Optional[Dict] = None


# ═══════════════════════════════════════════════════════════════════════════════
#  SCORING RUBRICS (20-Category System)
# ═══════════════════════════════════════════════════════════════════════════════

class BuilderScore(BaseModel):
    """20-category builder scoring rubric (1=Bad, 3=Average, 5=Great). Max 100."""
    # FUNCTIONALITY (25 pts)
    prompt_understanding: int = Field(1, ge=1, le=5)
    correct_feature_generation: int = Field(1, ge=1, le=5)
    project_completeness: int = Field(1, ge=1, le=5)
    runtime_stability: int = Field(1, ge=1, le=5)
    error_handling: int = Field(1, ge=1, le=5)
    # CODE QUALITY (25 pts)
    file_organization: int = Field(1, ge=1, le=5)
    architecture_decisions: int = Field(1, ge=1, le=5)
    readability: int = Field(1, ge=1, le=5)
    reusability: int = Field(1, ge=1, le=5)
    maintainability: int = Field(1, ge=1, le=5)
    # UX / PRODUCT THINKING (25 pts)
    ui_ux_quality: int = Field(1, ge=1, le=5)
    user_flow_clarity: int = Field(1, ge=1, le=5)
    responsiveness: int = Field(1, ge=1, le=5)
    accessibility: int = Field(1, ge=1, le=5)
    performance_optimization: int = Field(1, ge=1, le=5)
    # INTELLIGENCE / NOVELTY (25 pts)
    creativity: int = Field(1, ge=1, le=5)
    novel_problem_solving: int = Field(1, ge=1, le=5)
    smart_stack_selection: int = Field(1, ge=1, le=5)
    adaptability_to_request: int = Field(1, ge=1, le=5)
    overall_impressiveness: int = Field(1, ge=1, le=5)

    total_score: int = 0
    rank_label: str = "Broken"

    def calculate(self):
        scores = [
            self.prompt_understanding, self.correct_feature_generation, self.project_completeness,
            self.runtime_stability, self.error_handling,
            self.file_organization, self.architecture_decisions, self.readability,
            self.reusability, self.maintainability,
            self.ui_ux_quality, self.user_flow_clarity, self.responsiveness,
            self.accessibility, self.performance_optimization,
            self.creativity, self.novel_problem_solving, self.smart_stack_selection,
            self.adaptability_to_request, self.overall_impressiveness
        ]
        self.total_score = sum(scores)
        self.rank_label = self._get_rank_label(self.total_score)
        return self.total_score

    @staticmethod
    def _get_rank_label(score: int) -> str:
        if score <= 20: return "Broken"
        if score <= 40: return "Weak"
        if score <= 60: return "Functional"
        if score <= 80: return "Strong"
        if score <= 90: return "Advanced"
        return "Elite"

class AppScore(BaseModel):
    """20-category app scoring rubric (1=Bad, 3=Average, 5=Great). Max 100."""
    # CORE QUALITY (25 pts)
    does_it_run: int = Field(1, ge=1, le=5)
    matches_prompt: int = Field(1, ge=1, le=5)
    bug_level: int = Field(1, ge=1, le=5)
    speed_performance: int = Field(1, ge=1, le=5)
    stability: int = Field(1, ge=1, le=5)
    # USER EXPERIENCE (25 pts)
    ease_of_use: int = Field(1, ge=1, le=5)
    design_quality: int = Field(1, ge=1, le=5)
    mobile_responsiveness: int = Field(1, ge=1, le=5)
    accessibility: int = Field(1, ge=1, le=5)
    navigation_clarity: int = Field(1, ge=1, le=5)
    # FEATURES (25 pts)
    feature_completeness: int = Field(1, ge=1, le=5)
    feature_usefulness: int = Field(1, ge=1, le=5)
    data_handling: int = Field(1, ge=1, le=5)
    api_backend_integration: int = Field(1, ge=1, le=5)
    edge_case_handling: int = Field(1, ge=1, le=5)
    # NOVELTY (25 pts)
    originality: int = Field(1, ge=1, le=5)
    interesting_ideas: int = Field(1, ge=1, le=5)
    unique_ux: int = Field(1, ge=1, le=5)
    creative_implementation: int = Field(1, ge=1, le=5)
    overall_wow_factor: int = Field(1, ge=1, le=5)

    total_score: int = 0
    rank_label: str = "Broken"

    def calculate(self):
        scores = [
            self.does_it_run, self.matches_prompt, self.bug_level,
            self.speed_performance, self.stability,
            self.ease_of_use, self.design_quality, self.mobile_responsiveness,
            self.accessibility, self.navigation_clarity,
            self.feature_completeness, self.feature_usefulness, self.data_handling,
            self.api_backend_integration, self.edge_case_handling,
            self.originality, self.interesting_ideas, self.unique_ux,
            self.creative_implementation, self.overall_wow_factor
        ]
        self.total_score = sum(scores)
        self.rank_label = self._get_rank_label(self.total_score)
        return self.total_score

    @staticmethod
    def _get_rank_label(score: int) -> str:
        if score <= 20: return "Broken"
        if score <= 40: return "Weak"
        if score <= 60: return "Functional"
        if score <= 80: return "Strong"
        if score <= 90: return "Advanced"
        return "Elite"

class CategoryHistory(BaseModel):
    """Track score history per category for evolution analytics."""
    builder_version: str
    timestamp: str
    builder_scores: BuilderScore
    app_scores: AppScore

class EvolutionStrategy(BaseModel):
    """Factory-generated mutation strategy for improving builders."""
    target_version: str
    weak_categories: List[str]
    strong_categories: List[str]
    mutation_prompt: str
    constraint_changes: List[str]
    reasoning_style: str
    created_at: str

class BuildRequest(BaseModel):
    code_type: CodeType
    description: str = Field(..., min_length=10, max_length=2000)
    specific_requirements: str = ""
    preferred_frameworks: List[str] = Field(default_factory=list)
    target_audience: str = ""
    complexity_level: str = "medium"


# ═══════════════════════════════════════════════════════════════════════════════
#  TOOL INVENTORY
# ═══════════════════════════════════════════════════════════════════════════════

