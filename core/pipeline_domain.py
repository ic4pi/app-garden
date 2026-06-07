"""Pipeline domain: models, LLM services, and orchestration (no FastAPI)."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import sqlite3
import time
import traceback
import uuid
import zipfile
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from pydantic import BaseModel, Field, field_validator

from core.config import Config
from core.pipeline_context import PipelineContext
from core.pipeline_fsm import PipelineState, PipelineStateMachine, state_for_phase
from core.tools.builder_hooks import apply_builder_tools

class TraitCategory(str, Enum):
    """Canonical evaluation categories."""
    PROMPT_UNDERSTANDING = "prompt_understanding"
    FEATURE_CORRECTNESS = "feature_correctness"
    COMPLETENESS = "completeness"
    STABILITY = "stability"
    ERROR_HANDLING = "error_handling"
    FILE_ORGANIZATION = "file_organization"
    ARCHITECTURE = "architecture"
    READABILITY = "readability"
    REUSABILITY = "reusability"
    MAINTAINABILITY = "maintainability"
    UI_UX_QUALITY = "ui_ux_quality"
    USER_FLOW = "user_flow"
    RESPONSIVENESS = "responsiveness"
    ACCESSIBILITY = "accessibility"
    PERFORMANCE = "performance"
    CREATIVITY = "creativity"
    NOVEL_PROBLEM_SOLVING = "novel_problem_solving"
    STACK_SELECTION = "stack_selection"
    ADAPTABILITY = "adaptability"
    IMPRESSIVENESS = "impressiveness"

class ScoreValue(int, Enum):
    """Valid trait scores. ONLY 1, 3, or 5 allowed."""
    POOR = 1      # Failing, broken, or missing
    ACCEPTABLE = 3  # Works, standard, average
    EXCEPTIONAL = 5  # Great, innovative, production-grade

class TraitScore(BaseModel):
    """Individual trait score with mandatory evidence."""
    category: TraitCategory
    score: ScoreValue
    evidence: str = Field(..., min_length=10, description="Specific code evidence supporting this score")
    reasoning: str = Field(..., min_length=10, description="Why this score was chosen")
    confidence: int = Field(..., ge=0, le=100, description="Reviewer confidence in this assessment")

    @field_validator('score')
    @classmethod
    def validate_score(cls, v):
        if v not in [1, 3, 5]:
            raise ValueError(f"Score must be 1, 3, or 5. Got {v}")
        return v

class TraitVector(BaseModel):
    """Complete 20-trait evaluation vector. This is the canonical evaluation object."""
    attempt_id: str
    builder_type: str  # "responsible", "creative", "novelty"
    traits: Dict[TraitCategory, TraitScore]
    timestamp: str
    reviewer_id: str

    # Computed properties (for display only, NOT for selection)
    legacy_total_score: int = Field(0, description="Sum of all trait scores. DISPLAY ONLY.")
    legacy_rank_label: str = Field("", description="DISPLAY ONLY.")

    def compute_legacy_display(self):
        """Compute legacy score for display. Does NOT influence selection."""
        self.legacy_total_score = sum(t.score.value for t in self.traits.values())
        if self.legacy_total_score <= 20:
            self.legacy_rank_label = "Broken"
        elif self.legacy_total_score <= 40:
            self.legacy_rank_label = "Weak"
        elif self.legacy_total_score <= 60:
            self.legacy_rank_label = "Functional"
        elif self.legacy_total_score <= 80:
            self.legacy_rank_label = "Strong"
        elif self.legacy_total_score <= 90:
            self.legacy_rank_label = "Advanced"
        else:
            self.legacy_rank_label = "Elite"
        return self.legacy_total_score

class TraitVectorComparison(BaseModel):
    """Result of comparing two trait vectors."""
    winner_attempt_id: str
    loser_attempt_id: str
    dominant_traits: List[TraitCategory]  # Categories where winner is strictly better
    weak_traits: List[TraitCategory]  # Categories where winner is worse (growth areas)
    tie_traits: List[TraitCategory]  # Equal scores
    justification: str

class CanonicalRankedBuild(BaseModel):
    """Ranked build using trait-vector comparison."""
    attempt_id: str
    attempt_number: int
    tool_stack_name: str
    trait_vector: TraitVector
    rank: int = 0
    ranker_model: str = ""
    # Legacy display fields (computed from trait vector, not used for selection)
    display_total_score: int = 0
    display_rank_label: str = ""
    # Backward compatibility fields (required by pipeline/leaderboard)
    total_score: float = 0.0
    novelty_score: float = 50.0
    execution_score: float = 0.0
    confidence_score: float = 50.0
    functionality_score: int = 0
    code_quality_score: int = 0
    tool_optimization_score: int = 0
    documentation_score: int = 0
    justification: str = ""

    def compute_display_scores(self):
        """Update display scores from trait vector. DISPLAY ONLY."""
        self.display_total_score = self.trait_vector.legacy_total_score
        self.display_rank_label = self.trait_vector.legacy_rank_label
        self.execution_score = self._calculate_execution_score()
        self.confidence_score = self._calculate_confidence_score()
        self.novelty_score = self._calculate_novelty_score()

    def _calculate_execution_score(self) -> float:
        execution_traits = [
            TraitCategory.PROMPT_UNDERSTANDING,
            TraitCategory.FEATURE_CORRECTNESS,
            TraitCategory.COMPLETENESS,
            TraitCategory.STABILITY,
            TraitCategory.ERROR_HANDLING,
            TraitCategory.FILE_ORGANIZATION,
            TraitCategory.ARCHITECTURE,
            TraitCategory.READABILITY,
            TraitCategory.REUSABILITY,
            TraitCategory.MAINTAINABILITY,
        ]
        scores = [
            self.trait_vector.traits.get(cat).score.value
            if cat in self.trait_vector.traits else ScoreValue.ACCEPTABLE.value
            for cat in execution_traits
        ]
        return float(sum(scores) / len(scores) * 20)

    def _calculate_novelty_score(self) -> float:
        novelty_traits = [
            TraitCategory.CREATIVITY,
            TraitCategory.NOVEL_PROBLEM_SOLVING,
            TraitCategory.STACK_SELECTION,
            TraitCategory.ADAPTABILITY,
            TraitCategory.IMPRESSIVENESS,
        ]
        scores = [
            self.trait_vector.traits.get(cat).score.value
            for cat in novelty_traits if cat in self.trait_vector.traits
        ]
        if not scores:
            return 50.0
        return float(sum(scores) / len(scores) * 20)

    def _calculate_confidence_score(self) -> float:
        confidences = [t.confidence for t in self.trait_vector.traits.values()]
        if not confidences:
            return 50.0
        return float(sum(confidences) / len(confidences))

class TraitVectorRanker:
    """Ranks builds by comparing trait vectors directly.

    NO weighted averages. NO overall_score. NO blended totals.
    Comparison rules:
    1. Count categories where A > B (dominant count)
    2. Count categories where A < B (weak count)
    3. Count ties
    4. Winner = more dominant categories
    5. If tied on dominant count, winner = higher sum of dominant margins
    6. If still tied, winner = fewer weak categories
    """

    SYSTEM_PROMPT = """You are an expert code evaluator. You evaluate builds using TRAIT VECTORS.

CRITICAL RULES:
1. You MUST evaluate 20 specific traits. Each trait gets EXACTLY 1, 3, or 5.
2. 1 = POOR (broken, missing, or completely wrong)
3. 3 = ACCEPTABLE (works, standard implementation, nothing special)
4. 5 = EXCEPTIONAL (production-grade, innovative, thoughtful)
5. You MUST provide SPECIFIC EVIDENCE for every score. Generic praise is forbidden.
6. You MUST explain your REASONING for every score.
7. You MUST state your CONFIDENCE (0-100) for every score.

SCORE DISTRIBUTION RULES (Prevents Clustering):
- At least 5 traits MUST be scored 1 (poor)
- At least 5 traits MUST be scored 5 (exceptional)
- The remaining 10 can be 1, 3, or 5
- NEVER give all traits the same score
- NEVER default to 3

REQUIRED JSON FORMAT:
{
  "trait_vector": {
    "prompt_understanding": {"score": 1|3|5, "evidence": "specific code reference", "reasoning": "why", "confidence": 0-100},
    "feature_correctness": {"score": 1|3|5, "evidence": "...", "reasoning": "...", "confidence": 0-100},
    "completeness": {"score": 1|3|5, "evidence": "...", "reasoning": "...", "confidence": 0-100},
    "stability": {"score": 1|3|5, "evidence": "...", "reasoning": "...", "confidence": 0-100},
    "error_handling": {"score": 1|3|5, "evidence": "...", "reasoning": "...", "confidence": 0-100},
    "file_organization": {"score": 1|3|5, "evidence": "...", "reasoning": "...", "confidence": 0-100},
    "architecture": {"score": 1|3|5, "evidence": "...", "reasoning": "...", "confidence": 0-100},
    "readability": {"score": 1|3|5, "evidence": "...", "reasoning": "...", "confidence": 0-100},
    "reusability": {"score": 1|3|5, "evidence": "...", "reasoning": "...", "confidence": 0-100},
    "maintainability": {"score": 1|3|5, "evidence": "...", "reasoning": "...", "confidence": 0-100},
    "ui_ux_quality": {"score": 1|3|5, "evidence": "...", "reasoning": "...", "confidence": 0-100},
    "user_flow": {"score": 1|3|5, "evidence": "...", "reasoning": "...", "confidence": 0-100},
    "responsiveness": {"score": 1|3|5, "evidence": "...", "reasoning": "...", "confidence": 0-100},
    "accessibility": {"score": 1|3|5, "evidence": "...", "reasoning": "...", "confidence": 0-100},
    "performance": {"score": 1|3|5, "evidence": "...", "reasoning": "...", "confidence": 0-100},
    "creativity": {"score": 1|3|5, "evidence": "...", "reasoning": "...", "confidence": 0-100},
    "novel_problem_solving": {"score": 1|3|5, "evidence": "...", "reasoning": "...", "confidence": 0-100},
    "stack_selection": {"score": 1|3|5, "evidence": "...", "reasoning": "...", "confidence": 0-100},
    "adaptability": {"score": 1|3|5, "evidence": "...", "reasoning": "...", "confidence": 0-100},
    "impressiveness": {"score": 1|3|5, "evidence": "...", "reasoning": "...", "confidence": 0-100}
  },
  "builder_traits": {
    "risk_tolerance": 0-100,
    "architecture_creativity": 0-100,
    "stability_bias": 0-100,
    "tool_efficiency": 0-100,
    "documentation_habit": 0-100,
    "testing_discipline": 0-100,
    "ux_sensitivity": 0-100,
    "novelty_seeking": 0-100
  }
}

EVIDENCE REQUIREMENTS:
- For score 1: Quote the broken code or describe what's missing
- For score 3: Describe what works and what's standard about it
- For score 5: Quote the exceptional code and explain why it's above average

CONFIDENCE GUIDE:
- 90-100: You are certain, you can see the code clearly
- 70-89: You are fairly confident but some aspects are unclear
- 50-69: You are guessing based on partial information
- 0-49: You are unsure, code is unclear or incomplete"""

    def __init__(self, llm_client):
        self.llm = llm_client
        self._migration_logger = logging.getLogger("trait_vector_migration")

    def _log_legacy_access(self, source: str, field: str):
        """Log whenever legacy scoring is accessed."""
        self._migration_logger.warning(
            f"LEGACY_SCORE_ACCESS: source={source}, field={field}, "
            f"timestamp={datetime.now().isoformat()}"
        )

    async def rank_all(self, ctx_or_attempts, reviews=None) -> List[CanonicalRankedBuild]:
        """Rank via trait vectors. Accepts PipelineContext or legacy (attempts, reviews)."""
        from itertools import zip_longest

        if isinstance(ctx_or_attempts, PipelineContext):
            ctx = ctx_or_attempts
            attempts = ctx.files.all_attempts
            reviews = ctx.files.app_reviews
            write_ctx = ctx
        else:
            attempts = ctx_or_attempts
            write_ctx = None
        trait_vectors = []
        for attempt, review in zip_longest(attempts, reviews, fillvalue=None):
            if attempt is None:
                continue
            tv = await self._generate_trait_vector(attempt, review)
            trait_vectors.append((attempt, tv))

        ranked = self._compare_trait_vectors(trait_vectors)
        for i, r in enumerate(ranked, 1):
            r.rank = i
            r.compute_display_scores()

        if write_ctx:
            write_ctx.rankings.ranked_builds = ranked
            write_ctx.traits.vectors = [
                {
                    "attempt_id": r.attempt_id,
                    "rank": r.rank,
                    "trait_vector": r.trait_vector.model_dump() if hasattr(r, "trait_vector") else None,
                }
                for r in ranked
            ]
        return ranked

    async def _generate_trait_vector(self, attempt, review) -> TraitVector:
        """Generate a trait vector for a single attempt."""
        prompt = self._construct_trait_prompt(attempt, review)

        try:
            response = await self.llm.generate_code(
                prompt,
                Config.PRIMARYRANKER_MODEL,
                self.SYSTEM_PROMPT,
                provider=Config.PRIMARYRANKER_PROVIDER,
                allow_retries=False,
                allow_fallback_model=False,
            )
            return self._parse_trait_vector(response, attempt)
        except Exception as exc:
            self._migration_logger.warning(
                f"Trait vector LLM failed for {attempt.attempt_id}: {exc} — using fallback"
            )
            return self._fallback_trait_vector(attempt)

    def _construct_trait_prompt(self, attempt, review):
        """Build prompt for trait vector generation."""
        existing_dims = []
        if review and hasattr(review, 'dimensions') and review.dimensions:
            existing_dims = [f"- {d.dimension}: {d.score}/100" for d in review.dimensions]

        stack_name = attempt.tool_stack.name if attempt and hasattr(attempt, 'tool_stack') and attempt.tool_stack else "Unknown Stack"
        return f"""# TRAIT VECTOR EVALUATION
## Attempt #{attempt.attempt_number} | Stack: {stack_name} | Success: {attempt.success}
## Previous Review Scores:
""" + "\n".join(existing_dims[:6]) + f"""
## Code Preview (first 2000 chars):
```
{attempt.code_artifact[:2000]}
```

Evaluate ALL 20 traits individually. Provide SPECIFIC EVIDENCE for each.
Use ONLY scores 1, 3, or 5. At least 5 must be 1. At least 5 must be 5.
Return ONLY JSON matching the required format."""

    def _parse_trait_vector(self, text: str, attempt) -> TraitVector:
        """Parse trait vector from LLM response."""
        import json

        # Extract JSON
        json_text = text
        if "```json" in text:
            json_text = text.split("```json")[-1].split("```")[0]
        elif "```" in text:
            json_text = text.split("```")[-2] if text.count("```") >= 2 else text.split("```")[1]
        json_text = json_text.strip()

        try:
            data = json.loads(json_text)
        except json.JSONDecodeError:
            try:
                start = json_text.find('{')
                end = json_text.rfind('}')
                if start >= 0 and end > start:
                    data = json.loads(json_text[start:end+1])
                else:
                    raise ValueError("No JSON found")
            except Exception as e:
                raise ValueError(f"Trait vector parse failed for attempt {attempt.attempt_id}: {e}")

        # Parse trait scores
        tv_data = data.get("trait_vector", {})
        traits = {}

        category_map = {
            "prompt_understanding": TraitCategory.PROMPT_UNDERSTANDING,
            "feature_correctness": TraitCategory.FEATURE_CORRECTNESS,
            "completeness": TraitCategory.COMPLETENESS,
            "stability": TraitCategory.STABILITY,
            "error_handling": TraitCategory.ERROR_HANDLING,
            "file_organization": TraitCategory.FILE_ORGANIZATION,
            "architecture": TraitCategory.ARCHITECTURE,
            "readability": TraitCategory.READABILITY,
            "reusability": TraitCategory.REUSABILITY,
            "maintainability": TraitCategory.MAINTAINABILITY,
            "ui_ux_quality": TraitCategory.UI_UX_QUALITY,
            "user_flow": TraitCategory.USER_FLOW,
            "responsiveness": TraitCategory.RESPONSIVENESS,
            "accessibility": TraitCategory.ACCESSIBILITY,
            "performance": TraitCategory.PERFORMANCE,
            "creativity": TraitCategory.CREATIVITY,
            "novel_problem_solving": TraitCategory.NOVEL_PROBLEM_SOLVING,
            "stack_selection": TraitCategory.STACK_SELECTION,
            "adaptability": TraitCategory.ADAPTABILITY,
            "impressiveness": TraitCategory.IMPRESSIVENESS,
        }

        for key, category in category_map.items():
            t_data = tv_data.get(key, {})
            if isinstance(t_data, dict):
                score = t_data.get("score", 3)
                # Force valid score
                if score not in [1, 3, 5]:
                    score = 3
                traits[category] = TraitScore(
                    category=category,
                    score=ScoreValue(score),
                    evidence=t_data.get("evidence", "No evidence provided"),
                    reasoning=t_data.get("reasoning", "No reasoning provided"),
                    confidence=max(0, min(100, int(t_data.get("confidence", 50))))
                )

        # Validate: ensure at least 5 ones and 5 fives to prevent clustering
        ones = sum(1 for t in traits.values() if t.score == ScoreValue.POOR)
        fives = sum(1 for t in traits.values() if t.score == ScoreValue.EXCEPTIONAL)

        if ones < 5 or fives < 5:
            self._migration_logger.warning(
                f"CLUSTERING_DETECTED: attempt={attempt.attempt_id}, "
                f"ones={ones}, fives={fives}. Forcing distribution."
            )
            # Force distribution by adjusting lowest/highest scores
            sorted_traits = sorted(traits.items(), key=lambda x: x[1].score.value)
            if ones < 5:
                for i in range(min(5 - ones, len(sorted_traits))):
                    cat, ts = sorted_traits[i]
                    traits[cat] = TraitScore(
                        category=cat, score=ScoreValue.POOR,
                        evidence=ts.evidence + " [FORCED: clustering prevention]",
                        reasoning=ts.reasoning, confidence=max(30, ts.confidence - 20)
                    )
            if fives < 5:
                for i in range(min(5 - fives, len(sorted_traits))):
                    cat, ts = sorted_traits[-(i+1)]
                    traits[cat] = TraitScore(
                        category=cat, score=ScoreValue.EXCEPTIONAL,
                        evidence=ts.evidence + " [FORCED: clustering prevention]",
                        reasoning=ts.reasoning, confidence=max(30, ts.confidence - 20)
                    )

        tv = TraitVector(
            attempt_id=attempt.attempt_id,
            builder_type="unknown",
            traits=traits,
            timestamp=datetime.now().isoformat(),
            reviewer_id=Config.PRIMARYRANKER_MODEL,
        )
        tv.compute_legacy_display()
        return tv

    def _fallback_trait_vector(self, attempt) -> TraitVector:
        """Generate a fallback trait vector with forced spread."""
        import random
        random.seed(hash(attempt.attempt_id) % 10000)

        categories = list(TraitCategory)
        # Force 5 ones, 5 fives, 10 threes
        scores = [1]*5 + [5]*5 + [3]*10
        random.shuffle(scores)

        traits = {}
        for cat, score in zip(categories, scores):
            traits[cat] = TraitScore(
                category=cat,
                score=ScoreValue(score),
                evidence="Fallback scoring: generated due to LLM failure",
                reasoning="Random distribution to prevent clustering",
                confidence=50
            )

        tv = TraitVector(
            attempt_id=attempt.attempt_id,
            builder_type="fallback",
            traits=traits,
            timestamp=datetime.now().isoformat(),
            reviewer_id="fallback"
        )
        tv.compute_legacy_display()
        return tv

    def _compare_trait_vectors(self, trait_vector_pairs) -> List[CanonicalRankedBuild]:
        """Compare trait vectors and return ranked builds.

        Comparison logic (NO weighted averages):
        1. For each pair (A, B), count categories where A.score > B.score
        2. Winner = the one with more dominant categories
        3. Tie-breaker: sum of winning margins
        4. Second tie-breaker: fewer weak categories
        """
        builds = []
        for attempt, tv in trait_vector_pairs:
            cb = CanonicalRankedBuild(
                attempt_id=attempt.attempt_id,
                attempt_number=attempt.attempt_number,
                tool_stack_name=attempt.tool_stack.name if hasattr(attempt, 'tool_stack') and attempt.tool_stack else 'Unknown',
                trait_vector=tv,
                ranker_model=Config.PRIMARYRANKER_MODEL
            )
            cb.compute_display_scores()
            builds.append(cb)

        # Pairwise comparison
        def compare_two(a: CanonicalRankedBuild, b: CanonicalRankedBuild) -> int:
            """Returns 1 if a wins, -1 if b wins, 0 if tie."""
            a_traits = a.trait_vector.traits
            b_traits = b.trait_vector.traits

            a_dominant = 0
            b_dominant = 0
            a_margin = 0
            b_margin = 0
            a_weak = 0
            b_weak = 0

            all_cats = set(a_traits.keys()) | set(b_traits.keys())

            for cat in all_cats:
                a_score = a_traits.get(cat, TraitScore(category=cat, score=ScoreValue.ACCEPTABLE, evidence="No evidence available", reasoning="Default comparison placeholder", confidence=0)).score.value
                b_score = b_traits.get(cat, TraitScore(category=cat, score=ScoreValue.ACCEPTABLE, evidence="No evidence available", reasoning="Default comparison placeholder", confidence=0)).score.value

                if a_score > b_score:
                    a_dominant += 1
                    a_margin += (a_score - b_score)
                elif b_score > a_score:
                    b_dominant += 1
                    b_margin += (b_score - a_score)
                # If equal, no one dominates

            a_weak = b_dominant
            b_weak = a_dominant

            # Primary: more dominant categories
            if a_dominant > b_dominant:
                return 1
            elif b_dominant > a_dominant:
                return -1

            # Tie-breaker 1: sum of winning margins
            if a_margin > b_margin:
                return 1
            elif b_margin > a_margin:
                return -1

            # Tie-breaker 2: fewer weak categories
            if a_weak < b_weak:
                return 1
            elif b_weak < a_weak:
                return -1

            # True tie
            return 0

        # Sort using pairwise comparison
        from functools import cmp_to_key
        ranked = sorted(builds, key=cmp_to_key(lambda a, b: -compare_two(a, b)))

        return ranked

    def compare_pair(self, build_a: CanonicalRankedBuild, build_b: CanonicalRankedBuild) -> TraitVectorComparison:
        """Detailed comparison of two builds."""
        a_traits = build_a.trait_vector.traits
        b_traits = build_b.trait_vector.traits

        dominant = []
        weak = []
        ties = []

        all_cats = set(a_traits.keys()) | set(b_traits.keys())

        for cat in all_cats:
            a_score = a_traits.get(cat, TraitScore(category=cat, score=ScoreValue.ACCEPTABLE, evidence="No evidence available", reasoning="Default comparison placeholder", confidence=0)).score.value
            b_score = b_traits.get(cat, TraitScore(category=cat, score=ScoreValue.ACCEPTABLE, evidence="No evidence available", reasoning="Default comparison placeholder", confidence=0)).score.value

            if a_score > b_score:
                dominant.append(cat)
            elif b_score > a_score:
                weak.append(cat)
            else:
                ties.append(cat)

        winner = build_a if len(dominant) >= len(weak) else build_b
        loser = build_b if winner == build_a else build_a

        return TraitVectorComparison(
            winner_attempt_id=winner.attempt_id,
            loser_attempt_id=loser.attempt_id,
            dominant_traits=dominant,
            weak_traits=weak,
            tie_traits=ties,
            justification=f"{winner.attempt_id} wins by dominating {len(dominant)} categories vs {len(weak)} weak. Ties: {len(ties)}."
        )


# ═══════════════════════════════════════════════════════════════════════════════
#  MIGRATION LOGGING & DEPENDENCY MAP
# ═══════════════════════════════════════════════════════════════════════════════

class MigrationLogger:
    """Logs all accesses to legacy scoring fields to track migration progress."""

    def __init__(self):
        self.logger = logging.getLogger("scoring_migration")
        self.legacy_accesses = []
        self.unresolved_references = set()

    def log_legacy_access(self, source_file: str, line_number: int, field_name: str, context: str):
        """Log when legacy scoring is accessed."""
        entry = {
            "timestamp": datetime.now().isoformat(),
            "source": source_file,
            "line": line_number,
            "field": field_name,
            "context": context,
            "severity": "WARNING"
        }
        self.legacy_accesses.append(entry)
        self.unresolved_references.add(field_name)
        self.logger.warning(
            f"MIGRATION: Legacy field '{field_name}' accessed at {source_file}:{line_number} — {context}"
        )

    def get_dependency_map(self) -> Dict:
        """Generate dependency map showing old vs new paths."""
        return {
            "old_ranking_paths": [
                "Ranker.rank_all() → weighted average of 5 dimensions",
                "Ranker._parse_ranking_json() → overall_score calculation",
                "ScoreValidator → checks total_score spread",
                "LeaderboardSystem → sorts by total_score",
                "PipelineOrchestrator → selects winner by total_score"
            ],
            "new_trait_vector_paths": [
                "TraitVectorRanker.rank_all() → pairwise trait comparison",
                "TraitVectorRanker._compare_trait_vectors() → dominance counting",
                "TraitVectorRanker.compare_pair() → detailed category analysis",
                "CanonicalRankedBuild → stores full trait vector",
                "Selection → uses dominant category count, NOT total_score"
            ],
            "unresolved_legacy_references": sorted(list(self.unresolved_references)),
            "total_legacy_accesses": len(self.legacy_accesses),
            "migration_status": "IN_PROGRESS" if self.unresolved_references else "COMPLETE"
        }

    def print_migration_report(self):
        """Print human-readable migration report."""
        report = self.get_dependency_map()
        print("\n" + "="*70)
        print("TRAIT-VECTOR MIGRATION REPORT")
        print("="*70)
        print(f"\nStatus: {report['migration_status']}")
        print(f"Total Legacy Accesses Logged: {report['total_legacy_accesses']}")

        if report['unresolved_legacy_references']:
            print(f"\n⚠ UNRESOLVED LEGACY REFERENCES ({len(report['unresolved_legacy_references'])}):")
            for ref in report['unresolved_legacy_references']:
                print(f"  - {ref}")
        else:
            print("\n✓ All legacy references resolved")

        print("\nOLD RANKING PATHS (Legacy — Display Only):")
        for path in report['old_ranking_paths']:
            print(f"  [OLD] {path}")

        print("\nNEW TRAIT-VECTOR PATHS (Canonical — Selection & Evolution):")
        for path in report['new_trait_vector_paths']:
            print(f"  [NEW] {path}")

        print("\n" + "="*70)

# Global migration logger
migration_logger = MigrationLogger()

# Configuration lives in core/config.py (kernel). Config dirs created at import.

# ═══════════════════════════════════════════════════════════════════════════════
#  DATA MODELS
# ═══════════════════════════════════════════════════════════════════════════════

from core.models import (
    AppScore,
    BuildAttempt,
    BuildRequest,
    BuilderScore,
    CategoryHistory,
    CodeType,
    EvolutionStrategy,
    LeaderboardEntry,
    NoveltyAttempt,
    RankedBuild,
    ReviewDimension,
    ReviewReport,
    ToolStack,
)

class ToolInventory:
    FRONTEND_TOOLS = {
        "react": {"best_for": ["web_app", "dashboard", "e_commerce"], "synergy": ["nextjs", "tailwind", "typescript"]},
        "vue": {"best_for": ["web_app", "dashboard", "portfolio"], "synergy": ["nuxt", "vuetify", "pinia"]},
        "svelte": {"best_for": ["website", "portfolio", "blog"], "synergy": ["sveltekit", "tailwind", "typescript"]},
        "angular": {"best_for": ["e_commerce", "enterprise", "dashboard"], "synergy": ["rxjs", "material", "typescript"]},
        "nextjs": {"best_for": ["web_app", "e_commerce", "blog"], "synergy": ["react", "tailwind", "vercel"]},
        "nuxt": {"best_for": ["web_app", "portfolio", "blog"], "synergy": ["vue", "tailwind", "netlify"]},
        "astro": {"best_for": ["website", "blog", "portfolio"], "synergy": ["react", "vue", "tailwind"]},
        "htmx": {"best_for": ["website", "dashboard", "blog"], "synergy": ["django", "flask", "alpine"]},
        "alpinejs": {"best_for": ["website", "portfolio", "blog"], "synergy": ["tailwind", "htmx", "django"]},
        "threejs": {"best_for": ["game", "portfolio", "dashboard"], "synergy": ["react", "webgl", "gsap"]},
        "d3": {"best_for": ["dashboard", "data_pipeline", "website"], "synergy": ["react", "svelte", "typescript"]},
        "flutter_web": {"best_for": ["mobile_app", "web_app"], "synergy": ["dart", "firebase"]},
    }

    BACKEND_TOOLS = {
        "fastapi": {"best_for": ["api_backend", "web_app", "dashboard"], "synergy": ["sqlalchemy", "pydantic", "uvicorn"]},
        "django": {"best_for": ["e_commerce", "web_app", "blog"], "synergy": ["htmx", "tailwind", "postgres"]},
        "flask": {"best_for": ["api_backend", "web_app", "cli_tool"], "synergy": ["sqlalchemy", "jinja2", "gunicorn"]},
        "express": {"best_for": ["api_backend", "web_app", "e_commerce"], "synergy": ["mongodb", "typescript", "socket.io"]},
        "spring_boot": {"best_for": ["api_backend", "e_commerce", "enterprise"], "synergy": ["postgres", "redis", "docker"]},
        "go_gin": {"best_for": ["api_backend", "cli_tool", "high_performance"], "synergy": ["postgres", "redis", "docker"]},
        "rust_axum": {"best_for": ["api_backend", "high_performance", "web_app"], "synergy": ["sqlx", "tokio", "docker"]},
        "graphql_apollo": {"best_for": ["api_backend", "web_app", "dashboard"], "synergy": ["react", "prisma", "postgres"]},
        "websocket": {"best_for": ["chatbot", "game", "dashboard"], "synergy": ["socket.io", "redis", "fastapi"]},
        "serverless": {"best_for": ["api_backend", "web_app", "cli_tool"], "synergy": ["aws_lambda", "vercel", "dynamodb"]},
    }

    DATABASE_TOOLS = {
        "postgres": {"best_for": ["e_commerce", "web_app", "blog"], "synergy": ["sqlalchemy", "prisma", "redis"]},
        "mongodb": {"best_for": ["web_app", "chatbot", "dashboard"], "synergy": ["mongoose", "express", "redis"]},
        "sqlite": {"best_for": ["cli_tool", "prototype", "small_app"], "synergy": ["sqlalchemy", "flask", "fastapi"]},
        "redis": {"best_for": ["api_backend", "chatbot", "game"], "synergy": ["postgres", "fastapi", "docker"]},
        "firebase": {"best_for": ["mobile_app", "web_app", "chatbot"], "synergy": ["flutter", "react", "google_cloud"]},
        "supabase": {"best_for": ["web_app", "e_commerce", "blog"], "synergy": ["postgres", "nextjs", "tailwind"]},
        "prisma": {"best_for": ["web_app", "api_backend", "dashboard"], "synergy": ["nextjs", "postgres", "typescript"]},
        "sqlalchemy": {"best_for": ["api_backend", "web_app", "data_pipeline"], "synergy": ["fastapi", "postgres", "flask"]},
        "dynamodb": {"best_for": ["serverless", "web_app", "game"], "synergy": ["aws_lambda", "express", "serverless"]},
    }

    STYLING_TOOLS = {
        "tailwind": {"best_for": ["website", "web_app", "dashboard"], "synergy": ["react", "vue", "nextjs"]},
        "bootstrap": {"best_for": ["website", "dashboard", "e_commerce"], "synergy": ["react", "django", "flask"]},
        "sass": {"best_for": ["website", "portfolio", "blog"], "synergy": ["react", "vue", "angular"]},
        "styled_components": {"best_for": ["web_app", "dashboard", "e_commerce"], "synergy": ["react", "nextjs", "typescript"]},
        "framer_motion": {"best_for": ["portfolio", "web_app", "website"], "synergy": ["react", "tailwind", "nextjs"]},
        "gsap": {"best_for": ["portfolio", "game", "website"], "synergy": ["threejs", "react", "svelte"]},
        "shadcn": {"best_for": ["dashboard", "web_app", "e_commerce"], "synergy": ["react", "tailwind", "nextjs"]},
        "material_ui": {"best_for": ["dashboard", "web_app", "e_commerce"], "synergy": ["react", "nextjs", "typescript"]},
    }

    UTILITY_TOOLS = {
        "typescript": {"best_for": ["web_app", "api_backend", "dashboard"], "synergy": ["react", "nextjs", "express"]},
        "zod": {"best_for": ["api_backend", "web_app", "cli_tool"], "synergy": ["typescript", "nextjs", "fastapi"]},
        "pytest": {"best_for": ["api_backend", "cli_tool", "data_pipeline"], "synergy": ["fastapi", "django", "flask"]},
        "jest": {"best_for": ["web_app", "api_backend", "dashboard"], "synergy": ["react", "nextjs", "express"]},
        "docker": {"best_for": ["api_backend", "web_app", "e_commerce"], "synergy": ["postgres", "redis", "nginx"]},
        "nginx": {"best_for": ["web_app", "api_backend", "e_commerce"], "synergy": ["docker", "ssl", "load_balancer"]},
        "auth_jwt": {"best_for": ["api_backend", "web_app", "e_commerce"], "synergy": ["fastapi", "express", "nextjs"]},
        "stripe": {"best_for": ["e_commerce", "web_app", "saas"], "synergy": ["nextjs", "express", "postgres"]},
        "openai_api": {"best_for": ["chatbot", "web_app", "dashboard"], "synergy": ["fastapi", "react", "nextjs"]},
        "langchain": {"best_for": ["chatbot", "data_pipeline", "web_app"], "synergy": ["fastapi", "openai_api", "postgres"]},
        "pandas": {"best_for": ["data_pipeline", "dashboard", "cli_tool"], "synergy": ["fastapi", "postgres", "streamlit"]},
        "streamlit": {"best_for": ["dashboard", "data_pipeline", "prototype"], "synergy": ["pandas", "plotly", "fastapi"]},
    }

    @classmethod
    def get_tools_for_type(cls, code_type: str) -> Dict[str, List[str]]:
        result = {"frontend": [], "backend": [], "database": [], "styling": [], "utility": []}
        for name, meta in cls.FRONTEND_TOOLS.items():
            if code_type in meta["best_for"]: result["frontend"].append(name)
        for name, meta in cls.BACKEND_TOOLS.items():
            if code_type in meta["best_for"]: result["backend"].append(name)
        for name, meta in cls.DATABASE_TOOLS.items():
            if code_type in meta["best_for"]: result["database"].append(name)
        for name, meta in cls.STYLING_TOOLS.items():
            if code_type in meta["best_for"]: result["styling"].append(name)
        for name, meta in cls.UTILITY_TOOLS.items():
            if code_type in meta["best_for"]: result["utility"].append(name)
        return result

    @classmethod
    def generate_justification(cls, stack: ToolStack, code_type: str) -> str:
        all_tools = {**cls.FRONTEND_TOOLS, **cls.BACKEND_TOOLS, **cls.DATABASE_TOOLS,
                     **cls.STYLING_TOOLS, **cls.UTILITY_TOOLS}
        parts = []
        all_selected = stack.frontend + stack.backend + stack.database + stack.styling + stack.utilities

        parts.append(f"Planting Philosophy: Stack cultivated for '{code_type}' project.")
        parts.append(f"Selection prioritizes {'performance' if 'rust' in str(all_selected).lower() else 'developer experience' if 'react' in str(all_selected).lower() else 'simplicity and speed'}.")

        if stack.frontend:
            frontend = stack.frontend[0]
            meta = all_tools.get(frontend, {})
            parts.append(f"Frontend: {frontend} - excels at {', '.join(meta.get('best_for', ['general use'])[:2])}.")
            synergies = [s for s in meta.get('synergy', []) if s in all_selected]
            if synergies: parts.append(f"Natural synergy with: {', '.join(synergies)}")

        if stack.backend:
            backend = stack.backend[0]
            meta = all_tools.get(backend, {})
            parts.append(f"Backend: {backend} - strength in {', '.join(meta.get('best_for', ['general use'])[:2])}.")
            synergies = [s for s in meta.get('synergy', []) if s in all_selected]
            if synergies: parts.append(f"Complements stack via: {', '.join(synergies)}")

        if stack.database:
            parts.append(f"Database: {stack.database[0]} - matched to {code_type} data patterns.")
        if stack.styling:
            parts.append(f"Styling: {stack.styling[0]} - appropriate visual language.")

        novelty_indicators = []
        if "rust" in str(all_selected).lower(): novelty_indicators.append("Rust ecosystem")
        if "htmx" in str(all_selected).lower(): novelty_indicators.append("Hypermedia architecture")
        if "threejs" in str(all_selected).lower(): novelty_indicators.append("3D/webGL integration")
        if "langchain" in str(all_selected).lower(): novelty_indicators.append("AI-native architecture")
        if novelty_indicators: parts.append(f"Novelty: {', '.join(novelty_indicators)}")

        return "\n".join(parts)


# ═══════════════════════════════════════════════════════════════════════════════
#  FALLBACK CODE GENERATOR (When all LLMs fail)
# ═══════════════════════════════════════════════════════════════════════════════

class FallbackCodeGenerator:
    """Generates sensible fallback code when all LLM APIs fail."""

    TEMPLATES = {
        "website": """```file: index.html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{project_name}}</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-50">
    <nav class="bg-green-800 text-white p-4">
        <div class="container mx-auto">
            <h1 class="text-2xl font-bold">{{project_name}}</h1>
        </div>
    </nav>
    <main class="container mx-auto py-8 px-4">
        <div class="bg-white rounded-lg shadow-md p-6">
            <h2 class="text-xl font-semibold mb-4">Welcome</h2>
            <p class="text-gray-700">{{description}}</p>
            <div class="mt-6 grid grid-cols-1 md:grid-cols-3 gap-4">
                <div class="bg-green-50 p-4 rounded-lg">
                    <h3 class="font-semibold text-green-800">Feature 1</h3>
                    <p class="text-sm text-gray-600">Core functionality placeholder</p>
                </div>
                <div class="bg-green-50 p-4 rounded-lg">
                    <h3 class="font-semibold text-green-800">Feature 2</h3>
                    <p class="text-sm text-gray-600">Secondary functionality placeholder</p>
                </div>
                <div class="bg-green-50 p-4 rounded-lg">
                    <h3 class="font-semibold text-green-800">Feature 3</h3>
                    <p class="text-sm text-gray-600">Tertiary functionality placeholder</p>
                </div>
            </div>
        </div>
    </main>
    <footer class="bg-gray-800 text-white py-4 mt-8">
        <div class="container mx-auto text-center text-sm">
            Generated by AppGarden (Fallback Mode)
        </div>
    </footer>
</body>
</html>
```
```file: README.md
# {{project_name}}

## Description
{{description}}

## Stack
- Frontend: HTML + Tailwind CSS (CDN)
- Backend: Static
- Database: None

## Running
Open `index.html` in any browser.
```""",
        "web_app": """```file: app.py
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

app = FastAPI(title="{{project_name}}")

@app.get("/", response_class=HTMLResponse)
async def root():
    return '''
    <!DOCTYPE html>
    <html>
    <head><title>{{project_name}}</title><script src="https://cdn.tailwindcss.com"></script></head>
    <body class="bg-gray-50 p-8">
        <h1 class="text-3xl font-bold text-green-800 mb-4">{{project_name}}</h1>
        <p class="text-gray-700">{{description}}</p>
        <div class="mt-6 bg-white p-6 rounded-lg shadow">
            <p>Web application initialized. Add routes and components as needed.</p>
        </div>
    </body>
    </html>
    '''

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
```
```file: requirements.txt
fastapi>=0.104.0
uvicorn[standard]>=0.24.0
```
```file: README.md
# {{project_name}}

## Description
{{description}}

## Stack
- Backend: FastAPI
- Frontend: HTML + Tailwind CSS (CDN)

## Running
```bash
pip install -r requirements.txt
python app.py
```
```""",
        "api_backend": """```file: main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, List
import uvicorn

app = FastAPI(title="{{project_name}} API", version="1.0.0")

class Item(BaseModel):
    id: int
    name: str
    description: Optional[str] = None

items = []

@app.get("/")
async def root():
    return {"message": "{{project_name}} API", "status": "running"}

@app.get("/items", response_model=List[Item])
async def get_items():
    return items

@app.post("/items")
async def create_item(item: Item):
    items.append(item)
    return item

@app.get("/items/{item_id}")
async def get_item(item_id: int):
    for item in items:
        if item.id == item_id:
            return item
    raise HTTPException(status_code=404, detail="Item not found")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
```
```file: requirements.txt
fastapi>=0.104.0
uvicorn[standard]>=0.24.0
pydantic>=2.5.0
```
```file: README.md
# {{project_name}}

## Description
{{description}}

## API Endpoints
- GET / - Health check
- GET /items - List all items
- POST /items - Create item
- GET /items/{id} - Get specific item

## Running
```bash
pip install -r requirements.txt
python main.py
```
```""",
        "cli_tool": """```file: cli.py
import argparse
import sys
from datetime import datetime

def main():
    parser = argparse.ArgumentParser(description="{{project_name}}")
    parser.add_argument("--version", action="version", version="1.0.0")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("input", nargs="?", help="Input file or data")

    args = parser.parse_args()

    print(f"{{project_name}} v1.0.0")
    print(f"Description: {{description}}")
    print(f"Run at: {datetime.now().isoformat()}")

    if args.verbose:
        print("Verbose mode enabled")

    if args.input:
        print(f"Processing: {args.input}")
    else:
        print("No input provided. Use --help for usage.")

if __name__ == "__main__":
    main()
```
```file: README.md
# {{project_name}}

## Description
{{description}}

## Usage
```bash
python cli.py --help
python cli.py input.txt --verbose
```
```""",
        "dashboard": """```file: dashboard.py
import streamlit as st
import pandas as pd
import numpy as np

st.set_page_config(page_title="{{project_name}}", layout="wide")

st.title("{{project_name}}")
st.markdown("{{description}}")

# Sidebar
st.sidebar.header("Controls")
show_raw = st.sidebar.checkbox("Show raw data")
num_points = st.sidebar.slider("Data points", 10, 1000, 100)

# Main content
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Users", "1,234", "+12%")
with col2:
    st.metric("Revenue", "$45.2K", "+8%")
with col3:
    st.metric("Active Now", "89", "-3%")

# Chart
data = pd.DataFrame({
    'Date': pd.date_range('2024-01-01', periods=num_points),
    'Value': np.random.randn(num_points).cumsum()
})
st.line_chart(data.set_index('Date'))

if show_raw:
    st.subheader("Raw Data")
    st.dataframe(data)
```
```file: requirements.txt
streamlit>=1.28.0
pandas>=2.1.0
numpy>=1.26.0
```
```file: README.md
# {{project_name}}

## Description
{{description}}

## Running
```bash
pip install -r requirements.txt
streamlit run dashboard.py
```
```""",
        "chatbot": """```file: chatbot.py
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
import uvicorn

app = FastAPI(title="{{project_name}}")

HTML_PAGE = '''
<!DOCTYPE html>
<html>
<head><title>{{project_name}}</title><script src="https://cdn.tailwindcss.com"></script></head>
<body class="bg-gray-100 h-screen flex flex-col">
    <div class="bg-green-800 text-white p-4"><h1 class="text-xl font-bold">{{project_name}}</h1></div>
    <div id="messages" class="flex-1 overflow-y-auto p-4 space-y-2"></div>
    <div class="p-4 bg-white border-t flex gap-2">
        <input id="msg" type="text" class="flex-1 border rounded px-3 py-2" placeholder="Type a message...">
        <button onclick="send()" class="bg-green-600 text-white px-4 py-2 rounded">Send</button>
    </div>
    <script>
        const ws = new WebSocket("ws://" + location.host + "/ws");
        ws.onmessage = (e) => {
            const div = document.createElement("div");
            div.className = "bg-white p-3 rounded shadow text-sm";
            div.innerHTML = "<strong>Bot:</strong> " + e.data;
            document.getElementById("messages").appendChild(div);
        };
        function send() {
            const input = document.getElementById("msg");
            if (!input.value) return;
            const div = document.createElement("div");
            div.className = "bg-green-100 p-3 rounded shadow text-sm ml-auto max-w-md";
            div.innerHTML = "<strong>You:</strong> " + input.value;
            document.getElementById("messages").appendChild(div);
            ws.send(input.value);
            input.value = "";
        }
    </script>
</body>
</html>
'''

@app.get("/")
async def root():
    return HTMLResponse(content=HTML_PAGE)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    await websocket.send_text("Hello! I'm your assistant. How can I help?")
    try:
        while True:
            data = await websocket.receive_text()
            await websocket.send_text(f"You said: {data}. (Integrate LLM here)")
    except:
        pass

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
```
```file: requirements.txt
fastapi>=0.104.0
uvicorn[standard]>=0.24.0
```
```file: README.md
# {{project_name}}

## Description
{{description}}

## Running
```bash
pip install -r requirements.txt
python chatbot.py
```
Then open http://localhost:8000 in your browser.
```""",
    }

    @classmethod
    def generate(cls, code_type: str, description: str, stack: ToolStack) -> str:
        project_name = f"{code_type}_project".replace("_", " ").title()
        template = cls.TEMPLATES.get(code_type, cls.TEMPLATES["website"])
        return template.replace("{{project_name}}", project_name).replace("{{description}}", description)


# ═══════════════════════════════════════════════════════════════════════════════
#  LLM CLIENT
# ═══════════════════════════════════════════════════════════════════════════════

class LLMClient:
    def __init__(self):
        from aiolimiter import AsyncLimiter
        from core import api_keys
        self.semaphore = asyncio.Semaphore(3)
        self.openrouter_limiters: dict[str, AsyncLimiter] = {}
        self.nvidia_limiter = AsyncLimiter(Config.NVIDIA_RATE_LIMIT, 60)
        self.api_keys = api_keys
        self.logger = logging.getLogger("core.llm_client")
        self.key_cooldowns: dict[str, float] = {}

    def _key_hash(self, key: str) -> str:
        return key[:8]

    def _get_openrouter_limiter(self, key: str):
        from aiolimiter import AsyncLimiter
        kh = self._key_hash(key)
        if kh not in self.openrouter_limiters:
            self.openrouter_limiters[kh] = AsyncLimiter(Config.OPENROUTER_RATE_LIMIT, 60)
        return self.openrouter_limiters[kh]

    def _is_key_in_cooldown(self, key: str) -> bool:
        if not key:
            return False
        kh = self._key_hash(key)
        if kh in self.key_cooldowns:
            if time.time() < self.key_cooldowns[kh]:
                return True
            del self.key_cooldowns[kh]
        return False

    def _mark_key_cooldown(self, key: str, duration: float = 30.0) -> None:
        if not key:
            return
        kh = self._key_hash(key)
        self.key_cooldowns[kh] = time.time() + duration
        self.logger.debug("Key %s in cooldown for %.0fs", kh, duration)

    def _get_next_openrouter_key(self):
        """Rotate OpenRouter keys, skipping any in cooldown."""
        max_attempts = 10
        for _ in range(max_attempts):
            key = self.api_keys.get_next_key('openrouter', paid_fallback=True)
            if not key:
                return None
            if not self._is_key_in_cooldown(key):
                return key
        key = self.api_keys.get_next_key('openrouter', paid_fallback=True)
        return key or Config.OPENROUTER_API_KEY

    @property
    def nvidia_key(self):
        key = self.api_keys.get_next_key('nvidia', paid_fallback=False)
        return key or Config.NVIDIA_API_KEY

    @property
    def openrouter_key(self):
        key = self._get_next_openrouter_key()
        return key or Config.OPENROUTER_API_KEY

    async def call_nvidia(self, messages, model=None, temperature=0.7, max_tokens=4000, allow_retries=True):
        async with self.semaphore:
            async with self.nvidia_limiter:
                key = self.nvidia_key
                if not key:
                    return "ERROR: NVIDIA_API_KEY not configured and no stored keys available"
                model = model or Config.RESPONSIBLE_MODEL
                headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
                payload = {"model": model, "messages": messages, "temperature": temperature, "max_tokens": max_tokens, "stream": False}
                key_summary = f"{key[:6]}...{key[-4:]}" if key else "none"
                self.logger.info("NVIDIA request start model=%s key=%s allow_retries=%s", model, key_summary, allow_retries)
                retry_count = 0
                delay = Config.OPENROUTER_RETRY_BACKOFF
                try:
                    async with httpx.AsyncClient(timeout=Config.REQUEST_TIMEOUT) as client:
                        while True:
                            response = await client.post(Config.NVIDIA_API_URL, headers=headers, json=payload)
                            if response.status_code == 429:
                                if not allow_retries:
                                    return f"ERROR: NVIDIA API rate limit exceeded and retries disabled"
                                next_key = self.api_keys.get_next_key('nvidia', paid_fallback=False)
                                if next_key:
                                    key = next_key
                                    headers["Authorization"] = f"Bearer {key}"
                                    await asyncio.sleep(delay)
                                    retry_count += 1
                                    delay *= 2
                                    continue
                                if retry_count >= Config.OPENROUTER_RETRY_COUNT:
                                    return f"ERROR: NVIDIA API rate limit exceeded after {retry_count} retries"
                                await asyncio.sleep(delay)
                                retry_count += 1
                                delay *= 2
                                continue
                            if response.status_code == 404:
                                return f"ERROR: NVIDIA model '{model}' not found (404). Check model name."
                            response.raise_for_status()
                            content = response.json()["choices"][0]["message"]["content"]
                            self.logger.info(
                                "NVIDIA request success model=%s key=%s retries=%s",
                                model,
                                key_summary,
                                retry_count,
                            )
                            return content
                except httpx.HTTPStatusError as e:
                    return f"ERROR: NVIDIA API HTTP {e.response.status_code}: {e.response.text[:200]}"
                except Exception as e:
                    return f"ERROR: NVIDIA API failed: {str(e)}"
            return "ERROR: NVIDIA API request failed after retries"

    async def call_openrouter(self, messages, model=None, temperature=0.7, max_tokens=4000, allow_retries=True, allow_fallback_model=True):
        async with self.semaphore:
            key = self.openrouter_key
            if not key:
                return "ERROR: OPENROUTER_API_KEY not configured and no stored keys available"
            model = model or Config.PRIMARYRANKER_MODEL
            headers = {
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://gardener-platform.local",
                "X-Title": "AppGarden"
            }
            payload = {"model": model, "messages": messages, "temperature": temperature, "max_tokens": max_tokens, "stream": False}
            key_summary = f"{key[:6]}...{key[-4:]}" if key else "none"
            self.logger.info("OpenRouter request start model=%s key=%s allow_retries=%s allow_fallback_model=%s", model, key_summary, allow_retries, allow_fallback_model)
            retry_count = 0
            delay = Config.OPENROUTER_RETRY_BACKOFF
            use_fallback_model = model != Config.FALLBACKRANKER_MODEL and allow_fallback_model
            tried_keys: set[str] = set()
            try:
                while True:
                    limiter = self._get_openrouter_limiter(key)
                    async with limiter:
                        async with httpx.AsyncClient(timeout=Config.REQUEST_TIMEOUT) as client:
                            response = await client.post(Config.OPENROUTER_API_URL, headers=headers, json=payload)
                    if response.status_code in (429, 402):
                        if not allow_retries:
                            return f"ERROR: OpenRouter HTTP {response.status_code}: retries disabled"
                        self.logger.debug(
                            "OpenRouter HTTP %s for model=%s key=%s retry=%s",
                            response.status_code,
                            model,
                            f"{key[:6]}...{key[-4:]}" if key else "none",
                            retry_count,
                        )
                        cooldown = delay * 2
                        self._mark_key_cooldown(key, cooldown)
                        retry_after = response.headers.get("Retry-After")
                        if retry_after is not None and retry_after.isdigit():
                            delay = max(delay, int(retry_after))
                        next_key = self._get_next_openrouter_key()
                        if next_key and next_key != key:
                            key = next_key
                            headers["Authorization"] = f"Bearer {key}"
                            tried_keys.add(key)
                            await asyncio.sleep(delay)
                            retry_count += 1
                            delay = min(delay * 2, 120)
                            continue
                        if Config.OPENROUTER_API_KEY and key != Config.OPENROUTER_API_KEY:
                            key = Config.OPENROUTER_API_KEY
                            headers["Authorization"] = f"Bearer {key}"
                            await asyncio.sleep(delay)
                            retry_count += 1
                            delay = min(delay * 2, 120)
                            continue
                        if use_fallback_model:
                            model = Config.FALLBACKRANKER_MODEL
                            payload["model"] = model
                            use_fallback_model = False
                            retry_count += 1
                            await asyncio.sleep(delay)
                            delay = min(delay * 2, 120)
                            continue
                        if retry_count >= Config.OPENROUTER_RETRY_COUNT:
                            return f"ERROR: OpenRouter rate limit/payment required after {retry_count} retries (check credits)"
                        await asyncio.sleep(delay)
                        retry_count += 1
                        delay = min(delay * 2, 120)
                        continue
                    if response.status_code == 404:
                        if not allow_retries:
                            return f"ERROR: OpenRouter model '{model}' not found (404). Check model name."
                        self.logger.debug(
                            "OpenRouter 404 for model=%s key=%s retry=%s",
                            model,
                            f"{key[:6]}...{key[-4:]}" if key else "none",
                            retry_count,
                        )
                        next_key = self._get_next_openrouter_key()
                        if next_key and next_key != key:
                            key = next_key
                            headers["Authorization"] = f"Bearer {key}"
                            await asyncio.sleep(delay)
                            retry_count += 1
                            delay = min(delay * 2, 120)
                            continue
                        if Config.OPENROUTER_API_KEY and key != Config.OPENROUTER_API_KEY:
                            key = Config.OPENROUTER_API_KEY
                            headers["Authorization"] = f"Bearer {key}"
                            await asyncio.sleep(delay)
                            retry_count += 1
                            delay = min(delay * 2, 120)
                            continue
                        if use_fallback_model:
                            model = Config.FALLBACKRANKER_MODEL
                            payload["model"] = model
                            use_fallback_model = False
                            retry_count += 1
                            await asyncio.sleep(delay)
                            delay = min(delay * 2, 120)
                            continue
                        return f"ERROR: OpenRouter model '{model}' not found (404). Check model name."
                    response.raise_for_status()
                    data = response.json()
                    if "choices" in data and len(data["choices"]) > 0:
                        content = data["choices"][0]["message"]["content"]
                        self.logger.info(
                            "OpenRouter request success model=%s key=%s retries=%s",
                            model,
                            key_summary,
                            retry_count,
                        )
                        return content
                    return "ERROR: Unexpected response from OpenRouter"
            except httpx.HTTPStatusError as e:
                return f"ERROR: OpenRouter HTTP {e.response.status_code}: {e.response.text[:200]}"
            except Exception as e:
                return f"ERROR: OpenRouter failed: {str(e)}"
        return "ERROR: OpenRouter request failed after retries"

    def _normalize_provider(self, provider, model):
        if provider:
            provider_key = str(provider).strip().lower()
            if provider_key in ("nvidia", "nvidiaai", "nvidia_api"):
                return "nvidia"
            if provider_key in ("openrouter", "openrouter_api"):
                return "openrouter"
            raise ValueError(f"Unsupported provider: {provider}")

        if "nvidia" in model.lower():
            return "nvidia"
        if any(x in model.lower() for x in ["openrouter", "meta-llama", "deepseek", "minimax", "qwen", "anthropic"]):
            return "openrouter"
        return None

    async def generate_code(self, prompt, model, system_prompt="", provider=None, allow_retries=True, allow_fallback_model=True):
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        effective_provider = None
        if provider is not None:
            effective_provider = self._normalize_provider(provider, model)
        else:
            effective_provider = self._normalize_provider(None, model)

        self.logger.info(
            "LLM generate_code request model=%s provider=%s allow_retries=%s allow_fallback_model=%s",
            model,
            effective_provider or "auto",
            allow_retries,
            allow_fallback_model,
        )

        if effective_provider == "nvidia":
            return await self.call_nvidia(messages, model=model, temperature=0.3, max_tokens=6000, allow_retries=allow_retries)
        if effective_provider == "openrouter":
            return await self.call_openrouter(
                messages,
                model=model,
                temperature=0.3,
                max_tokens=6000,
                allow_retries=allow_retries,
                allow_fallback_model=allow_fallback_model,
            )

        # Fallback only when model string is ambiguous and no explicit provider was requested.
        if Config.NVIDIA_API_KEY:
            result = await self.call_nvidia(messages, model=model, temperature=0.3, max_tokens=6000, allow_retries=allow_retries)
            if result and not result.startswith("ERROR:"):
                return result
        if Config.OPENROUTER_API_KEY:
            result = await self.call_openrouter(
                messages,
                model=model,
                temperature=0.3,
                max_tokens=6000,
                allow_retries=allow_retries,
                allow_fallback_model=allow_fallback_model,
            )
            if result and not result.startswith("ERROR:"):
                return result
        return "ERROR: No API keys configured or all APIs failed"


# ═══════════════════════════════════════════════════════════════════════════════
#  META-BUILDER ("AppGarden")
# ═══════════════════════════════════════════════════════════════════════════════

class FactoryBuilder:
    def __init__(self, llm_client):
        self.llm = llm_client
        self.inventory = ToolInventory()

    def _generate_variation_1_classic_fullstack(self, code_type, tools):
        stack = ToolStack(
            name="Classic Fullstack",
            frontend=["react", "nextjs"] if "nextjs" in tools["frontend"] else ["react"],
            backend=["fastapi"] if "fastapi" in tools["backend"] else [tools["backend"][0] if tools["backend"] else "flask"],
            database=["postgres", "sqlalchemy"] if "postgres" in tools["database"] else [tools["database"][0] if tools["database"] else "sqlite"],
            styling=["tailwind", "shadcn"] if "tailwind" in tools["styling"] else [tools["styling"][0] if tools["styling"] else "bootstrap"],
            utilities=["typescript", "zod", "docker"] if "typescript" in tools["utility"] else ["docker"],
            deployment=["docker", "nginx"], novelty_score=45
        )
        stack.justification = self.inventory.generate_justification(stack, code_type)
        return stack

    def _generate_variation_2_minimalist_hypermedia(self, code_type, tools):
        stack = ToolStack(
            name="Minimalist Hypermedia",
            frontend=["htmx", "alpinejs"] if "htmx" in tools["frontend"] else ["svelte"],
            backend=["django"] if "django" in tools["backend"] else ["flask"],
            database=["sqlite"] if "sqlite" in tools["database"] else ["postgres"],
            styling=["tailwind"] if "tailwind" in tools["styling"] else ["bootstrap"],
            utilities=["auth_jwt", "pytest"], deployment=["docker"], novelty_score=65
        )
        stack.justification = self.inventory.generate_justification(stack, code_type)
        return stack

    def _generate_variation_3_ai_native(self, code_type, tools):
        stack = ToolStack(
            name="AI-Native Architecture",
            frontend=["react", "nextjs"] if "react" in tools["frontend"] else ["vue"],
            backend=["fastapi", "graphql_apollo"] if "fastapi" in tools["backend"] else ["express"],
            database=["supabase", "prisma"] if "supabase" in tools["database"] else ["postgres", "prisma"],
            styling=["tailwind", "framer_motion"] if "framer_motion" in tools["styling"] else ["tailwind"],
            utilities=["langchain", "openai_api", "typescript", "zod"],
            deployment=["docker", "serverless"], novelty_score=85
        )
        stack.justification = self.inventory.generate_justification(stack, code_type)
        return stack

    def _generate_variation_4_performance_rust(self, code_type, tools):
        stack = ToolStack(
            name="High-Performance Rust",
            frontend=["svelte", "htmx"] if "svelte" in tools["frontend"] else ["react"],
            backend=["rust_axum"],
            database=["postgres", "redis"] if "redis" in tools["database"] else ["postgres"],
            styling=["tailwind", "gsap"] if "gsap" in tools["styling"] else ["tailwind"],
            utilities=["docker", "nginx", "auth_jwt"],
            deployment=["docker", "nginx"], novelty_score=90
        )
        stack.justification = self.inventory.generate_justification(stack, code_type)
        return stack

    def _generate_variation_5_realtime_collaborative(self, code_type, tools):
        stack = ToolStack(
            name="Real-Time Collaborative",
            frontend=["vue", "nuxt"] if "vue" in tools["frontend"] else ["react"],
            backend=["express", "websocket"] if "express" in tools["backend"] else ["fastapi", "websocket"],
            database=["mongodb", "redis"] if "mongodb" in tools["database"] else ["postgres", "redis"],
            styling=["tailwind", "material_ui"] if "material_ui" in tools["styling"] else ["tailwind"],
            utilities=["typescript", "docker", "auth_jwt"],
            deployment=["docker", "serverless"], novelty_score=75
        )
        stack.justification = self.inventory.generate_justification(stack, code_type)
        return stack

    def generate_tool_combinations(self, code_type, preferred=[]):
        tools = self.inventory.get_tools_for_type(code_type)
        if preferred:
            for category in tools:
                preferred_in = [p for p in preferred if p in tools[category]]
                if preferred_in:
                    tools[category] = preferred_in + [t for t in tools[category] if t not in preferred_in]
        return [
            self._generate_variation_1_classic_fullstack(code_type, tools),
            self._generate_variation_2_minimalist_hypermedia(code_type, tools),
            self._generate_variation_3_ai_native(code_type, tools),
            self._generate_variation_4_performance_rust(code_type, tools),
            self._generate_variation_5_realtime_collaborative(code_type, tools),
        ]

    def plan(self, ctx: PipelineContext):
        """Write tool stacks into ctx.plan."""
        stacks = self.generate_tool_combinations(
            ctx.request.code_type.value,
            ctx.request.preferred_frameworks,
        )
        ctx.plan.tool_combinations = stacks
        # persist plan artifacts immediately to avoid lost stacks during later
        # completion/cleanup steps
        try:
            ctx.db.save_checkpoint(
                ctx.build_id,
                {"tool_combinations": [t.model_dump() for t in stacks]},
            )
        except Exception:
            pass
        return stacks

# ═══════════════════════════════════════════════════════════════════════════════
#  BUILDER BOT
# ═══════════════════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════════════════
#  RESPONSIBLE BUILDER (Builds final deployable app)
# ═══════════════════════════════════════════════════════════════════════════════

class ResponsibleBuilder:
    """Builds production-ready, deployable code. Conservative, reliable, tested."""
    SYSTEM_PROMPT = """You are a senior software engineer who builds PRODUCTION-READY code.
Your code MUST be:
- Fully functional and runnable
- Well-tested with error handling
- Properly structured with clean architecture
- Documented with README and comments
- Using best practices for the chosen stack

Generate COMPLETE, working code. No stubs. No placeholders. No TODOs.
Every feature must be implemented and working."""

    def __init__(self, llm_client):
        self.llm = llm_client

    async def build(self, ctx_or_request, stack, attempt_number):
        request = ctx_or_request.request if isinstance(ctx_or_request, PipelineContext) else ctx_or_request
        attempt_id = f"resp_{uuid.uuid4().hex[:8]}"
        start_time = time.time()

        model = Config.RESPONSIBLE_MODEL
        model_name = "Responsible Builder (" + Config.RESPONSIBLE_MODEL + ")"

        prompt = self._construct_build_prompt(request, stack, attempt_number)

        build_log = f"[{datetime.now().isoformat()}] Responsible build attempt {attempt_number}\n"
        build_log += f"[{datetime.now().isoformat()}] Stack: {stack.name} | Model: {model_name}\n"

        try:
            code_artifact = await self.llm.generate_code(prompt, model, self.SYSTEM_PROMPT)
            if code_artifact.startswith("ERROR:"):
                success, error = False, code_artifact
                build_log += f"[{datetime.now().isoformat()}] FAILED: {error}\n"
            else:
                success, error = True, ""
                build_log += f"[{datetime.now().isoformat()}] SUCCESS: {len(code_artifact)} chars\n"
        except Exception as e:
            code_artifact, success, error = "", False, str(e)
            build_log += f"[{datetime.now().isoformat()}] EXCEPTION: {error}\n"

        build_time = time.time() - start_time
        build_log += f"[{datetime.now().isoformat()}] Completed in {build_time:.2f}s\n"

        attempt = BuildAttempt(
            attempt_id=attempt_id, attempt_number=attempt_number, tool_stack=stack,
            model_used=model_name, code_artifact=code_artifact, build_log=build_log,
            tool_usage_report=self._generate_tool_report(stack, code_artifact, success),
            build_time_seconds=build_time, success=success, error_message=error,
            timestamp=datetime.now().isoformat()
        )
        if success and isinstance(ctx_or_request, PipelineContext):
            attempt = await apply_builder_tools(
                attempt,
                build_id=ctx_or_request.build_id,
                code_type=request.code_type.value,
                ctx=ctx_or_request,
            )
        return attempt

    def _construct_build_prompt(self, request, stack, attempt_number):
        return f"""# PRODUCTION BUILD REQUEST
## Project Type: {request.code_type.value}
## Description: {request.description}
## Requirements: {request.specific_requirements or "None"}
## Audience: {request.target_audience or "General"}
## Complexity: {request.complexity_level}

## TOOL STACK (Responsible Build #{attempt_number}): {stack.name}
### Frontend:
""" + "\n".join(f"- {t}" for t in stack.frontend) + """
### Backend:
""" + "\n".join(f"- {t}" for t in stack.backend) + """
### Database:
""" + "\n".join(f"- {t}" for t in stack.database) + """
### Styling:
""" + "\n".join(f"- {t}" for t in stack.styling) + """
### Utilities:
""" + "\n".join(f"- {t}" for t in stack.utilities) + """
### Deployment:
""" + "\n".join(f"- {t}" for t in stack.deployment) + f"""

## JUSTIFICATION:
{stack.justification}

Generate COMPLETE, production-ready, DEPLOYABLE code using ONLY these tools.
Every feature must work. Include tests. Include error handling."""

    def _generate_tool_report(self, stack, code, success):
        report = f"Tool Usage Report for {stack.name}\n"
        for tool in stack.frontend + stack.backend + stack.database + stack.styling + stack.utilities:
            mentions = code.lower().count(tool.lower().replace("_", " "))
            report += f"{tool}: {mentions} mentions - {'Used' if mentions > 0 else 'Not detected'}\n"
        report += f"Status: {'SUCCESS' if success else 'FAILED'}"
        return report

class CreativeBuilder:
    SYSTEM_PROMPT = """You are an expert software architect. Generate COMPLETE, production-ready code.
RULES:
1. Generate COMPLETE, runnable code
2. Include all necessary files
3. Use ONLY assigned tools/frameworks
4. Provide file structure comments
5. Include error handling and validation
6. Write clean, well-commented code
7. Include README as comments
8. Make code NOVEL and CREATIVE
OUTPUT FORMAT:
```file: filename.ext
// code content
```"""

    def __init__(self, llm_client):
        self.llm = llm_client

    async def build(self, ctx_or_request, stack, attempt_number):
        request = ctx_or_request.request if isinstance(ctx_or_request, PipelineContext) else ctx_or_request
        attempt_id = f"build_{uuid.uuid4().hex[:8]}"
        start_time = time.time()

        # Use NVIDIA for attempts 1-2, OpenRouter for 3-5
        if attempt_number <= 2:
            model = Config.CREATIVE_MODEL
            model_name = "Creative Builder Round 1 (" + Config.CREATIVE_MODEL + ")"
        else:
            model = Config.MINIMAX_MODEL
            model_name = "Creative Builder Round 2 (" + Config.MINIMAX_MODEL + ")"

        prompt = self._construct_build_prompt(request, stack, attempt_number)

        build_log = f"[{datetime.now().isoformat()}] Starting build attempt {attempt_number}\n"
        build_log += f"[{datetime.now().isoformat()}] Stack: {stack.name} | Model: {model_name}\n"

        try:
            code_artifact = await self.llm.generate_code(prompt, model, self.SYSTEM_PROMPT)
            if code_artifact.startswith("ERROR:"):
                success, error = False, code_artifact
                build_log += f"[{datetime.now().isoformat()}] FAILED: {error}\n"
            else:
                success, error = True, ""
                build_log += f"[{datetime.now().isoformat()}] SUCCESS: {len(code_artifact)} chars\n"
        except Exception as e:
            code_artifact, success, error = "", False, str(e)
            build_log += f"[{datetime.now().isoformat()}] EXCEPTION: {error}\n"

        build_time = time.time() - start_time
        build_log += f"[{datetime.now().isoformat()}] Completed in {build_time:.2f}s\n"

        attempt = BuildAttempt(
            attempt_id=attempt_id, attempt_number=attempt_number, tool_stack=stack,
            model_used=model_name, code_artifact=code_artifact, build_log=build_log,
            tool_usage_report=self._generate_tool_report(stack, code_artifact, success),
            build_time_seconds=build_time, success=success, error_message=error,
            timestamp=datetime.now().isoformat()
        )
        if success and isinstance(ctx_or_request, PipelineContext):
            attempt = await apply_builder_tools(
                attempt,
                build_id=ctx_or_request.build_id,
                code_type=request.code_type.value,
                ctx=ctx_or_request,
            )
        return attempt

    def _construct_build_prompt(self, request, stack, attempt_number):
        return f"""# BUILD REQUEST
## Project Type: {request.code_type.value}
## Description: {request.description}
## Requirements: {request.specific_requirements or "None"}
## Audience: {request.target_audience or "General"}
## Complexity: {request.complexity_level}

## TOOL STACK (Attempt #{attempt_number}): {stack.name}
### Frontend:
""" + "\n".join(f"- {t}" for t in stack.frontend) + """
### Backend:
""" + "\n".join(f"- {t}" for t in stack.backend) + """
### Database:
""" + "\n".join(f"- {t}" for t in stack.database) + """
### Styling:
""" + "\n".join(f"- {t}" for t in stack.styling) + """
### Utilities:
""" + "\n".join(f"- {t}" for t in stack.utilities) + """
### Deployment:
""" + "\n".join(f"- {t}" for t in stack.deployment) + f"""

## JUSTIFICATION:
{stack.justification}

Generate COMPLETE, production-ready code using ONLY these tools."""

    def _generate_tool_report(self, stack, code, success):
        report = f"Tool Usage Report for {stack.name}\n"
        for tool in stack.frontend + stack.backend + stack.database + stack.styling + stack.utilities:
            mentions = code.lower().count(tool.lower().replace("_", " "))
            report += f"{tool}: {mentions} mentions - {'Used' if mentions > 0 else 'Not detected'}\n"
        report += f"Status: {'SUCCESS' if success else 'FAILED'}"
        return report

    async def build_with_feedback(self, ctx: PipelineContext, stack, attempt_number, feedback):
        """Build with corrective feedback from previous round."""
        request = ctx.request
        attempt_id = f"build_{uuid.uuid4().hex[:8]}"
        start_time = time.time()

        # Use OpenRouter for round 2 (different model = fresh perspective)
        model = Config.MINIMAX_MODEL
        model_name = "Creative Builder Feedback Round (" + Config.MINIMAX_MODEL + ")"

        prompt = self._construct_build_prompt(request, stack, attempt_number)
        # Append feedback to the prompt
        prompt += f"\n\n## CORRECTIVE FEEDBACK FROM PREVIOUS ATTEMPT:\n{feedback}\n\n"
        prompt += "Address ALL issues above. Generate corrected, working code."

        build_log = f"[{datetime.now().isoformat()}] Round 2 build attempt {attempt_number}\n"
        build_log += f"[{datetime.now().isoformat()}] Stack: {stack.name} | Model: {model_name}\n"

        try:
            code_artifact = await self.llm.generate_code(prompt, model, self.SYSTEM_PROMPT)
            if code_artifact.startswith("ERROR:"):
                success, error = False, code_artifact
                build_log += f"[{datetime.now().isoformat()}] FAILED: {error}\n"
            else:
                success, error = True, ""
                build_log += f"[{datetime.now().isoformat()}] SUCCESS: {len(code_artifact)} chars\n"
        except Exception as e:
            code_artifact, success, error = "", False, str(e)
            build_log += f"[{datetime.now().isoformat()}] EXCEPTION: {error}\n"

        build_time = time.time() - start_time
        build_log += f"[{datetime.now().isoformat()}] Completed in {build_time:.2f}s\n"

        attempt = BuildAttempt(
            attempt_id=attempt_id, attempt_number=attempt_number, tool_stack=stack,
            model_used=model_name, code_artifact=code_artifact, build_log=build_log,
            tool_usage_report=self._generate_tool_report(stack, code_artifact, success),
            build_time_seconds=build_time, success=success, error_message=error,
            timestamp=datetime.now().isoformat()
        )
        if success:
            attempt = await apply_builder_tools(
                attempt,
                build_id=ctx.build_id,
                code_type=request.code_type.value,
                ctx=ctx,
            )
        return attempt


# ═══════════════════════════════════════════════════════════════════════════════
#  LLM REVIEWER
# ═══════════════════════════════════════════════════════════════════════════════


class CodeQualityReviewer:
    """Reviewer A: Focuses on code correctness, architecture, and tool efficiency."""
    SYSTEM_PROMPT = """You are a senior software engineer specializing in code quality and architecture.
Analyze code and output ONLY a valid JSON object with no markdown, no explanation, no comments.

CRITICAL RULES:
1. NEVER default to 70. Use the FULL 0-100 range.
2. If code has bugs, score below 60.
3. If code is excellent, score above 85.
4. If code is mediocre/average, score 50-65.
5. Be HARSH. Most code should score 40-75. Only exceptional code gets 80+.

REQUIRED JSON FORMAT:
{
  "code_correctness": {"score": 0-100, "analysis": "specific bugs, syntax errors, runtime issues", "suggestions": ["fix 1", "fix 2"]},
  "tool_efficiency": {"score": 0-100, "analysis": "are tools combined well? any anti-patterns?", "suggestions": ["improvement 1"]},
  "architecture": {"score": 0-100, "analysis": "structure, separation of concerns, scalability", "suggestions": ["suggestion 1"]},
  "overall_score": 0-100,
  "comparative_notes": "how this compares to other attempts",
  "improvement_suggestions": ["top 3 improvements"],
  "potential_failure_points": ["risk 1", "risk 2"]
}

SCORING RUBRIC (be critical, use full 0-100 range):
- 90-100: Flawless, production-grade architecture (rare)
- 80-89: Good structure, minor issues (uncommon)
- 70-79: Works but has structural debt (somewhat common)
- 60-69: Significant architectural problems (common)
- 50-59: Poor structure, hard to maintain (common)
- 0-49: Broken or fundamentally wrong (if bugs exist)

Score based on ACTUAL code quality. Be harsh. Most builds should NOT score 70."""

    def __init__(self, llm_client):
        self.llm = llm_client

    async def review_all(self, ctx_or_request, attempts=None):
        if isinstance(ctx_or_request, PipelineContext):
            ctx = ctx_or_request
            attempts = ctx.files.all_attempts
            request = ctx.request
        else:
            request, attempts = ctx_or_request, attempts
        return [await self._review_single(request, a, attempts) for a in attempts]

    async def _review_single(self, request, attempt, all_attempts):
        prompt = self._construct_review_prompt(request, attempt, all_attempts)
        try:
            review_json = await self.llm.generate_code(
                prompt,
                Config.CODEQUALITY_MODEL,
                self.SYSTEM_PROMPT,
                provider=Config.CODEQUALITY_PROVIDER,
                allow_retries=False,
                allow_fallback_model=False,
            )
            return self._parse_review_json(review_json, attempt.attempt_id)
        except Exception as e:
            return self._fallback_review(attempt.attempt_id, str(e))

    def _construct_review_prompt(self, request, attempt, all_attempts):
        others = [a for a in all_attempts if a.attempt_id != attempt.attempt_id]
        stack_name = attempt.tool_stack.name if hasattr(attempt, 'tool_stack') and attempt.tool_stack else 'Unknown Stack'
        return f"""# CODE QUALITY REVIEW
## Project: {request.code_type.value} - {request.description}
## Attempt #{attempt.attempt_number} | Stack: {stack_name} | Success: {attempt.success}
## Code (first 3000 chars):
```
{attempt.code_artifact[:3000]}
```
## Other Attempts:
""" + "\n".join(f"- #{a.attempt_number}: {a.tool_stack.name if hasattr(a, 'tool_stack') and a.tool_stack else 'Unknown'}" for a in others) + f"""

Return ONLY JSON. Focus on: correctness, architecture, tool usage. Score 0-100."""

    def _parse_review_json(self, text, attempt_id):
        import json
        json_text = text
        if "```json" in text:
            json_text = text.split("```json")[-1].split("```")[0]
        elif "```" in text:
            json_text = text.split("```")[-2] if text.count("```") >= 2 else text.split("```")[1]
        json_text = json_text.strip()

        try:
            data = json.loads(json_text)
        except json.JSONDecodeError:
            try:
                start, end = json_text.find('{'), json_text.rfind('}')
                data = json.loads(json_text[start:end+1]) if start >= 0 and end > start else {}
            except: 
                return self._fallback_review(attempt_id, f"Parse failed: {text[:200]}")

        dimensions = [
            ReviewDimension(dimension="Code Correctness", score=max(0,min(100,int(data.get("code_correctness",{}).get("score",70)))), analysis=data.get("code_correctness",{}).get("analysis",""), suggestions=data.get("code_correctness",{}).get("suggestions",[])),
            ReviewDimension(dimension="Tool Efficiency", score=max(0,min(100,int(data.get("tool_efficiency",{}).get("score",70)))), analysis=data.get("tool_efficiency",{}).get("analysis",""), suggestions=data.get("tool_efficiency",{}).get("suggestions",[])),
            ReviewDimension(dimension="Architecture", score=max(0,min(100,int(data.get("architecture",{}).get("score",70)))), analysis=data.get("architecture",{}).get("analysis",""), suggestions=data.get("architecture",{}).get("suggestions",[])),
        ]
        overall = max(0, min(100, int(data.get("overall_score", sum(d.score for d in dimensions)//len(dimensions) if dimensions else 50))))

        return ReviewReport(
            attempt_id=attempt_id, overall_score=overall, dimensions=dimensions,
            comparative_notes=data.get("comparative_notes", ""),
            what_works_better="",
            improvement_suggestions=data.get("improvement_suggestions", []),
            potential_failure_points=data.get("potential_failure_points", []),
            reviewer_model=Config.CODEQUALITY_MODEL,
            timestamp=datetime.now().isoformat()
        )

    def _fallback_review(self, attempt_id, error):
        return ReviewReport(
            attempt_id=attempt_id, overall_score=50,
            dimensions=[ReviewDimension(dimension=d, score=50, analysis=f"Review failed: {error}", suggestions=[])
                       for d in ["Code Correctness", "Tool Efficiency", "Architecture"]],
            comparative_notes="", what_works_better="",
            improvement_suggestions=["Retry"], potential_failure_points=["System failure"],
            reviewer_model=Config.CODEQUALITY_MODEL,
            timestamp=datetime.now().isoformat()
        )


class ProductQualityReviewer:
    """Reviewer B: Focuses on novelty, documentation, UX, and product thinking."""
    SYSTEM_PROMPT = """You are a product manager and UX expert who judges code by its real-world value.
Analyze code and output ONLY a valid JSON object with no markdown, no explanation, no comments.

CRITICAL RULES:
1. NEVER default to 70. Use the FULL 0-100 range.
2. If the solution is boring/generic, score below 60.
3. If the solution is creative and well-documented, score above 80.
4. If the solution is average, score 50-65.
5. Be HARSH. Most code should score 40-75. Only exceptional work gets 80+.

REQUIRED JSON FORMAT:
{
  "novelty": {"score": 0-100, "analysis": "how creative/original is this approach?", "suggestions": ["idea 1"]},
  "documentation": {"score": 0-100, "analysis": "README quality, comments, onboarding", "suggestions": ["doc fix 1"]},
  "ux_product": {"score": 0-100, "analysis": "user experience, flow, accessibility", "suggestions": ["ux fix 1"]},
  "overall_score": 0-100,
  "comparative_notes": "how this compares to other attempts",
  "improvement_suggestions": ["top 3 improvements"],
  "potential_failure_points": ["risk 1", "risk 2"]
}

SCORING RUBRIC (be critical, use full 0-100 range):
- 90-100: Revolutionary approach, exceptional UX, perfect docs (rare)
- 80-89: Creative, well-documented, good UX (uncommon)
- 70-79: Standard approach, adequate docs, okay UX (somewhat common)
- 60-69: Boring, sparse docs, poor UX (common)
- 50-59: Copy-paste job, no docs, bad UX (common)
- 0-49: Completely unoriginal, no documentation, unusable (if applicable)

Score based on REAL PRODUCT VALUE. Be harsh. Most builds should NOT score 70."""

    def __init__(self, llm_client):
        self.llm = llm_client

    async def review_all(self, ctx_or_request, attempts=None):
        if isinstance(ctx_or_request, PipelineContext):
            request, attempts = ctx_or_request.request, ctx_or_request.files.all_attempts
        else:
            request, attempts = ctx_or_request, attempts
        return [await self._review_single(request, a, attempts) for a in attempts]

    async def _review_single(self, request, attempt, all_attempts):
        prompt = self._construct_review_prompt(request, attempt, all_attempts)
        try:
            review_json = await self.llm.generate_code(
                prompt,
                Config.PRODUCT_MODEL,
                self.SYSTEM_PROMPT,
                provider=Config.PRODUCT_PROVIDER,
                allow_retries=False,
                allow_fallback_model=False,
            )
            return self._parse_review_json(review_json, attempt.attempt_id)
        except Exception as e:
            return self._fallback_review(attempt.attempt_id, str(e))

    def _construct_review_prompt(self, request, attempt, all_attempts):
        others = [a for a in all_attempts if a.attempt_id != attempt.attempt_id]
        stack_name = attempt.tool_stack.name if hasattr(attempt, 'tool_stack') and attempt.tool_stack else 'Unknown Stack'
        return f"""# PRODUCT QUALITY REVIEW
## Project: {request.code_type.value} - {request.description}
## Attempt #{attempt.attempt_number} | Stack: {stack_name} | Success: {attempt.success}
## Code (first 3000 chars):
```
{attempt.code_artifact[:3000]}
```
## Other Attempts:
""" + "\n".join(f"- #{a.attempt_number}: {a.tool_stack.name if hasattr(a, 'tool_stack') and a.tool_stack else 'Unknown'}" for a in others) + f"""

Return ONLY JSON. Focus on: novelty, documentation, UX/product value. Score 0-100."""

    def _parse_review_json(self, text, attempt_id):
        import json
        json_text = text
        if "```json" in text:
            json_text = text.split("```json")[-1].split("```")[0]
        elif "```" in text:
            json_text = text.split("```")[-2] if text.count("```") >= 2 else text.split("```")[1]
        json_text = json_text.strip()

        try:
            data = json.loads(json_text)
        except json.JSONDecodeError:
            try:
                start, end = json_text.find('{'), json_text.rfind('}')
                data = json.loads(json_text[start:end+1]) if start >= 0 and end > start else {}
            except:
                return self._fallback_review(attempt_id, f"Parse failed: {text[:200]}")

        dimensions = [
            ReviewDimension(dimension="Novelty", score=max(0,min(100,int(data.get("novelty",{}).get("score",70)))), analysis=data.get("novelty",{}).get("analysis",""), suggestions=data.get("novelty",{}).get("suggestions",[])),
            ReviewDimension(dimension="Documentation", score=max(0,min(100,int(data.get("documentation",{}).get("score",70)))), analysis=data.get("documentation",{}).get("analysis",""), suggestions=data.get("documentation",{}).get("suggestions",[])),
            ReviewDimension(dimension="UX/Product", score=max(0,min(100,int(data.get("ux_product",{}).get("score",70)))), analysis=data.get("ux_product",{}).get("analysis",""), suggestions=data.get("ux_product",{}).get("suggestions",[])),
        ]
        overall = max(0, min(100, int(data.get("overall_score", sum(d.score for d in dimensions)//len(dimensions) if dimensions else 50))))

        return ReviewReport(
            attempt_id=attempt_id, overall_score=overall, dimensions=dimensions,
            comparative_notes=data.get("comparative_notes", ""),
            what_works_better="",
            improvement_suggestions=data.get("improvement_suggestions", []),
            potential_failure_points=data.get("potential_failure_points", []),
            reviewer_model=Config.PRODUCT_MODEL,
            timestamp=datetime.now().isoformat()
        )

    def _fallback_review(self, attempt_id, error):
        return ReviewReport(
            attempt_id=attempt_id, overall_score=50,
            dimensions=[ReviewDimension(dimension=d, score=50, analysis=f"Review failed: {error}", suggestions=[])
                       for d in ["Novelty", "Documentation", "UX/Product"]],
            comparative_notes="", what_works_better="",
            improvement_suggestions=["Retry"], potential_failure_points=["System failure"],
            reviewer_model=Config.PRODUCT_MODEL,
            timestamp=datetime.now().isoformat()
        )


class CreativeCoderReviewer:
    """Reviewer C: The visionary who sees what COULD be, not just what is.

    This reviewer acts like a creative director who:
    - Imagines alternative implementations the builder missed
    - Suggests wild features that would elevate the project
    - Identifies missed opportunities for delight and surprise
    - Proposes unconventional tech combinations
    - Dreams up features that make users go "wow"

    They score based on POTENTIAL, not just execution.
    A boring but working app scores low. A flawed but visionary app scores high.
    """

    SYSTEM_PROMPT = """You are a visionary creative technologist who sees possibilities others miss.
You judge code not by what it does, but by what it COULD do.

Your personality:
- You get excited by unconventional ideas
- You spot missed opportunities for delight
- You suggest features that seem impossible but aren't
- You think in "what ifs" and "imagine if"
- You value audacity over perfection

Output ONLY a valid JSON object with no markdown, no explanation.

REQUIRED JSON FORMAT:
{
  "vision_score": {"score": 0-100, "analysis": "how bold/ambitious is the vision?", "missed_opportunities": ["idea 1", "idea 2"]},
  "alternative_approaches": {"score": 0-100, "analysis": "what other ways could this be built?", "suggestions": ["alt approach 1", "alt approach 2"]},
  "delight_potential": {"score": 0-100, "analysis": "what moments of joy are missing?", "suggestions": ["delight 1", "delight 2"]},
  "tech_creativity": {"score": 0-100, "analysis": "are tools used creatively? any unconventional combos?", "suggestions": ["tech idea 1"]},
  "overall_score": 0-100,
  "wild_ideas": ["a crazy feature that would make this unforgettable", "an unexpected tech twist"],
  "if_i_were_building_this": "one paragraph on how I would approach this differently",
  "improvement_suggestions": ["specific creative improvements"],
  "potential_failure_points": ["where the vision might exceed the execution"]
}

SCORING RUBRIC (score POTENTIAL and VISION, not just execution):
- 90-100: Mind-blowing vision, world-changing potential, unforgettable concept
- 80-89: Highly creative, surprising approaches, strong "wow" factor
- 70-79: Good ideas but playing it safe, missed some opportunities
- 60-69: Standard implementation, could be much more ambitious
- 50-59: Boring, predictable, zero imagination
- 0-49: Actively unimaginative, copy-paste thinking, no soul

NEVER default to 70. Most builds should score 50-75. Only visionary work gets 80+.
Be the reviewer who makes builders think 'damn, I wish I had thought of that.'"""

    def __init__(self, llm_client):
        self.llm = llm_client

    async def review_all(self, ctx_or_request, attempts=None):
        if isinstance(ctx_or_request, PipelineContext):
            request, attempts = ctx_or_request.request, ctx_or_request.files.all_attempts
        else:
            request, attempts = ctx_or_request, attempts
        return [await self._review_single(request, a, attempts) for a in attempts]

    async def _review_single(self, request, attempt, all_attempts):
        prompt = self._construct_review_prompt(request, attempt, all_attempts)
        try:
            review_json = await self.llm.generate_code(
                prompt,
                Config.CREATIVECODER_MODEL,
                self.SYSTEM_PROMPT,
                provider=Config.CREATIVECODER_PROVIDER,
                allow_retries=False,
                allow_fallback_model=False,
            )
            return self._parse_review_json(review_json, attempt.attempt_id)
        except Exception as e:
            return self._fallback_review(attempt.attempt_id, str(e))

    def _construct_review_prompt(self, request, attempt, all_attempts):
        others = [a for a in all_attempts if a.attempt_id != attempt.attempt_id]
        stack_name = attempt.tool_stack.name if hasattr(attempt, 'tool_stack') and attempt.tool_stack else 'Unknown Stack'
        return f"""# CREATIVE VISION REVIEW
## Project: {request.code_type.value} - {request.description}
## Attempt #{attempt.attempt_number} | Stack: {stack_name} | Success: {attempt.success}
## Code (first 3000 chars):
```
{attempt.code_artifact[:3000]}
```
## Other Attempts:
""" + "\n".join(f"- #{a.attempt_number}: {a.tool_stack.name if hasattr(a, 'tool_stack') and a.tool_stack else 'Unknown'}" for a in others) + f"""

## YOUR MISSION
Don't just review what IS. Imagine what COULD BE.
What wild features are missing? What unconventional approaches were ignored?
If you were dreaming up the most exciting version of this project, what would it look like?

Return ONLY JSON. Score the VISION and POTENTIAL. Be provocative."""

    def _parse_review_json(self, text, attempt_id):
        import json
        json_text = text
        if "```json" in text:
            json_text = text.split("```json")[-1].split("```")[0]
        elif "```" in text:
            json_text = text.split("```")[-2] if text.count("```") >= 2 else text.split("```")[1]
        json_text = json_text.strip()

        try:
            data = json.loads(json_text)
        except json.JSONDecodeError:
            try:
                start, end = json_text.find('{'), json_text.rfind('}')
                data = json.loads(json_text[start:end+1]) if start >= 0 and end > start else {}
            except:
                return self._fallback_review(attempt_id, f"Parse failed: {text[:200]}")

        dimensions = [
            ReviewDimension(dimension="Vision Score", score=max(0,min(100,int(data.get("vision_score",{}).get("score",70)))), analysis=data.get("vision_score",{}).get("analysis",""), suggestions=data.get("vision_score",{}).get("missed_opportunities",[])),
            ReviewDimension(dimension="Alternative Approaches", score=max(0,min(100,int(data.get("alternative_approaches",{}).get("score",70)))), analysis=data.get("alternative_approaches",{}).get("analysis",""), suggestions=data.get("alternative_approaches",{}).get("suggestions",[])),
            ReviewDimension(dimension="Delight Potential", score=max(0,min(100,int(data.get("delight_potential",{}).get("score",70)))), analysis=data.get("delight_potential",{}).get("analysis",""), suggestions=data.get("delight_potential",{}).get("suggestions",[])),
            ReviewDimension(dimension="Tech Creativity", score=max(0,min(100,int(data.get("tech_creativity",{}).get("score",70)))), analysis=data.get("tech_creativity",{}).get("analysis",""), suggestions=data.get("tech_creativity",{}).get("suggestions",[])),
        ]
        overall = max(0, min(100, int(data.get("overall_score", sum(d.score for d in dimensions)//len(dimensions) if dimensions else 50))))

        # Add wild ideas as additional suggestions
        wild_ideas = data.get("wild_ideas", [])
        if_i_were = data.get("if_i_were_building_this", "")
        all_suggestions = list(set(data.get("improvement_suggestions", []) + wild_ideas))
        if if_i_were:
            all_suggestions.append(f"Creative Director's vision: {if_i_were}")

        return ReviewReport(
            attempt_id=attempt_id, overall_score=overall, dimensions=dimensions,
            comparative_notes=data.get("comparative_notes", ""),
            what_works_better="",
            improvement_suggestions=all_suggestions,
            potential_failure_points=data.get("potential_failure_points", []),
            reviewer_model=Config.CREATIVECODER_MODEL,
            timestamp=datetime.now().isoformat()
        )

    def _fallback_review(self, attempt_id, error):
        return ReviewReport(
            attempt_id=attempt_id, overall_score=50,
            dimensions=[ReviewDimension(dimension=d, score=50, analysis=f"Review failed: {error}", suggestions=[])
                       for d in ["Vision Score", "Alternative Approaches", "Delight Potential", "Tech Creativity"]],
            comparative_notes="", what_works_better="",
            improvement_suggestions=["Retry"], potential_failure_points=["System failure"],
            reviewer_model=Config.CREATIVECODER_MODEL,
            timestamp=datetime.now().isoformat()
        )


class FactoryReviewer:
    """Reviews the Factory's builder-generation strategy.

    Did the Factory:
    - Pick appropriate tool stacks for the request?
    - Set clear goals and constraints?
    - Choose the right builder type for the job?
    - Provide useful success criteria?
    """

    SYSTEM_PROMPT = """You are an expert software architect who evaluates build strategies.
Review the Factory's approach to generating builders and output ONLY JSON.

REQUIRED JSON FORMAT:
{
  "stack_appropriateness": {"score": 0-100, "analysis": "were the chosen stacks right for the project?"},
  "goal_clarity": {"score": 0-100, "analysis": "were builder goals clear and achievable?"},
  "builder_selection": {"score": 0-100, "analysis": "was the right builder type chosen?"},
  "constraint_quality": {"score": 0-100, "analysis": "were constraints helpful or limiting?"},
  "overall_score": 0-100,
  "improvement_suggestions": ["how to improve factory strategy"]
}

SCORING:
- 90-100: Perfect strategy, ideal stacks, clear goals
- 80-89: Good strategy, minor improvements possible
- 70-79: Adequate but missed opportunities
- 60-69: Poor stack choices or unclear goals
- 50-59: Wrong builder type or bad constraints
- 0-49: Completely misguided strategy

NEVER default to 70. Be critical."""

    def __init__(self, llm_client):
        self.llm = llm_client

    async def review_plan(self, ctx: PipelineContext):
        return await self.review_factory(
            ctx.request,
            ctx.plan.tool_combinations,
            "ResponsibleBuilder",
        )

    async def review_factory(self, request, tool_combinations, builder_type):
        prompt = f"""# FACTORY STRATEGY REVIEW
## User Request: {request.code_type.value} - {request.description}
## Requirements: {request.specific_requirements or "None"}
## Complexity: {request.complexity_level}
## Builder Type Selected: {builder_type}

## Tool Stacks Generated ({len(tool_combinations)}):
""" + "\n".join(f"{i+1}. {t.name}: frontend={t.frontend}, backend={t.backend}, db={t.database}" for i, t in enumerate(tool_combinations)) + f"""

## Review the Factory's strategy:
1. Are these stacks appropriate for a {request.code_type.value} project?
2. Are the goals clear for a builder working with these stacks?
3. Is {builder_type} the right builder type for this request?
4. Are the constraints helpful or overly limiting?

Return ONLY JSON matching the required format."""

        try:
            review_json = await self.llm.generate_code(
                prompt,
                Config.FACTORY_MODEL,
                self.SYSTEM_PROMPT,
                provider=Config.FACTORY_PROVIDER,
                allow_retries=False,
                allow_fallback_model=False,
            )
            return self._parse_json(review_json)
        except Exception as e:
            return {"overall_score": 50, "error": str(e)}

    def _parse_json(self, text):
        import json

        if not text:
            return {"overall_score": 50}

        json_text = text
        if "```json" in text:
            json_text = text.split("```json")[-1].split("```")[0]
        elif "```" in text:
            json_text = text.split("```")[-2] if text.count("```") >= 2 else text.split("```")[1]
        json_text = json_text.strip()

        try:
            parsed = json.loads(json_text)
            if not isinstance(parsed, dict) or "overall_score" not in parsed:
                return {"overall_score": 50}
            return parsed
        except Exception:
            try:
                start, end = json_text.find('{'), json_text.rfind('}')
                if start >= 0 and end > start:
                    parsed = json.loads(json_text[start:end+1])
                    if not isinstance(parsed, dict) or "overall_score" not in parsed:
                        return {"overall_score": 50}
                    return parsed
                return {"overall_score": 50}
            except Exception:
                return {"overall_score": 50}


class BuilderReviewer:
    """Reviews the Builder's coding ability and implementation quality.

    Did the Builder:
    - Write clean, working code?
    - Follow best practices?
    - Handle errors properly?
    - Use the stack effectively?
    """

    SYSTEM_PROMPT = """You are a senior software engineer who evaluates coding skill.
Review the Builder's code output and output ONLY JSON.

REQUIRED JSON FORMAT:
{
  "code_quality": {"score": 0-100, "analysis": "is the code clean and well-structured?"},
  "best_practices": {"score": 0-100, "analysis": "does it follow language/framework conventions?"},
  "error_handling": {"score": 0-100, "analysis": "are errors handled gracefully?"},
  "stack_usage": {"score": 0-100, "analysis": "are the chosen tools used effectively?"},
  "completeness": {"score": 0-100, "analysis": "is the implementation complete or stub-filled?"},
  "overall_score": 0-100,
  "improvement_suggestions": ["how the builder could code better"]
}

SCORING:
- 90-100: Production-grade code, exemplary practices
- 80-89: Good code, minor issues
- 70-79: Works but has technical debt
- 60-69: Significant code quality issues
- 50-59: Poor practices, many bugs
- 0-49: Broken, non-functional, or unreadable

NEVER default to 70. Be critical."""

    def __init__(self, llm_client):
        self.llm = llm_client

    async def review_all_builders(self, ctx: PipelineContext):
        reviews = []
        for attempt in ctx.files.all_attempts:
            reviews.append(await self.review_builder(attempt, ctx.request))
        return reviews

    async def review_builder(self, attempt, request):
        prompt = f"""# BUILDER CODE REVIEW
## Project: {request.code_type.value} - {request.description}
## Builder: {attempt.model_used}
## Stack: {attempt.tool_stack.name if hasattr(attempt, 'tool_stack') and attempt.tool_stack else 'Unknown'}
## Success: {attempt.success}

## Code (first 3000 chars):
```
{attempt.code_artifact[:3000]}
```

## Review the Builder's coding:
1. Is the code clean and well-structured?
2. Does it follow best practices for {', '.join(attempt.tool_stack.frontend + attempt.tool_stack.backend) if attempt and hasattr(attempt, 'tool_stack') and attempt.tool_stack else 'the chosen stack'}?
3. Are errors handled properly?
4. Are the chosen tools used effectively?
5. Is the implementation complete or full of stubs?

Return ONLY JSON matching the required format."""

        try:
            review_json = await self.llm.generate_code(
                prompt,
                Config.BUILDERREVIEWER_MODEL,
                self.SYSTEM_PROMPT,
                provider=Config.BUILDERREVIEWER_PROVIDER,
                allow_retries=False,
                allow_fallback_model=False,
            )
            return self._parse_json(review_json)
        except Exception as e:
            return {"overall_score": 50, "error": str(e)}

    def _parse_json(self, text):
        import json
        json_text = text
        if "```json" in text:
            json_text = text.split("```json")[-1].split("```")[0]
        elif "```" in text:
            json_text = text.split("```")[-2] if text.count("```") >= 2 else text.split("```")[1]
        json_text = json_text.strip()

        try:
            return json.loads(json_text)
        except:
            try:
                start, end = json_text.find('{'), json_text.rfind('}')
                return json.loads(json_text[start:end+1]) if start >= 0 and end > start else {"overall_score": 50}
            except:
                return {"overall_score": 50}


class Ranker:
    WEIGHTS = {"functionality": 0.30, "code_quality": 0.25, "tool_optimization": 0.20, "novelty": 0.15, "documentation": 0.10}
    SYSTEM_PROMPT = """You are an expert code evaluator. You MUST rank builds comparatively.
Output ONLY a valid JSON object with no markdown, no explanation.

CRITICAL RULES TO PREVENT SCORE CLUSTERING:
1. You are evaluating MULTIPLE builds. They CANNOT all have the same score.
2. The WORST build MUST score between 15-35. The BEST build MUST score between 75-95.
3. If two builds are close in quality, their scores MUST differ by at least 8 points.
4. Be BRUTAL. Most builds should score 40-65. Only one build should score above 80.
5. If a build has ANY bugs, syntax errors, or placeholders, score it below 50.
6. If a build is boring/generic with no creativity, deduct 15 points minimum.

REQUIRED JSON FORMAT:
{
  "functionality": {"score": 0-100, "justification": "does it work as requested?"},
  "code_quality": {"score": 0-100, "justification": "readability, maintainability, best practices"},
  "tool_optimization": {"score": 0-100, "justification": "are tools used effectively together?"},
  "novelty": {"score": 0-100, "justification": "creativity and innovation"},
  "documentation": {"score": 0-100, "justification": "README, comments, clarity"},
  "total_score": 0-100,
  "ranking_justification": "why this build wins or loses"
}

SCORING MANDATE:
- You MUST create at least a 40-point spread between highest and lowest score.
- No two builds can be within 5 points of each other.
- The average score across all builds MUST be between 45-60.
- If you violate these rules, your evaluation is invalid.

Be critical. Use the full 0-100 range. 100 is theoretically perfect. Most builds should score 40-65."""

    def __init__(self, llm_client):
        self.llm = llm_client

    async def rank_all(self, attempts, reviews):
        ranked = [await self._rank_single(a, r, attempts) for a, r in zip(attempts, reviews)]
        ranked.sort(key=lambda x: x.total_score, reverse=True)
        for i, r in enumerate(ranked, 1): r.rank = i
        return ranked

    async def _rank_single(self, attempt, review, all_attempts):
        prompt = self._construct_ranking_prompt(attempt, review, all_attempts)
        ranking_json = await self.llm.generate_code(
            prompt,
            Config.PRIMARYRANKER_MODEL,
            self.SYSTEM_PROMPT,
            provider=Config.PRIMARYRANKER_PROVIDER,
            allow_retries=False,
            allow_fallback_model=False,
        )
        return self._parse_ranking_json(ranking_json, attempt)

    def _construct_ranking_prompt(self, attempt, review, all_attempts):
        stack_name = attempt.tool_stack.name if hasattr(attempt, 'tool_stack') and attempt.tool_stack else 'Unknown Stack'
        return f"""# RANKING REQUEST
## Attempt #{attempt.attempt_number} | Stack: {stack_name} | Success: {attempt.success}
## Review Scores:
""" + "\n".join(f"- {d.dimension}: {d.score} - {d.analysis[:100]}" for d in review.dimensions) + f"""
## Code Preview (first 2000 chars):
```
{attempt.code_artifact[:2000]}
```
## Other Attempts:
""" + "\n".join(f"- #{a.attempt_number}: {a.tool_stack.name if hasattr(a, 'tool_stack') and a.tool_stack else 'Unknown'} (success={a.success})" for a in all_attempts if a.attempt_id != attempt.attempt_id) + f"""

Return ONLY JSON matching the required format. Score critically using full 0-100 range."""

    def _parse_ranking_json(self, text, attempt):
        """Parse JSON ranking with fallback."""
        import json

        json_text = text
        if "```json" in text:
            json_text = text.split("```json")[-1].split("```")[0]
        elif "```" in text:
            json_text = text.split("```")[-2] if text.count("```") >= 2 else text.split("```")[1]

        json_text = json_text.strip()

        try:
            data = json.loads(json_text)
        except json.JSONDecodeError:
            try:
                start = json_text.find('{')
                end = json_text.rfind('}')
                if start >= 0 and end > start:
                    data = json.loads(json_text[start:end+1])
                else:
                    raise ValueError("No JSON found")
            except Exception as e:
                raise ValueError(f"Ranking JSON parse failed for attempt {attempt.attempt_id}: {e}")

        def get_score(key):
            val = data.get(key, {})
            if isinstance(val, dict):
                return max(0, min(100, int(val.get("score", 70))))
            return max(0, min(100, int(val))) if isinstance(val, (int, float)) else 70

        func = get_score("functionality")
        quality = get_score("code_quality")
        tool = get_score("tool_optimization")
        novelty = get_score("novelty")
        doc = get_score("documentation")

        total = data.get("total_score")
        if isinstance(total, dict):
            total = total.get("score", None)
        if total is None:
            total = func * 0.30 + quality * 0.25 + tool * 0.20 + novelty * 0.15 + doc * 0.10
        else:
            total = max(0, min(100, float(total)))

        justification = data.get("ranking_justification", "")
        if isinstance(justification, dict):
            justification = justification.get("justification", "")

        return RankedBuild(
            attempt_id=attempt.attempt_id, attempt_number=attempt.attempt_number,
            tool_stack_name=attempt.tool_stack.name if hasattr(attempt, 'tool_stack') and attempt.tool_stack else 'Unknown', functionality_score=func,
            code_quality_score=quality, tool_optimization_score=tool,
            novelty_score=novelty, documentation_score=doc,
            total_score=round(total, 2), justification=str(justification)[:500], rank=0,
            ranker_model=Config.PRIMARYRANKER_MODEL
        )


class NoveltySiteBuilder:
    SYSTEM_PROMPT = """You are a creative coding virtuoso. Build the most NOVEL, CREATIVE, 
INNOVATIVE version possible. Push boundaries. Use unexpected patterns. Include delightful 
micro-interactions. Make code a work of art. Each iteration MORE creative than last.
Generate COMPLETE, runnable code."""

    def __init__(self, llm_client):
        self.llm = llm_client

    async def build_novelty(self, ctx: PipelineContext):
        request = ctx.request
        winning_attempt = ctx.rankings.winning_attempt
        if not winning_attempt:
            return []
        winning_stack = getattr(winning_attempt, 'tool_stack', None)
        if not winning_stack:
            return []
        attempts = []
        prev_code = winning_attempt.code_artifact
        for iteration in range(1, 4):
            attempt = await self._build_iteration(request, winning_stack, iteration, prev_code, attempts)
            attempts.append(attempt)
            if attempt.success:
                prev_code = attempt.code_artifact
        ctx.files.novelty_attempts = attempts
        return attempts

    async def _build_iteration(self, request, stack, iteration, prev_code, prev_attempts):
        attempt_id = f"novelty_{uuid.uuid4().hex[:8]}"
        start_time = time.time()
        model = Config.NOVELTY_MODEL if Config.NOVELTY_MODEL else Config.CREATIVE_MODEL if iteration % 2 == 1 else Config.MINIMAX_MODEL
        prompt = self._construct_novelty_prompt(request, stack, iteration, prev_code, prev_attempts)

        build_log = f"[{datetime.now().isoformat()}] Novelty iteration {iteration} starting\n"
        try:
            code = await self.llm.generate_code(prompt, model, self.SYSTEM_PROMPT)
            if code.startswith("ERROR:"):
                success, error = False, code
                build_log += f"[{datetime.now().isoformat()}] FAILED: {error}\n"
            else:
                success, error = True, ""
                build_log += f"[{datetime.now().isoformat()}] SUCCESS: {len(code)} chars\n"
        except Exception as e:
            code, success, error = "", False, str(e)
            build_log += f"[{datetime.now().isoformat()}] EXCEPTION: {error}\n"

        return NoveltyAttempt(attempt_id=attempt_id, iteration=iteration, winning_config=stack,
                             code_artifact=code, build_log=build_log,
                             creativity_notes=self._creativity_notes(iteration, stack, success),
                             build_time_seconds=time.time() - start_time, success=success,
                             timestamp=datetime.now().isoformat())

    def _construct_novelty_prompt(self, request, stack, iteration, prev_code, prev_attempts):
        learnings = ""
        if prev_attempts:
            learnings = "\n## PREVIOUS LEARNINGS:\n" + "\n".join(
                f"Iteration {p.iteration}: {p.creativity_notes}" for p in prev_attempts)

        directions = {
            1: "Focus: Unexpected visual design, unique palettes, creative layouts. Add: Smooth animations, micro-interactions.",
            2: "Focus: Advanced interactivity, gamification, storytelling. Add: Physics-based animations, 3D elements.",
            3: "Focus: Pushing absolute boundaries. Add: Experimental features, artistic code expression."
        }

        return f"""# NOVELTY BUILD - Iteration {iteration}
## Request: {request.code_type.value} - {request.description}
## Stack: {stack.name}
{learnings}
## Directions: {directions.get(iteration, "Be creative")}
## Previous Code (do NOT copy):
```
{prev_code[:2000]}
```
Generate the MOST CREATIVE version. Make it unforgettable."""

    def _creativity_notes(self, iteration, stack, success):
        notes = [f"Iteration {iteration} approach:"]
        if iteration == 1: notes.extend(["Visual innovation and micro-interactions", "Unexpected color theory"])
        elif iteration == 2: notes.extend(["Gamification and advanced interactivity", "Storytelling in UX"])
        else: notes.extend(["Pushing creative boundaries", "Experimental features"])
        notes.extend([f"Stack: {stack.name}", f"Success: {success}"])
        return "\n".join(notes)


# ═══════════════════════════════════════════════════════════════════════════════
#  LEADERBOARD SYSTEM
# ═══════════════════════════════════════════════════════════════════════════════


class _IsolatedLegacyRanker:
    """Wrapper that isolates the legacy Ranker to prevent authoritative use.

    This ranker CANNOT influence:
    - Selection (which build wins)
    - Survival (which builds proceed)
    - Ranking order (which build is #1, #2, etc.)
    - Evolution (which traits get mutated)

    It ONLY produces display scores for backward-compatible UI rendering.
    Any attempt to use it for selection will be logged and rejected.
    """

    def __init__(self, legacy_ranker_instance):
        self._ranker = legacy_ranker_instance
        self._logger = logging.getLogger("legacy_ranker_isolation")
        self._access_count = 0

    async def rank_all(self, attempts, reviews):
        """Generate display-only scores. Returns legacy format for UI compatibility."""
        self._access_count += 1
        self._logger.warning(
            f"LEGACY_RANKER_ACCESS #{self._access_count}: "
            f"Display-only ranking called. NOT used for selection."
        )
        # Call the actual legacy ranker but mark results as display-only
        ranked = await self._ranker.rank_all(attempts, reviews)
        # Mark each result as non-authoritative
        for r in ranked:
            if hasattr(r, 'justification'):
                r.justification = f"[DISPLAY-ONLY] {r.justification}"
        return ranked

    def __getattr__(self, name):
        """Block any attribute access that could be used for selection."""
        if name in ('select_winner', 'get_best', 'compare', 'dominates'):
            raise RuntimeError(
                f"LEGACY_RANKER_BLOCKED: Attempted to access '{name}' for selection. "
                f"Use TraitVectorRanker for all authoritative decisions."
            )
        return getattr(self._ranker, name)

class LeaderboardSystem:
    """SQLite-backed leaderboard via the shared AppDatabase."""

    def __init__(self, db_path=None):
        from core.database import AppDatabase, get_database

        self.db = AppDatabase(db_path) if db_path else get_database()

    def add_entry(
        self,
        entry,
        trait_vector=None,
        dominant_traits=None,
        weak_traits=None,
        builder_traits=None,
        build_id=None,
    ):
        return self.db.add_leaderboard_entry(
            entry,
            build_id=build_id,
            trait_vector=trait_vector,
            dominant_traits=dominant_traits,
            weak_traits=weak_traits,
            builder_traits=builder_traits,
        )

    def get_entries(self, timeframe="all_time", code_type=None, sort_by="score", limit=50):
        rows = self.db.get_leaderboard_entries(timeframe, code_type, sort_by, limit)
        entries = []
        for row in rows:
            entry = LeaderboardEntry(
                entry_id=row["entry_id"],
                project_name=row["project_name"],
                code_type=row["code_type"],
                score=row["score"],
                novelty_rating=row["novelty_rating"],
                tool_stack=row["tool_stack"],
                build_time_seconds=row["build_time_seconds"],
                user_rating=row["user_rating"],
                created_at=row["created_at"],
                download_path=row["download_path"],
                model_used=row["model_used"] or "",
            )
            entry.trait_vector = row.get("trait_vector")
            entry.dominant_traits = row.get("dominant_traits") or []
            entry.weak_traits = row.get("weak_traits") or []
            entry.builder_traits = row.get("builder_traits")
            entries.append(entry)
        return entries

    def get_stats(self):
        return self.db.get_leaderboard_stats()

    def rate_entry(self, entry_id, rating):
        return self.db.rate_leaderboard_entry(entry_id, rating)


# ═══════════════════════════════════════════════════════════════════════════════
#  DOWNLOAD MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class DownloadManager:
    def __init__(self, output_dir=None):
        self.output_dir = output_dir or Config.OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def create_package(self, project_name, code_artifact, tool_stack, build_request):
        package_id = f"{project_name}_{uuid.uuid4().hex[:8]}"
        package_dir = self.output_dir / package_id
        package_dir.mkdir(parents=True, exist_ok=True)

        files = self._parse_code_artifact(code_artifact)

        src_dir = package_dir / "src"
        src_dir.mkdir(exist_ok=True)
        for filename, content in files.items():
            file_path = src_dir / filename
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(content, encoding="utf-8")

        readme = self._generate_readme(project_name, tool_stack, build_request, files)
        (package_dir / "README.md").write_text(readme, encoding="utf-8")

        requirements = self._generate_requirements(tool_stack)
        (package_dir / "requirements.txt").write_text(requirements, encoding="utf-8")

        package_info = {
            "project_name": project_name, "generated_at": datetime.now().isoformat(),
            "tool_stack": tool_stack.model_dump(), "build_request": build_request.model_dump(),
            "files": list(files.keys())
        }
        (package_dir / "package.json").write_text(json.dumps(package_info, indent=2), encoding="utf-8")

        zip_path = self.output_dir / f"{package_id}.zip"
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for file_path in package_dir.rglob("*"):
                if file_path.is_file():
                    zf.write(file_path, file_path.relative_to(package_dir))

        shutil.rmtree(package_dir)
        return str(zip_path)

    def _parse_code_artifact(self, code_artifact):
        files = {}
        # Pattern 1: ```file: filename.ext\ncode\n```
        pattern = r'```(?:file:\s*)?([^\n]+)\n(.*?)```'
        matches = re.findall(pattern, code_artifact, re.DOTALL)
        if matches:
            for filename, content in matches:
                filename = filename.strip()
                if filename and content.strip():
                    files[filename] = content.strip()
        else:
            # Pattern 2: // FILE: filename.ext\ncode\n
            pattern2 = r'(?:^|\n)//?\s*FILE:\s*([^\n]+)\n(.*?)(?=\n//?\s*FILE:|$)'
            matches2 = re.findall(pattern2, code_artifact, re.DOTALL | re.IGNORECASE)
            if matches2:
                for filename, content in matches2:
                    filename = filename.strip()
                    if filename and content.strip():
                        files[filename] = content.strip()
            else:
                files["main.py"] = code_artifact
        return files

    def _generate_readme(self, project_name, tool_stack, build_request, files):
        file_list = "\n".join(files.keys())
        return f"""# {project_name}

## Generated by AppGarden

A multi-agent code generation platform that cultivates the best code.

## Project Info
- **Type**: {build_request.code_type.value}
- **Description**: {build_request.description}
- **Generated**: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Tool Stack
- **Frontend**: {', '.join(tool_stack.frontend)}
- **Backend**: {', '.join(tool_stack.backend)}
- **Database**: {', '.join(tool_stack.database)}
- **Styling**: {', '.join(tool_stack.styling)}
- **Utilities**: {', '.join(tool_stack.utilities)}

## Files
```
{file_list}
```

## Getting Started
```bash
pip install -r requirements.txt
python src/main.py
```

## Stack Justification
{tool_stack.justification}

---
*Generated by AppGarden Platform*
"""

    def _generate_requirements(self, tool_stack):
        requirements = []
        all_tools = str(tool_stack.backend + tool_stack.database + tool_stack.utilities + tool_stack.styling).lower()
        if "fastapi" in all_tools: requirements.extend(["fastapi>=0.104.0", "uvicorn[standard]>=0.24.0"])
        if "flask" in all_tools: requirements.extend(["flask>=3.0.0", "gunicorn>=21.0.0"])
        if "django" in all_tools: requirements.extend(["django>=5.0.0"])
        if "sqlalchemy" in all_tools or "postgres" in all_tools: requirements.extend(["sqlalchemy>=2.0.0", "psycopg2-binary>=2.9.0"])
        if "redis" in all_tools: requirements.extend(["redis>=5.0.0"])
        if "pydantic" in all_tools or "zod" in all_tools: requirements.extend(["pydantic>=2.5.0", "email-validator>=2.1.0"])
        if "pytest" in all_tools: requirements.extend(["pytest>=7.4.0", "pytest-asyncio>=0.21.0"])
        if "docker" in all_tools: requirements.append("docker>=6.1.0")
        if "openai" in all_tools or "langchain" in all_tools: requirements.extend(["openai>=1.0.0", "langchain>=0.1.0"])
        if "pandas" in all_tools: requirements.extend(["pandas>=2.1.0", "numpy>=1.26.0"])
        if "streamlit" in all_tools: requirements.extend(["streamlit>=1.28.0"])
        if "auth" in all_tools: requirements.extend(["PyJWT>=2.8.0", "passlib[bcrypt]>=1.7.0"])
        if "stripe" in all_tools: requirements.extend(["stripe>=7.0.0"])
        requirements.extend(["python-dotenv>=1.0.0", "httpx>=0.25.0", "jinja2>=3.1.0"])
        return "\n".join(sorted(set(requirements)))


# ═══════════════════════════════════════════════════════════════════════════════
#  PIPELINE ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════════════════
#  SCORE VALIDATOR (Prevents all-70 baseline scoring)
# ═══════════════════════════════════════════════════════════════════════════════

class ScoreValidator:
    """Validates that scores are meaningful and not all defaulting to the same value."""

    @staticmethod
    def validate_reviews(reviews, default_threshold=75):
        """Check if all reviews have suspiciously similar scores."""
        if not reviews:
            return False, "No reviews to validate"

        all_scores = []
        for review in reviews:
            for dim in review.dimensions:
                all_scores.append(dim.score)

        if not all_scores:
            return False, "No dimension scores found"

        # Check if all scores are identical
        if len(set(all_scores)) == 1:
            return False, f"All scores identical at {all_scores[0]} - likely LLM defaulting"

        # Check if all scores are within 5 points of each other
        score_range = max(all_scores) - min(all_scores)
        if score_range < 5:
            return False, f"Score range only {score_range} - too narrow for meaningful ranking"

        # Check if too many scores are at the default threshold
        at_default = sum(1 for s in all_scores if abs(s - default_threshold) <= 3)
        if at_default / len(all_scores) > 0.7:
            return False, f"{at_default}/{len(all_scores)} scores near {default_threshold} - likely defaulting"

        return True, f"Score range: {min(all_scores)}-{max(all_scores)}, spread: {score_range}"

    @staticmethod
    def validate_rankings(ranked_builds):
        """Validate trait-vector ranking spread."""
        if not ranked_builds or len(ranked_builds) < 2:
            return False, "Need at least 2 builds"

        dominance_spreads = []

        for rb in ranked_builds:
            traits = rb.trait_vector.traits.values()
            values = [t.score.value for t in traits]

            ones = values.count(1)
            threes = values.count(3)
            fives = values.count(5)

            dominance_spreads.append((ones, threes, fives))

        unique_patterns = len(set(dominance_spreads))

        if unique_patterns <= 1:
            return False, "Trait distributions are identical"

        return True, f"{unique_patterns} unique trait distributions detected"

class PipelineOrchestrator:
    def __init__(self):
        self.llm = LLMClient()
        self.factory_builder = FactoryBuilder(self.llm)
        self.responsible_builder = ResponsibleBuilder(self.llm)  # FIXED: Was missing!
        self.creative_builder = CreativeBuilder(self.llm)
        self.code_reviewer = CodeQualityReviewer(self.llm)
        self.product_reviewer = ProductQualityReviewer(self.llm)
        self.creative_reviewer = CreativeCoderReviewer(self.llm)
        self.factory_reviewer = FactoryReviewer(self.llm)
        self.builder_reviewer = BuilderReviewer(self.llm)
        # CANONICAL: TraitVectorRanker is the ONLY authoritative ranker
        # It drives selection, survival, and evolution decisions
        self.primary_ranker = TraitVectorRanker(self.llm)

        # LEGACY: Isolated compatibility-only ranker
        # - Does NOT influence selection, survival, or ranking
        # - Used ONLY for backward-compatible display scores
        # - Wrapped to prevent accidental authoritative use
        self._legacy_ranker = _IsolatedLegacyRanker(Ranker(self.llm))

        # FALLBACK: Only used if canonical ranker completely fails
        self.fallback_ranker = TraitVectorRanker(self.llm)
        self.novelty_builder = NoveltySiteBuilder(self.llm)
        from core.database import get_database

        self.db = get_database()
        self.leaderboard = LeaderboardSystem()
        self.download_manager = DownloadManager()
        from core.tools.registry import ToolRegistry

        self.tool_registry = ToolRegistry.default()
        self.current_build_id = None

    def _fsm(self, build_id: str) -> PipelineStateMachine:
        return PipelineStateMachine(self.db, build_id)

    def create_context(
        self,
        request: BuildRequest,
        build_id: str,
        *,
        resume: bool = False,
        stage: str = "",
    ) -> PipelineContext:
        from core.pipeline_store import _services_from_orchestrator

        services = _services_from_orchestrator(self)
        return PipelineContext(
            build_id=build_id,
            request=request,
            prompt=request.model_dump(),
            db=self.db,
            fsm=self._fsm(build_id),
            services=services,
            stage=stage,
            resume=resume,
            download_manager=self.download_manager,
            leaderboard=self.leaderboard,
            stack_factory=self._stacks_from_checkpoint,
            attempt_factory=self._attempts_from_checkpoint,
            ranked_factory=self._ranked_from_checkpoint,
            review_factory=lambda data: [ReviewReport(**r) for r in data],
            novelty_factory=lambda data: [NoveltyAttempt(**a) for a in data],
        )

    async def build_all_attempts(self, ctx: PipelineContext):
        stacks = ctx.plan.tool_combinations
        resp_tasks = [
            self.responsible_builder.build(ctx, stack, i + 1)
            for i, stack in enumerate(stacks)
        ]
        creative_tasks = [
            self.creative_builder.build(ctx, stack, i + 1)
            for i, stack in enumerate(stacks)
        ]
        return await asyncio.gather(*resp_tasks), await asyncio.gather(*creative_tasks)

    async def apply_build_fallbacks(self, ctx: PipelineContext):
        all_attempts = ctx.files.resp_attempts + ctx.files.creative_attempts
        stacks = ctx.plan.tool_combinations
        for i, attempt in enumerate(all_attempts):
            if not attempt.success:
                stack = stacks[i % len(stacks)]
                fallback_code = FallbackCodeGenerator.generate(
                    ctx.request.code_type.value,
                    ctx.request.description,
                    stack,
                )
                all_attempts[i] = BuildAttempt(
                    attempt_id=f"fallback_{uuid.uuid4().hex[:8]}",
                    attempt_number=attempt.attempt_number,
                    tool_stack=stack,
                    model_used="Fallback Generator",
                    code_artifact=fallback_code,
                    build_log=f"[{datetime.now().isoformat()}] FALLBACK for {attempt.model_used}\n",
                    tool_usage_report="Fallback",
                    build_time_seconds=0.5,
                    success=True,
                    error_message="",
                    timestamp=datetime.now().isoformat(),
                )
        ctx.files.all_attempts = all_attempts
        return all_attempts

    async def run_app_reviews(self, ctx: PipelineContext):
        code_reviews = await self.code_reviewer.review_all(ctx)
        product_reviews = await self.product_reviewer.review_all(ctx)
        creative_reviews = await self.creative_reviewer.review_all(ctx)
        return self._merge_reviews_triple(code_reviews, product_reviews, creative_reviews)

    def record_leaderboard(self, ctx: PipelineContext) -> None:
        winner = ctx.rankings.winner
        winning_attempt = ctx.rankings.winning_attempt
        if not winning_attempt or not winner:
            return
        total_time = time.time() - ctx.start_time
        project_name = f"{ctx.request.code_type.value}_project"

        winner_trait_vector = None
        winner_dominant = []
        winner_weak = []
        winner_builder_traits = None

        if hasattr(winner, "trait_vector") and winner.trait_vector:
            winner_trait_vector = winner.trait_vector.model_dump()
            traits = winner.trait_vector.traits
            for cat, trait in traits.items():
                if trait.score.value == 5:
                    winner_dominant.append(str(cat))
                elif trait.score.value == 1:
                    winner_weak.append(str(cat))
            if hasattr(winner.trait_vector, "builder_traits"):
                winner_builder_traits = winner.trait_vector.builder_traits.model_dump()

        app_entry = LeaderboardEntry(
            entry_id=f"app_{ctx.build_id}",
            project_name=project_name,
            code_type=ctx.request.code_type.value,
            score=winner.execution_score if winner.execution_score else winner.total_score,
            novelty_rating=round(winner.novelty_score),
            tool_stack=winning_attempt.tool_stack.name if hasattr(winning_attempt, 'tool_stack') and winning_attempt.tool_stack else 'Unknown',
            build_time_seconds=total_time,
            user_rating=None,
            created_at=datetime.now().isoformat(),
            download_path=ctx.files.zip_path,
            model_used=winning_attempt.model_used,
        )
        self.leaderboard.add_entry(
            app_entry,
            build_id=ctx.build_id,
            trait_vector=winner_trait_vector,
            dominant_traits=winner_dominant,
            weak_traits=winner_weak,
            builder_traits=winner_builder_traits,
        )

        if hasattr(winner, "display_total_score"):
            migration_logger.log_legacy_access(
                "run_pipeline",
                0,
                "display_total_score",
                "Winner display score accessed for leaderboard UI — does NOT influence selection",
            )

    def build_results_payload(self, ctx: PipelineContext, *, avg_builder_score: float) -> dict:
        winner = ctx.rankings.winner
        return {
            "status": "success",
            "build_id": ctx.build_id,
            "request": ctx.request.model_dump(),
            "factory": {
                "score": ctx.plan.factory_score,
                "review": ctx.plan.factory_review,
                "tool_combinations": [t.model_dump() for t in ctx.plan.tool_combinations],
            },
            "builders": {
                "responsible": [a.model_dump() for a in ctx.files.resp_attempts],
                "creative": [a.model_dump() for a in ctx.files.creative_attempts],
                "reviews": ctx.files.builder_reviews,
                "average_score": avg_builder_score,
            },
            "apps": {
                "all_attempts": [a.model_dump() for a in ctx.files.all_attempts],
                "reviews": [r.model_dump() for r in ctx.files.app_reviews],
                "ranked_builds": [r.model_dump() for r in ctx.rankings.ranked_builds],
                "winner": winner.model_dump(),
                "novelty_attempts": [a.model_dump() for a in ctx.files.novelty_attempts],
                "final_code": ctx.files.final_code,
                "download_path": ctx.files.zip_path,
            },
            "trait_vectors": ctx.traits.vectors,
            "total_time_seconds": time.time() - ctx.start_time,
            "errors": ctx.errors,
            "lifecycle_count": len(ctx.db.get_lifecycle(ctx.build_id)),
            "migration_info": {
                "version": "2.1_trait_vector",
                "canonical_ranking": "trait_vector_dominance",
                "display_scores": "legacy_backward_compatible",
                "legacy_access_count": len(migration_logger.legacy_accesses),
            },
        }

    def _merge_reviews_triple(self, code_reviews, product_reviews, creative_reviews):
        """Merge code quality + product quality + creative vision reviews."""
        merged = []
        for cr, pr, vr in zip(code_reviews, product_reviews, creative_reviews):
            all_dims = cr.dimensions + pr.dimensions + vr.dimensions
            overall = None  # legacy aggregate scoring removed

            # Merge all suggestions, prioritizing creative wild ideas
            all_suggestions = list(set(cr.improvement_suggestions + pr.improvement_suggestions + vr.improvement_suggestions))
            # Put creative suggestions first
            creative_suggestions = [s for s in all_suggestions if any(kw in s.lower() for kw in ['imagine', 'what if', 'wild', 'crazy', 'vision', 'alternative', 'unconventional'])]
            other_suggestions = [s for s in all_suggestions if s not in creative_suggestions]
            all_suggestions = creative_suggestions + other_suggestions

            merged.append(ReviewReport(
                attempt_id=cr.attempt_id,
                overall_score=overall,
                dimensions=all_dims,
                comparative_notes=cr.comparative_notes or pr.comparative_notes or vr.comparative_notes,
                what_works_better=cr.what_works_better or pr.what_works_better or vr.what_works_better,
                improvement_suggestions=all_suggestions,
                potential_failure_points=list(set(cr.potential_failure_points + pr.potential_failure_points + vr.potential_failure_points)),
                reviewer_model=", ".join([m for m in [cr.reviewer_model, pr.reviewer_model, vr.reviewer_model] if m]),
                timestamp=datetime.now().isoformat()
            ))
        return merged

    async def _retry_review(self, reviewer, request, attempts):
        """Retry review with an extra harshness instruction."""
        # Temporarily modify the system prompt to be harsher
        original_prompt = reviewer.SYSTEM_PROMPT
        reviewer.SYSTEM_PROMPT = original_prompt + "\n\nCRITICAL: The previous scoring was too lenient. This time, be EXTREMELY harsh. Deduct points for EVERY flaw. Most builds should score 30-60. Only exceptional work gets 70+."
        try:
            reviews = await reviewer.review_all(request, attempts)
        finally:
            reviewer.SYSTEM_PROMPT = original_prompt
        return reviews
    def _needs_correction(self, round_result):
        """Determine if round 2 is needed based on round 1 results."""
        ranked = round_result["ranked_builds"]
        if not ranked:
            return True

        scores = [r.total_score for r in ranked]

        # Trigger round 2 if:
        # 1. All scores are identical (baseline/default scoring)
        if len(set(scores)) == 1 and scores[0] > 0:
            return True

        # 2. Winner score is too low (below 60)
        if scores[0] < 60:
            return True

        # 3. Score spread is too narrow (all within 10 points)
        if max(scores) - min(scores) < 10:
            return True

        # 4. Any build failed
        successful = [a for a in round_result["build_attempts"] if a.success]
        if len(successful) < len(round_result["build_attempts"]):
            return True

        return False

    def _generate_feedback(self, reviews, ranked_builds):
        """Generate actionable feedback from round 1 for round 2 prompts."""
        feedback_parts = []

        for review, rank in zip(reviews, ranked_builds):
            # Separate issues by reviewer type
            code_issues = [d for d in review.dimensions if d.score < 75 and d.dimension in ["Code Correctness", "Tool Efficiency", "Architecture"]]
            product_issues = [d for d in review.dimensions if d.score < 75 and d.dimension in ["Novelty", "Documentation", "UX/Product"]]
            creative_issues = [d for d in review.dimensions if d.score < 75 and d.dimension in ["Vision Score", "Alternative Approaches", "Delight Potential", "Tech Creativity"]]

            # Get wild ideas from creative reviewer
            wild_ideas = [s for s in review.improvement_suggestions if any(kw in s.lower() for kw in ['imagine', 'what if', 'wild', 'crazy', 'vision', 'alternative'])]

            feedback_parts.append(f"""
## Attempt #{rank.attempt_number} (Score: {rank.total_score})
### Code Quality Issues (from Code Reviewer):
""" + ("\n".join(f"- {d.dimension}: {d.score}/100 - {d.analysis}" for d in code_issues) if code_issues else "- No major code quality issues") + f"""
### Product Quality Issues (from Product Reviewer):
""" + ("\n".join(f"- {d.dimension}: {d.score}/100 - {d.analysis}" for d in product_issues) if product_issues else "- No major product quality issues") + f"""
### Creative Vision Issues (from Creative Coder):
""" + ("\n".join(f"- {d.dimension}: {d.score}/100 - {d.analysis}" for d in creative_issues) if creative_issues else "- Vision is strong") + f"""
### Wild Ideas to Consider:
""" + ("\n".join(f"- 💡 {s}" for s in wild_ideas[:3]) if wild_ideas else "- No wild ideas suggested") + f"""
### Improvements Needed:
""" + "\n".join(f"- {s}" for s in review.improvement_suggestions[:5]))

        # Add specific corrective instructions
        feedback_parts.append("""
## CORRECTIVE INSTRUCTIONS FOR ROUND 2:
1. Fix ALL syntax errors and runtime issues (Code Quality priority)
2. Improve documentation and UX based on Product Reviewer feedback
3. Consider the Creative Coder's wild ideas - implement at least ONE surprising feature
4. Try an alternative approach or unconventional tool combination
5. Add a "wow moment" - something users won't expect
6. Ensure complete, working code - not stubs or placeholders
""")

        return "\n\n".join(feedback_parts)

    async def _build_round(self, request, build_id, tool_combinations, round_num, feedback):
        """Execute one build round (5 attempts + review + rank)."""
        base_percent = 10 if round_num == 1 else 50

        # PHASE 1: Factory Builder (only if round 1, otherwise reuse stacks)
        if tool_combinations is None:
            self._update_progress(build_id, "factory_builder", f"Round {round_num}: Factory Builder selecting tool stacks...", base_percent)
            tool_combinations = self.factory_builder.generate_tool_combinations(request.code_type.value, request.preferred_frameworks)

        # PHASE 2: App Builder
        self._update_progress(build_id, "creative_builder", f"Round {round_num}: App Builder constructing code...", base_percent + 5)

        # Round 1: Responsible Builder (production-ready)
        # Round 2: Creative Builder (novel/experimental, with feedback)
        if round_num == 1:
            self._update_progress(build_id, "responsible_builder", f"Round {round_num}: Responsible Builder constructing deployable code...", base_percent + 5)
            build_tasks = [self.responsible_builder.build(request, stack, i+1) for i, stack in enumerate(tool_combinations)]
        elif feedback and round_num == 2:
            self._update_progress(build_id, "creative_builder", f"Round {round_num}: Creative Builder with feedback...", base_percent + 5)
            build_tasks = [self.creative_builder.build_with_feedback(request, stack, i+1, feedback) for i, stack in enumerate(tool_combinations)]
        else:
            build_tasks = [self.creative_builder.build(request, stack, i+1) for i, stack in enumerate(tool_combinations)]

        build_attempts = await asyncio.gather(*build_tasks)

        # Check for failures and use fallback if needed
        successful = [a for a in build_attempts if a.success]
        if not successful:
            fallback_attempts = []
            for i, stack in enumerate(tool_combinations):
                fallback_code = FallbackCodeGenerator.generate(request.code_type.value, request.description, stack)
                attempt = BuildAttempt(
                    attempt_id=f"build_{uuid.uuid4().hex[:8]}",
                    attempt_number=i+1, tool_stack=stack, model_used="Fallback Generator",
                    code_artifact=fallback_code,
                    build_log=f"[{datetime.now().isoformat()}] FALLBACK: Round {round_num} template for {stack.name}\n",
                    tool_usage_report=f"Fallback template for {request.code_type.value}",
                    build_time_seconds=0.5, success=True, error_message="",
                    timestamp=datetime.now().isoformat()
                )
                fallback_attempts.append(attempt)
            build_attempts = fallback_attempts
            successful = fallback_attempts

        # PHASE 3: TRIPLE REVIEWERS (Code + Product + Creative)
        self._update_progress(build_id, "reviewer", f"Round {round_num}: Code Quality Reviewer analyzing...", base_percent + 10)
        code_reviews = await self.code_reviewer.review_all(request, build_attempts)

        self._update_progress(build_id, "reviewer", f"Round {round_num}: Product Quality Reviewer analyzing...", base_percent + 13)
        product_reviews = await self.product_reviewer.review_all(request, build_attempts)

        self._update_progress(build_id, "reviewer", f"Round {round_num}: Creative Coder reviewing vision...", base_percent + 16)
        creative_reviews = await self.creative_reviewer.review_all(request, build_attempts)

        # Merge all three reviews
        reviews = self._merge_reviews_triple(code_reviews, product_reviews, creative_reviews)

        # VALIDATE: Check if scores are meaningful (not all defaulting to 70)
        valid, msg = ScoreValidator.validate_reviews(reviews)
        if not valid:
            print(f"[SCORE WARNING] Round {round_num}: {msg}")
            # Retry with stronger prompt if scores look defaulted
            self._update_progress(build_id, "reviewer", f"Round {round_num}: Retrying with stricter scoring...", base_percent + 16)
            # Re-run reviewers with an extra "BE HARSH" instruction appended
            code_reviews = await self._retry_review(self.code_reviewer, request, build_attempts)
            product_reviews = await self._retry_review(self.product_reviewer, request, build_attempts)
            reviews = self._merge_reviews(code_reviews, product_reviews)

        # PHASE 4: CANONICAL TRAIT-VECTOR RANKING
        self._update_progress(build_id, "ranker", f"Round {round_num}: Trait-Vector Ranker scoring...", base_percent + 22)

        # CANONICAL: Use TraitVectorRanker for selection/evolution
        try:
            canonical_ranked = await self.primary_ranker.rank_all(build_attempts, reviews)
        except Exception as rank_err:
            self._update_progress(build_id, "ranker", f"Trait-vector ranker failed, using fallback...", base_percent + 24)
            canonical_ranked = await self.fallback_ranker.rank_all(build_attempts, reviews)

        # LEGACY: Also run old ranker for backward-compatible display scores
        # This does NOT influence selection — it's for the leaderboard UI only
        try:
            legacy_ranked = await self.legacy_ranker.rank_all(build_attempts, reviews)
            # Merge: canonical ranking order + legacy display scores
            legacy_score_map = {r.attempt_id: r for r in legacy_ranked}
        except Exception:
            legacy_score_map = {}

        # Build final ranked list: canonical order, with legacy scores for display
        ranked_builds = []
        for cr in canonical_ranked:
            # Add legacy display scores if available
            if cr.attempt_id in legacy_score_map:
                lr = legacy_score_map[cr.attempt_id]
                cr.display_total_score = lr.total_score
                cr.display_functionality_score = getattr(lr, 'functionality_score', 0)
                cr.display_code_quality_score = getattr(lr, 'code_quality_score', 0)
                cr.display_tool_optimization_score = getattr(lr, 'tool_optimization_score', 0)
                cr.display_novelty_score = getattr(lr, 'novelty_score', 0)
                cr.display_documentation_score = getattr(lr, 'documentation_score', 0)
            ranked_builds.append(cr)

        # Log that legacy scores are being used for display only
        if legacy_score_map:
            migration_logger.log_legacy_access(
                "run_pipeline", 0, "legacy_ranker.total_score",
                "Display-only legacy scores merged into canonical trait-vector ranking"
            )

        return {
            "tool_combinations": tool_combinations,
            "build_attempts": build_attempts,
            "reviews": reviews,
            "ranked_builds": ranked_builds
        }
    def _checkpoint(
        self,
        build_id: str,
        ckpt: dict,
        stage: PipelineState,
        **data: Any,
    ) -> None:
        ckpt.update(data)
        self._fsm(build_id).stage_checkpoint(stage, **data)

    def _load_checkpoint(self, build_id: str) -> dict:
        ckpt = self.db.get_checkpoint(build_id)
        ckpt.pop("_resume_phase", None)
        return ckpt

    @staticmethod
    def _stacks_from_checkpoint(data: list) -> list:
        return [ToolStack(**t) for t in data]

    @staticmethod
    def _attempts_from_checkpoint(data: list) -> list:
        return [BuildAttempt(**a) for a in data]

    @staticmethod
    def _ranked_from_checkpoint(data: list) -> list:
        ranked = []
        for item in data:
            if isinstance(item, dict) and item.get("trait_vector"):
                ranked.append(CanonicalRankedBuild(**item))
            elif isinstance(item, dict):
                ranked.append(RankedBuild(**item))
            else:
                ranked.append(item)
        return ranked

    async def resume_pipeline(self, build_id: str):
        """Resume an interrupted build from the last persisted checkpoint."""
        build = self.db.get_build(build_id)
        if not build:
            return {"status": "failed", "error": "Build not found"}
        progress = self.db.get_progress(build_id)
        if not progress.get("can_resume"):
            return {
                "status": "failed",
                "error": f"Build cannot resume (pipeline_status={progress.get('pipeline_status')})",
            }
        from core.build_queue import dispatch_build

        task_id = dispatch_build(build_id)
        return {
            "status": "queued",
            "build_id": build_id,
            "task_id": task_id,
            "message": "Resume dispatched via recovery pipeline",
        }

    def agent_registry(self):
        from agents.registry import build_agent_registry

        return build_agent_registry(self)

    async def run_pipeline(self, request: BuildRequest, build_id=None, resume: bool = False):
        """Execute the factory pipeline through a single PipelineContext (sync/dev path)."""
        from core.pipeline_runner import PipelineRunner
        build_id = build_id or f"pipeline_{uuid.uuid4().hex[:8]}"
        ctx = self.create_context(request, build_id, resume=resume)
        return await PipelineRunner(ctx, agents=self.agent_registry()).execute()

    def _update_progress(self, build_id, phase, message, percent, status=None):
        """Sub-round progress (evolution rounds) — routed through the FSM."""
        pipe = self._fsm(build_id)
        if status == "failed":
            pipe.fail(message, phase=phase)
            return
        target = PipelineState(status) if status else state_for_phase(phase)
        if pipe.current_state == target:
            pipe.phase(phase, message, percent)
        else:
            pipe.enter(target, phase, message, percent)

    def get_progress(self, build_id):
        return self.db.get_progress(build_id)

    def get_results(self, build_id):
        return self.db.get_results(build_id)


