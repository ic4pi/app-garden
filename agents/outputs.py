"""Independent structured outputs per agent role."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Optional


@dataclass
class PlannerOutput:
    tool_combinations_count: int = 0
    factory_score: float = 0.0
    factory_review: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class BuilderOutput:
    resp_count: int = 0
    creative_count: int = 0
    all_count: int = 0
    validated_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ReviewerOutput:
    builder_reviews_count: int = 0
    app_reviews_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RankerOutput:
    ranked_count: int = 0
    winner_attempt_id: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RepairOutput:
    fallback_repairs: int = 0
    validation_repairs: int = 0
    strategies: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class NoveltyOutput:
    novelty_attempts_count: int = 0
    successful_count: int = 0
    final_code_length: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class LeaderboardOutput:
    leaderboard_updated: bool = False
    winner_attempt_id: Optional[str] = None
    winner_score: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
