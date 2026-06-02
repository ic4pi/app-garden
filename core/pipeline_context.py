"""PipelineContext — single source of truth passed through every stage."""

from __future__ import annotations

import time
import traceback
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Optional

from core.database import AppDatabase
from core.pipeline_fsm import PipelineState, PipelineStateMachine
from core.pipeline_stages import PipelineStage

if TYPE_CHECKING:
    from core.pipeline_store import PipelineServices


@dataclass
class PipelinePlan:
    """Factory / strategy phase artifacts (synced with ctx.plan dict)."""

    tool_combinations: list[Any] = field(default_factory=list)
    factory_review: dict[str, Any] = field(default_factory=dict)
    factory_score: float = 70.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "tool_combinations": [
                t.model_dump() if hasattr(t, "model_dump") else t
                for t in self.tool_combinations
            ],
            "factory_review": self.factory_review,
            "factory_score": self.factory_score,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PipelinePlan":
        return cls(
            tool_combinations=data.get("tool_combinations", []),
            factory_review=data.get("factory_review", {}),
            factory_score=float(data.get("factory_score", 70.0)),
        )


@dataclass
class PipelineWorkspaces:
    by_attempt: dict[str, str] = field(default_factory=dict)

    def record(self, attempt_id: str, path: str) -> None:
        if path:
            self.by_attempt[attempt_id] = path


@dataclass
class PipelineFiles:
    resp_attempts: list[Any] = field(default_factory=list)
    creative_attempts: list[Any] = field(default_factory=list)
    all_attempts: list[Any] = field(default_factory=list)
    builder_reviews: list[Any] = field(default_factory=list)
    app_reviews: list[Any] = field(default_factory=list)
    novelty_attempts: list[Any] = field(default_factory=list)
    final_code: str = ""
    zip_path: str = ""


@dataclass
class PipelineReviews:
    """All review artifacts (builder + app)."""

    builder: list[Any] = field(default_factory=list)
    app: list[Any] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "builder": self.builder,
            "app": [
                r.model_dump() if hasattr(r, "model_dump") else r for r in self.app
            ],
        }

    def sync_from_files(self, files: PipelineFiles) -> None:
        self.builder = list(files.builder_reviews)
        self.app = list(files.app_reviews)

    def sync_to_files(self, files: PipelineFiles) -> None:
        files.builder_reviews = list(self.builder)
        files.app_reviews = list(self.app)


@dataclass
class PipelineTraits:
    vectors: list[dict[str, Any]] = field(default_factory=list)
    dominant_by_attempt: dict[str, list[str]] = field(default_factory=dict)
    weak_by_attempt: dict[str, list[str]] = field(default_factory=dict)


@dataclass
class PipelineRankings:
    ranked_builds: list[Any] = field(default_factory=list)
    winner: Optional[Any] = None
    winning_attempt: Optional[Any] = None


@dataclass
class PipelineValidation:
    by_attempt: dict[str, dict[str, Any]] = field(default_factory=dict)
    all_passed: bool = False
    final_code_passed: bool = False

    def hydrate(self, data: dict[str, Any]) -> None:
        self.by_attempt = data.get("by_attempt", self.by_attempt)
        self.all_passed = bool(data.get("all_passed", self.all_passed))
        self.final_code_passed = bool(data.get("final_code_passed", self.final_code_passed))

    def attempt_passed(self, attempt_id: str) -> bool:
        return bool(self.by_attempt.get(attempt_id, {}).get("passed"))


class PipelineContext:
    """
    Canonical pipeline state object. Stages must read/write ONLY through this type.

    Persisted fields: prompt, plan, files, reviews, checkpoints (via DB).
    Ephemeral per invocation: stage (set at stage entry).
    """

    def __init__(
        self,
        *,
        build_id: str,
        db: AppDatabase,
        fsm: PipelineStateMachine,
        services: "PipelineServices",
        stage: str = "",
        prompt: Optional[dict[str, Any]] = None,
        resume: bool = False,
        download_manager: Any = None,
        leaderboard: Any = None,
        stack_factory: Optional[Callable[[list], list]] = None,
        attempt_factory: Optional[Callable[[list], list]] = None,
        ranked_factory: Optional[Callable[[list], list]] = None,
        review_factory: Optional[Callable[[list], list]] = None,
        novelty_factory: Optional[Callable[[list], list]] = None,
        request: Any = None,
    ) -> None:
        self.build_id = build_id
        self.db = db
        self.fsm = fsm
        self.services = services
        self.stage = stage

        self.prompt: dict[str, Any] = dict(prompt or {})
        self.checkpoints: dict[str, Any] = {}
        self.logs: list[dict[str, Any]] = []
        self.errors: list[dict[str, Any]] = []

        self._request = request
        self.resume = resume

        self._plan = PipelinePlan()
        self._files = PipelineFiles()
        self._reviews = PipelineReviews()
        self.traits = PipelineTraits()
        self.rankings = PipelineRankings()
        self.validation = PipelineValidation()
        self.workspaces = PipelineWorkspaces()

        self.download_manager = download_manager or services.download_manager
        self.leaderboard = leaderboard or services.leaderboard
        self._stack_factory = stack_factory or services.stack_factory
        self._attempt_factory = attempt_factory or services.attempt_factory
        self._ranked_factory = ranked_factory or services.ranked_factory
        self._review_factory = review_factory or services.review_factory
        self._novelty_factory = novelty_factory or services.novelty_factory

        self.start_time = time.time()
        self._agent_results: list[Any] = []

        if resume:
            self.hydrate_from_db()

    @property
    def request(self) -> Any:
        """Build request object from prompt dict (read-only view)."""
        if self._request is not None:
            return self._request
        from core.models import BuildRequest

        self._request = BuildRequest(**self.prompt)
        return self._request

    @property
    def pipe(self) -> PipelineStateMachine:
        return self.fsm

    @property
    def checkpoint(self) -> dict[str, Any]:
        """Alias for checkpoints (backward compatible)."""
        return self.checkpoints

    def set_stage(self, stage: PipelineStage | str) -> None:
        self.stage = stage.value if isinstance(stage, PipelineStage) else stage
        self.checkpoints["current_stage"] = self.stage

    def log(self, stage: str, message: str, *, level: str = "info") -> None:
        entry = {"stage": stage, "message": message, "level": level}
        self.logs.append(entry)
        self.db.append_log(self.build_id, stage, message, level=level)

    def record_error(
        self,
        message: str,
        *,
        phase: str = "",
        exc: Optional[BaseException] = None,
    ) -> None:
        detail = traceback.format_exc() if exc else ""
        entry = {
            "phase": phase,
            "message": message,
            "detail": detail[:2000] if detail else "",
        }
        self.errors.append(entry)
        self.log(phase or "error", message, level="error")

    def has_ckpt(self, key: str) -> bool:
        return bool(self.checkpoints.get(key))

    def hydrate_from_db(self) -> None:
        raw = self.db.get_checkpoint(self.build_id)
        raw.pop("_resume_phase", None)
        self.checkpoints = dict(raw)
        self.stage = self.checkpoints.get("current_stage", self.stage)
        stored_logs = self.db.get_logs(self.build_id, limit=500)
        self.logs = list(stored_logs)
        self._apply_checkpoints_to_sections()

    def _apply_checkpoints_to_sections(self) -> None:
        c = self.checkpoints
        if "request" in c:
            self.prompt.update(c["request"] if isinstance(c["request"], dict) else {})
        if "start_time" in c:
            self.start_time = c["start_time"]
        if "tool_combinations" in c:
            self._plan.tool_combinations = self._deserialize_stacks(c["tool_combinations"])
        if "factory_review" in c:
            self._plan.factory_review = c["factory_review"]
        self._plan.factory_score = c.get("factory_score", self._plan.factory_score)
        if "resp_attempts" in c:
            self._files.resp_attempts = self._deserialize_attempts(c["resp_attempts"])
        if "creative_attempts" in c:
            self._files.creative_attempts = self._deserialize_attempts(c["creative_attempts"])
        if "all_attempts" in c:
            self._files.all_attempts = self._deserialize_attempts(c["all_attempts"])
        if "builder_reviews" in c:
            self._files.builder_reviews = c["builder_reviews"]
        if "app_reviews" in c:
            self._files.app_reviews = self._deserialize_reviews(c["app_reviews"])
        if "ranked_builds" in c:
            self.rankings.ranked_builds = self._deserialize_ranked(c["ranked_builds"])
            self.resolve_winner()
        if "novelty_attempts" in c:
            self._files.novelty_attempts = self._deserialize_novelty(c["novelty_attempts"])
        self._files.final_code = c.get("final_code", self._files.final_code)
        self._files.zip_path = c.get("zip_path", self._files.zip_path)
        if "validation" in c:
            self.validation.hydrate(c["validation"])
        self._reviews.sync_from_files(self._files)

    def _apply_ckpt_to_sections(self) -> None:
        """Backward-compatible alias."""
        self._apply_checkpoints_to_sections()

    def _deserialize_stacks(self, data: list) -> list:
        if self._stack_factory:
            return self._stack_factory(data)
        return data

    def _deserialize_attempts(self, data: list) -> list:
        if self._attempt_factory:
            return self._attempt_factory(data)
        return data

    def _deserialize_ranked(self, data: list) -> list:
        if self._ranked_factory:
            return self._ranked_factory(data)
        return data

    def _deserialize_reviews(self, data: list) -> list:
        if self._review_factory:
            return self._review_factory(data)
        return data

    def _deserialize_novelty(self, data: list) -> list:
        if self._novelty_factory:
            return self._novelty_factory(data)
        return data

    def save_checkpoint(self, stage: PipelineState, **extra: Any) -> None:
        """Merge into checkpoints and persist to DB (only write path for artifacts)."""
        self._reviews.sync_to_files(self._files)
        payload = {**extra}
        if self._plan.tool_combinations:
            payload.setdefault("tool_combinations", self._plan.to_dict()["tool_combinations"])
        if self._plan.factory_review:
            payload.setdefault("factory_review", self._plan.factory_review)
        payload.setdefault("factory_score", self._plan.factory_score)
        self.checkpoints.update(payload)
        self.checkpoints["current_stage"] = self.stage
        self.fsm.stage_checkpoint(stage, **payload)

    def to_state_dict(self) -> dict[str, Any]:
        """Serializable snapshot for debugging/API."""
        self._reviews.sync_to_files(self._files)
        return {
            "build_id": self.build_id,
            "stage": self.stage,
            "prompt": self.prompt,
            "plan": self._plan.to_dict(),
            "files": {
                "all_attempts_count": len(self._files.all_attempts),
                "final_code_len": len(self._files.final_code or ""),
                "zip_path": self._files.zip_path,
            },
            "reviews": self._reviews.to_dict(),
            "checkpoints_keys": sorted(self.checkpoints.keys()),
            "errors": self.errors,
            "logs_count": len(self.logs),
        }

    def fail(self, message: str, *, exc: Optional[BaseException] = None) -> None:
        self.record_error(message, phase="failed", exc=exc)
        detail = traceback.format_exc() if exc else message
        self.fsm.fail(message, traceback_text=detail)

    def resolve_winner(self) -> bool:
        if not self.rankings.ranked_builds:
            return False
        self.rankings.winner = self.rankings.ranked_builds[0]
        self.rankings.winning_attempt = next(
            (
                a
                for a in self._files.all_attempts
                if a.attempt_id == self.rankings.winner.attempt_id
            ),
            None,
        )
        return self.rankings.winning_attempt is not None

    # ── Typed section accessors (agents read/write these → checkpoints) ─────

    @property
    def plan(self) -> PipelinePlan:
        return self._plan

    @property
    def files(self) -> PipelineFiles:
        return self._files

    @property
    def reviews(self) -> PipelineReviews:
        return self._reviews

    @property
    def plan_json(self) -> dict[str, Any]:
        return self._plan.to_dict()

    @property
    def reviews_json(self) -> dict[str, Any]:
        return self._reviews.to_dict()
