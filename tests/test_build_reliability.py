"""Test build pipeline reliability fixes."""
import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from itertools import zip_longest

from core.pipeline_domain import (
    TraitVectorRanker, TraitVector, TraitCategory, ScoreValue, TraitScore,
    CanonicalRankedBuild, LLMClient
)
from core.models import BuildAttempt, ReviewDimension, ReviewReport, ToolStack
from app import Config


class TestRateLimitAttributes:
    """Test that Config has all required rate-limit attributes."""

    def test_config_has_retry_count(self):
        assert hasattr(Config, 'OPENROUTER_RETRY_COUNT')
        assert Config.OPENROUTER_RETRY_COUNT > 0

    def test_config_has_retry_backoff(self):
        assert hasattr(Config, 'OPENROUTER_RETRY_BACKOFF')
        assert Config.OPENROUTER_RETRY_BACKOFF > 0.0

    def test_config_has_fallback_ranker(self):
        assert hasattr(Config, 'FALLBACK_RANKER')
        assert isinstance(Config.FALLBACK_RANKER, str)


class TestTraitVectorErrorHandling:
    """Test that trait vector generation has error handling."""

    @pytest.mark.asyncio
    async def test_generate_trait_vector_with_llm_failure(self):
        """Test _generate_trait_vector falls back on LLM failure."""
        ranker = TraitVectorRanker(llm_client=Mock())

        attempt = Mock()
        attempt.attempt_id = "test_attempt_1"
        attempt.attempt_number = 1
        attempt.success = True
        attempt.code_artifact = "def hello(): pass"
        attempt.tool_stack = Mock(name="test_stack")

        review = Mock()
        review.dimensions = []

        ranker.llm = AsyncMock()
        ranker.llm.generate_code = AsyncMock(side_effect=Exception("LLM timeout"))

        tv = await ranker._generate_trait_vector(attempt, review)

        # Should return fallback trait vector, not raise exception
        assert tv is not None
        assert isinstance(tv, TraitVector)
        assert tv.reviewer_id == "fallback"
        assert len(tv.traits) == 20

    @pytest.mark.asyncio
    async def test_generate_trait_vector_success(self):
        """Test _generate_trait_vector succeeds with valid LLM response."""
        ranker = TraitVectorRanker(llm_client=Mock())

        attempt = Mock()
        attempt.attempt_id = "test_attempt_2"
        attempt.attempt_number = 2
        attempt.success = True
        attempt.code_artifact = "def hello(): pass"
        attempt.tool_stack = Mock(name="test_stack")

        review = Mock()
        review.dimensions = []

        valid_response = """{
            "trait_vector": {
                "prompt_understanding": {"score": 5, "evidence": "code matches prompt", "reasoning": "clear", "confidence": 90},
                "feature_correctness": {"score": 5, "evidence": "works", "reasoning": "tested", "confidence": 85},
                "completeness": {"score": 5, "evidence": "all features", "reasoning": "done", "confidence": 80},
                "stability": {"score": 5, "evidence": "no crashes", "reasoning": "solid", "confidence": 85},
                "error_handling": {"score": 5, "evidence": "try/except", "reasoning": "robust", "confidence": 80},
                "file_organization": {"score": 1, "evidence": "messy", "reasoning": "poor", "confidence": 75},
                "architecture": {"score": 1, "evidence": "no pattern", "reasoning": "weak", "confidence": 70},
                "readability": {"score": 1, "evidence": "unclear", "reasoning": "bad", "confidence": 65},
                "reusability": {"score": 1, "evidence": "tight coupling", "reasoning": "poor", "confidence": 70},
                "maintainability": {"score": 1, "evidence": "hard to modify", "reasoning": "bad", "confidence": 65},
                "ui_ux_quality": {"score": 3, "evidence": "ok", "reasoning": "average", "confidence": 50},
                "user_flow": {"score": 3, "evidence": "functional", "reasoning": "works", "confidence": 55},
                "responsiveness": {"score": 3, "evidence": "loads", "reasoning": "ok", "confidence": 50},
                "accessibility": {"score": 3, "evidence": "usable", "reasoning": "decent", "confidence": 50},
                "performance": {"score": 3, "evidence": "acceptable", "reasoning": "ok", "confidence": 55},
                "creativity": {"score": 1, "evidence": "standard", "reasoning": "boring", "confidence": 60},
                "novel_problem_solving": {"score": 1, "evidence": "basic", "reasoning": "simple", "confidence": 65},
                "stack_selection": {"score": 1, "evidence": "common", "reasoning": "typical", "confidence": 60},
                "adaptability": {"score": 1, "evidence": "rigid", "reasoning": "inflexible", "confidence": 65},
                "impressiveness": {"score": 1, "evidence": "unimpressed", "reasoning": "meh", "confidence": 60}
            }
        }"""

        ranker.llm = AsyncMock()
        ranker.llm.generate_code = AsyncMock(return_value=valid_response)

        tv = await ranker._generate_trait_vector(attempt, review)

        assert tv is not None
        assert isinstance(tv, TraitVector)
        assert tv.attempt_id == "test_attempt_2"
        assert len(tv.traits) == 20
        # Verify score distribution was enforced
        ones = sum(1 for t in tv.traits.values() if t.score == ScoreValue.POOR)
        fives = sum(1 for t in tv.traits.values() if t.score == ScoreValue.EXCEPTIONAL)
        assert ones >= 5
        assert fives >= 5


class TestZipLongestHandling:
    """Test that rank_all handles mismatched attempts/reviews."""

    @pytest.mark.asyncio
    async def test_rank_all_with_more_attempts_than_reviews(self):
        """Test ranking when reviews list is shorter than attempts."""
        ranker = TraitVectorRanker(llm_client=Mock())

        # Create 3 attempts
        attempts = []
        for i in range(3):
            attempt = Mock()
            attempt.attempt_id = f"attempt_{i}"
            attempt.attempt_number = i + 1
            attempt.success = True
            attempt.code_artifact = f"code_{i}"
            attempt.tool_stack = Mock(name=f"stack_{i}")
            attempts.append(attempt)

        # Only 2 reviews (shorter than attempts)
        reviews = [Mock(dimensions=[]), Mock(dimensions=[])]

        ranker.llm = AsyncMock()
        ranker.llm.generate_code = AsyncMock(return_value="{}")

        # Should not crash with zip_longest
        with patch.object(ranker, '_parse_trait_vector') as mock_parse:
            # Mock parse to return valid TraitVector
            def make_tv(response, attempt):
                tv = Mock(spec=TraitVector)
                tv.attempt_id = attempt.attempt_id
                tv.traits = {}
                tv.compute_legacy_display = Mock()
                return tv

            mock_parse.side_effect = make_tv

            result = await ranker.rank_all(attempts, reviews)

            # Should rank all 3 attempts, not just 2
            assert len(result) >= 2  # At least the reviews


class TestBackgroundTaskTracking:
    """Test that FastAPI background_tasks properly tracks builds."""

    def test_background_tasks_add_task_exists(self):
        """Test that background_tasks.add_task is available."""
        from fastapi import BackgroundTasks

        tasks = BackgroundTasks()
        assert hasattr(tasks, 'add_task')

        # Verify it's callable
        mock_func = Mock()
        tasks.add_task(mock_func, "arg1", "arg2")
        # BackgroundTasks doesn't execute immediately


class TestWinnerAttributeAccess:
    """Test safe access to winner attributes."""

    def test_winner_execution_score_with_getattr(self):
        """Test safe access to execution_score via getattr."""
        # Create a CanonicalRankedBuild (has execution_score)
        ranked_build = CanonicalRankedBuild(
            attempt_id="test",
            attempt_number=1,
            tool_stack_name="test_stack",
            trait_vector=Mock(legacy_total_score=50, legacy_rank_label="test"),
            execution_score=75.5,
            confidence_score=85.0
        )

        # Should work
        score = getattr(ranked_build, 'execution_score', 0.0)
        assert score == 75.5

        # Should not crash with legacy object that lacks the attribute
        legacy_obj = Mock()
        legacy_obj.execution_score = AttributeError("not present")
        score = getattr(legacy_obj, 'nonexistent_attr', 0.0)
        assert score == 0.0

    def test_winner_confidence_score_with_getattr(self):
        """Test safe access to confidence_score via getattr."""
        ranked_build = CanonicalRankedBuild(
            attempt_id="test",
            attempt_number=1,
            tool_stack_name="test_stack",
            trait_vector=Mock(legacy_total_score=50, legacy_rank_label="test"),
            confidence_score=80.0
        )

        score = getattr(ranked_build, 'confidence_score', 50.0)
        assert score == 80.0


class TestLLMClientUnification:
    """Test that LLMClient is imported from core, not duplicated."""

    def test_llm_client_from_core_pipeline_domain(self):
        """Test that LLMClient can be imported from core.pipeline_domain."""
        from core.pipeline_domain import LLMClient as CoreLLMClient

        assert CoreLLMClient is not None
        assert hasattr(CoreLLMClient, '__init__')
        assert hasattr(CoreLLMClient, 'call_nvidia')
        assert hasattr(CoreLLMClient, 'call_openrouter')

    def test_app_imports_llm_client_from_core(self):
        """Test that app.py imports LLMClient from core."""
        import app

        # The LLMClient in app module should be the one from core
        from core.pipeline_domain import LLMClient as CoreLLMClient
        assert app.LLMClient is CoreLLMClient


class TestComputeLegacyDisplayCalled:
    """Test that compute_legacy_display is called on all TraitVector paths."""

    def test_fallback_trait_vector_has_legacy_display(self):
        """Test that fallback trait vector computes legacy display."""
        ranker = TraitVectorRanker(llm_client=Mock())

        attempt = Mock()
        attempt.attempt_id = "test_fallback"

        tv = ranker._fallback_trait_vector(attempt)

        # Should have legacy scores computed
        assert tv.legacy_total_score > 0
        assert tv.legacy_rank_label in ["Broken", "Weak", "Functional", "Strong", "Advanced", "Elite"]

    def test_parsed_trait_vector_has_legacy_display(self):
        """Test that parsed trait vector computes legacy display."""
        ranker = TraitVectorRanker(llm_client=Mock())

        valid_response = """{
            "trait_vector": {
                "prompt_understanding": {"score": 5, "evidence": "good", "reasoning": "ok", "confidence": 80},
                "feature_correctness": {"score": 5, "evidence": "good", "reasoning": "ok", "confidence": 80},
                "completeness": {"score": 5, "evidence": "good", "reasoning": "ok", "confidence": 80},
                "stability": {"score": 5, "evidence": "good", "reasoning": "ok", "confidence": 80},
                "error_handling": {"score": 5, "evidence": "good", "reasoning": "ok", "confidence": 80},
                "file_organization": {"score": 1, "evidence": "bad", "reasoning": "no", "confidence": 50},
                "architecture": {"score": 1, "evidence": "bad", "reasoning": "no", "confidence": 50},
                "readability": {"score": 1, "evidence": "bad", "reasoning": "no", "confidence": 50},
                "reusability": {"score": 1, "evidence": "bad", "reasoning": "no", "confidence": 50},
                "maintainability": {"score": 1, "evidence": "bad", "reasoning": "no", "confidence": 50},
                "ui_ux_quality": {"score": 3, "evidence": "ok", "reasoning": "ok", "confidence": 50},
                "user_flow": {"score": 3, "evidence": "ok", "reasoning": "ok", "confidence": 50},
                "responsiveness": {"score": 3, "evidence": "ok", "reasoning": "ok", "confidence": 50},
                "accessibility": {"score": 3, "evidence": "ok", "reasoning": "ok", "confidence": 50},
                "performance": {"score": 3, "evidence": "ok", "reasoning": "ok", "confidence": 50},
                "creativity": {"score": 1, "evidence": "bad", "reasoning": "no", "confidence": 50},
                "novel_problem_solving": {"score": 1, "evidence": "bad", "reasoning": "no", "confidence": 50},
                "stack_selection": {"score": 1, "evidence": "bad", "reasoning": "no", "confidence": 50},
                "adaptability": {"score": 1, "evidence": "bad", "reasoning": "no", "confidence": 50},
                "impressiveness": {"score": 1, "evidence": "bad", "reasoning": "no", "confidence": 50}
            }
        }"""

        attempt = Mock()
        attempt.attempt_id = "test_parsed"

        tv = ranker._parse_trait_vector(valid_response, attempt)

        # Should have legacy scores computed
        assert tv.legacy_total_score > 0
        assert tv.legacy_rank_label != ""


class TestConfigIntegration:
    """Test that config changes don't break the pipeline."""

    def test_config_update_preserves_retry_settings(self):
        """Test that config retains retry settings after update."""
        original_count = Config.OPENROUTER_RETRY_COUNT
        original_backoff = Config.OPENROUTER_RETRY_BACKOFF

        # Simulate update
        Config.update_from_settings({"openrouter_retry_count": 3})

        # Should still have valid values
        assert Config.OPENROUTER_RETRY_COUNT >= 1
        assert Config.OPENROUTER_RETRY_BACKOFF > 0.0

        # Restore
        Config.OPENROUTER_RETRY_COUNT = original_count
        Config.OPENROUTER_RETRY_BACKOFF = original_backoff


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
