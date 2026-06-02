"""Frozen configuration kernel: env defaults, settings mapping, secret masking."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# ── Paths (rules of existence) ───────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.getenv("APP_GARDEN_DATA_DIR", str(PROJECT_ROOT / "data")))
SETTINGS_PATH = DATA_DIR / "settings.json"
SETTINGS_VERSION = 1

MASK_PLACEHOLDER = "••••••••••••••••"

_SECRET_KEY_RE = re.compile(
    r"(api[_-]?key|_key$|^key$|secret|token|password|credential)",
    re.IGNORECASE,
)

# UI / persistence key → Config attribute
SETTINGS_TO_CONFIG: dict[str, str] = {
    "nvidia_api_key": "NVIDIA_API_KEY",
    "nvidia_key": "NVIDIA_API_KEY",
    "openrouter_api_key": "OPENROUTER_API_KEY",
    "openrouter_key": "OPENROUTER_API_KEY",
    "ollama_host": "OLLAMA_HOST",
    "ollama_cloud_host": "OLLAMA_CLOUD_HOST",
    "responsible_provider": "RESPONSIBLE_PROVIDER",
    "responsible_model": "RESPONSIBLE_MODEL",
    "creative_provider": "CREATIVE_PROVIDER",
    "creative_model": "CREATIVE_MODEL",
    "codequality_provider": "CODEQUALITY_PROVIDER",
    "codequality_model": "CODEQUALITY_MODEL",
    "product_provider": "PRODUCT_PROVIDER",
    "product_model": "PRODUCT_MODEL",
    "creativecoder_provider": "CREATIVECODER_PROVIDER",
    "creativecoder_model": "CREATIVECODER_MODEL",
    "factory_provider": "FACTORY_PROVIDER",
    "factory_model": "FACTORY_MODEL",
    "builderreviewer_provider": "BUILDERREVIEWER_PROVIDER",
    "builderreviewer_model": "BUILDERREVIEWER_MODEL",
    "primaryranker_provider": "PRIMARYRANKER_PROVIDER",
    "primaryranker_model": "PRIMARYRANKER_MODEL",
    "fallbackranker_provider": "FALLBACKRANKER_PROVIDER",
    "fallbackranker_model": "FALLBACKRANKER_MODEL",
    "minimax_model": "MINIMAX_MODEL",
    "novelty_model": "NOVELTY_MODEL",
    "builder_model": "RESPONSIBLE_MODEL",
    "reviewer_model": "CODEQUALITY_MODEL",
    "ranker_model": "PRIMARYRANKER_MODEL",
    "fallback_ranker": "FALLBACKRANKER_MODEL",
}

# Canonical UI keys returned by the public config API (settings page)
PUBLIC_UI_KEYS: tuple[str, ...] = (
    "nvidia_api_key",
    "openrouter_api_key",
    "ollama_host",
    "ollama_cloud_host",
    "responsible_provider",
    "responsible_model",
    "creative_provider",
    "creative_model",
    "codequality_provider",
    "codequality_model",
    "product_provider",
    "product_model",
    "creativecoder_provider",
    "creativecoder_model",
    "factory_provider",
    "factory_model",
    "builderreviewer_provider",
    "builderreviewer_model",
    "primaryranker_provider",
    "primaryranker_model",
    "fallbackranker_provider",
    "fallbackranker_model",
    "minimax_model",
    "novelty_model",
)


def is_secret_key(key: str) -> bool:
    return bool(_SECRET_KEY_RE.search(key.replace("-", "_")))


def is_masked_value(value: Any) -> bool:
    if value is None:
        return True
    if not isinstance(value, str):
        return False
    text = value.strip()
    if not text:
        return True
    if text == MASK_PLACEHOLDER:
        return True
    if set(text) <= {"•", "*", "·"}:
        return True
    return False


def mask_secret(value: str) -> str:
    if not value or not str(value).strip():
        return ""
    return MASK_PLACEHOLDER


def mask_secrets(data: dict[str, Any]) -> dict[str, Any]:
    """Return a copy safe for logs and HTTP responses."""
    out: dict[str, Any] = {}
    for key, value in data.items():
        if is_secret_key(key):
            out[key] = mask_secret(str(value)) if value else ""
        else:
            out[key] = value
    return out


def redact_for_log(data: dict[str, Any]) -> dict[str, Any]:
    """Alias for mask_secrets — use before logging any settings-shaped dict."""
    return mask_secrets(data)


def _canonical_ui_key(key: str) -> str:
    aliases = {
        "nvidia_key": "nvidia_api_key",
        "openrouter_key": "openrouter_api_key",
        "builder_model": "responsible_model",
        "reviewer_model": "codequality_model",
        "ranker_model": "primaryranker_model",
        "fallback_ranker": "fallbackranker_model",
    }
    return aliases.get(key, key)


# One config attribute per canonical UI key (no legacy duplicates on disk)
CANONICAL_SETTINGS_MAP: dict[str, str] = {}
for _ui_key, _attr in SETTINGS_TO_CONFIG.items():
    _canonical = _canonical_ui_key(_ui_key)
    if _canonical not in CANONICAL_SETTINGS_MAP:
        CANONICAL_SETTINGS_MAP[_canonical] = _attr


def secrets_configured(data: dict[str, Any]) -> dict[str, bool]:
    """Whether each secret UI key has a non-empty stored value (no raw material)."""
    return {
        key: bool(str(data.get(key, "") or "").strip())
        for key in CANONICAL_SETTINGS_MAP
        if is_secret_key(key)
    }


class Config:
    """Runtime configuration. Env provides defaults; persisted settings override."""

    NVIDIA_API_KEY = os.getenv("NVIDIA_API_KEY", "")
    OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
    OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
    OLLAMA_CLOUD_HOST = os.getenv("OLLAMA_CLOUD_HOST", "")

    RESPONSIBLE_PROVIDER = os.getenv("RESPONSIBLE_PROVIDER", "openrouter")
    RESPONSIBLE_MODEL = os.getenv("RESPONSIBLE_MODEL", "nvidia/nemotron-3-super-120b-a12b")
    CREATIVE_PROVIDER = os.getenv("CREATIVE_PROVIDER", "openrouter")
    CREATIVE_MODEL = os.getenv("CREATIVE_MODEL", "nvidia/nemotron-3-super-120b-a12b")

    CODEQUALITY_PROVIDER = os.getenv("CODEQUALITY_PROVIDER", "openrouter")
    CODEQUALITY_MODEL = os.getenv("CODEQUALITY_MODEL", "nvidia/nemotron-3-super-120b-a12b")
    PRODUCT_PROVIDER = os.getenv("PRODUCT_PROVIDER", "openrouter")
    PRODUCT_MODEL = os.getenv("PRODUCT_MODEL", "nvidia/nemotron-3-super-120b-a12b")
    CREATIVECODER_PROVIDER = os.getenv("CREATIVECODER_PROVIDER", "openrouter")
    CREATIVECODER_MODEL = os.getenv("CREATIVECODER_MODEL", "nvidia/nemotron-3-super-120b-a12b")
    FACTORY_PROVIDER = os.getenv("FACTORY_PROVIDER", "openrouter")
    FACTORY_MODEL = os.getenv("FACTORY_MODEL", "nvidia/nemotron-3-super-120b-a12b")
    BUILDERREVIEWER_PROVIDER = os.getenv("BUILDERREVIEWER_PROVIDER", "openrouter")
    BUILDERREVIEWER_MODEL = os.getenv("BUILDERREVIEWER_MODEL", "nvidia/nemotron-3-super-120b-a12b")

    PRIMARYRANKER_PROVIDER = os.getenv("PRIMARYRANKER_PROVIDER", "openrouter")
    PRIMARYRANKER_MODEL = os.getenv("PRIMARYRANKER_MODEL", "meta/llama-4-maverick-17b-128e-instruct:free")
    FALLBACKRANKER_PROVIDER = os.getenv("FALLBACKRANKER_PROVIDER", "openrouter")
    FALLBACKRANKER_MODEL = os.getenv("FALLBACKRANKER_MODEL", "deepseek/deepseek-r1:free")

    MINIMAX_MODEL = os.getenv("MINIMAX_MODEL", "minimax/minimax-m2.5")
    NOVELTY_MODEL = os.getenv("NOVELTY_MODEL", "nvidia/nemotron-3-super-120b-a12b")

    NVIDIA_BUILDER_MODEL = RESPONSIBLE_MODEL
    NVIDIA_REVIEWER_MODEL = CODEQUALITY_MODEL
    RANKER_MODEL = PRIMARYRANKER_MODEL
    FALLBACK_RANKER = FALLBACKRANKER_MODEL

    NVIDIA_API_URL = os.getenv("NVIDIA_API_URL", "https://api.nvidia.com/v1/chat/completions")
    OPENROUTER_API_URL = os.getenv("OPENROUTER_API_URL", "https://openrouter.ai/api/v1/chat/completions")
    OPENROUTER_RATE_LIMIT = int(os.getenv("OPENROUTER_RATE_LIMIT", "20"))
    NVIDIA_RATE_LIMIT = int(os.getenv("NVIDIA_RATE_LIMIT", "100"))
    OPENROUTER_RETRY_COUNT = int(os.getenv("OPENROUTER_RETRY_COUNT", "5"))
    OPENROUTER_RETRY_BACKOFF = float(os.getenv("OPENROUTER_RETRY_BACKOFF", "2.0"))
    REQUEST_TIMEOUT = 120
    DB_PATH = Path(os.getenv("DB_PATH", str(DATA_DIR / "gardener.db")))
    OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./outputs"))
    WORKSPACES_DIR = Path(os.getenv("WORKSPACES_DIR", str(DATA_DIR / "workspaces")))
    ENABLE_BUILDER_TOOLS = os.getenv("ENABLE_BUILDER_TOOLS", "true").lower() in (
        "1",
        "true",
        "yes",
    )
    STATIC_DIR = Path(os.getenv("STATIC_DIR", "./static"))

    # Celery / Redis worker queue
    CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://127.0.0.1:6379/0")
    CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", CELERY_BROKER_URL)
    USE_CELERY = os.getenv("USE_CELERY", "true").lower() in ("1", "true", "yes")
    STAGE_LOCK_TTL_SECONDS = int(os.getenv("STAGE_LOCK_TTL_SECONDS", "3600"))
    STAGE_LOCK_RETRY_SECONDS = int(os.getenv("STAGE_LOCK_RETRY_SECONDS", "45"))
    STAGE_MAX_ATTEMPTS = int(os.getenv("STAGE_MAX_ATTEMPTS", "3"))
    STUCK_BUILD_THRESHOLD_MINUTES = int(os.getenv("STUCK_BUILD_THRESHOLD_MINUTES", "30"))
    RECOVERY_INTERVAL_SECONDS = int(os.getenv("RECOVERY_INTERVAL_SECONDS", "300"))
    RECOVERY_BATCH_LIMIT = int(os.getenv("RECOVERY_BATCH_LIMIT", "50"))
    AUTO_RESUME_ON_STARTUP = os.getenv("AUTO_RESUME_ON_STARTUP", "true").lower() in (
        "1",
        "true",
        "yes",
    )
    STARTUP_RECOVERY_LOCK_SECONDS = int(os.getenv("STARTUP_RECOVERY_LOCK_SECONDS", "120"))

    @classmethod
    def update_from_settings(cls, settings: dict[str, Any]) -> None:
        canonical_values: dict[str, Any] = {}
        for ui_key, value in settings.items():
            canonical_values[_canonical_ui_key(ui_key)] = value
        for ui_key, config_key in CANONICAL_SETTINGS_MAP.items():
            if ui_key not in canonical_values:
                continue
            value = canonical_values[ui_key]
            if is_secret_key(ui_key) and is_masked_value(value):
                continue
            if value is None or (
                isinstance(value, str) and not value.strip() and is_secret_key(ui_key)
            ):
                continue
            if value or not is_secret_key(ui_key):
                setattr(cls, config_key, value)
        cls._sync_legacy_aliases()

    @classmethod
    def _sync_legacy_aliases(cls) -> None:
        cls.NVIDIA_BUILDER_MODEL = cls.RESPONSIBLE_MODEL
        cls.NVIDIA_REVIEWER_MODEL = cls.CODEQUALITY_MODEL
        cls.RANKER_MODEL = cls.PRIMARYRANKER_MODEL
        cls.FALLBACK_RANKER = cls.FALLBACKRANKER_MODEL

    @classmethod
    def to_settings_dict(cls) -> dict[str, str]:
        """Export current config as canonical UI settings (raw secrets for persistence)."""
        return {
            ui_key: str(getattr(cls, attr, "") or "")
            for ui_key, attr in CANONICAL_SETTINGS_MAP.items()
        }

    @classmethod
    def to_public_dict(cls) -> dict[str, Any]:
        return mask_secrets(cls.to_settings_dict())


DATA_DIR.mkdir(parents=True, exist_ok=True)
Config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
Config.WORKSPACES_DIR.mkdir(parents=True, exist_ok=True)
Config.STATIC_DIR.mkdir(parents=True, exist_ok=True)
