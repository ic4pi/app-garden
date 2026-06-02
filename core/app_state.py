"""Application identity and lifecycle: load on boot, save on shutdown."""

from __future__ import annotations

import atexit
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from core.config import (
    CANONICAL_SETTINGS_MAP,
    SETTINGS_PATH,
    SETTINGS_VERSION,
    Config,
    _canonical_ui_key,
    is_masked_value,
    is_secret_key,
    mask_secrets,
    secrets_configured,
)

logger = logging.getLogger("app_garden.kernel")

_atexit_registered = False


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class AppState:
    """Single source of truth for persisted settings and boot identity."""

    boot_id: str = ""
    started_at: str = ""
    last_saved_at: str = ""
    settings: dict[str, Any] = {}
    loaded_from_disk: bool = False

    @classmethod
    def reset_runtime_identity(cls) -> None:
        cls.boot_id = uuid.uuid4().hex[:12]
        cls.started_at = _utc_now()

    @classmethod
    def _normalize_envelope(cls, raw: dict[str, Any]) -> dict[str, Any]:
        """Accept envelope or legacy flat dict; return canonical settings only."""
        if "settings" in raw and isinstance(raw.get("settings"), dict):
            stored = raw["settings"]
        else:
            stored = raw
        canonical: dict[str, Any] = {}
        for key, value in stored.items():
            if key in ("version", "boot_id", "updated_at"):
                continue
            ui_key = _canonical_ui_key(key)
            if ui_key not in CANONICAL_SETTINGS_MAP:
                continue
            canonical[ui_key] = value
        return canonical

    @classmethod
    def load_settings_file(cls) -> dict[str, Any]:
        if not SETTINGS_PATH.exists():
            return {}
        try:
            raw = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                return {"settings": cls._normalize_envelope(raw)}
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Could not read %s: %s", SETTINGS_PATH, exc)
        return {}

    @classmethod
    def save_settings_file(cls) -> None:
        cls.sync_from_config()
        payload = {
            "version": SETTINGS_VERSION,
            "boot_id": cls.boot_id,
            "updated_at": _utc_now(),
            "settings": cls.settings,
        }
        SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = SETTINGS_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        tmp.replace(SETTINGS_PATH)
        cls.last_saved_at = payload["updated_at"]
        logger.info("Settings persisted to %s", SETTINGS_PATH)

    @classmethod
    def apply_settings(cls, incoming: dict[str, Any]) -> dict[str, Any]:
        """Merge incoming UI settings; preserve secrets when client sends placeholders."""
        merged = dict(cls.settings)
        for key, value in incoming.items():
            canonical = _canonical_ui_key(key)
            if is_secret_key(canonical) and is_masked_value(value):
                continue
            if value is None:
                continue
            if isinstance(value, str) and not value.strip() and is_secret_key(canonical):
                merged.pop(canonical, None)
                continue
            merged[canonical] = value
        Config.update_from_settings(merged)
        cls.sync_from_config()
        return cls.settings

    @classmethod
    def sync_from_config(cls) -> None:
        cls.settings = Config.to_settings_dict()

    @classmethod
    def public_settings(cls) -> dict[str, Any]:
        return mask_secrets(cls.settings)

    @classmethod
    def public_view(cls) -> dict[str, Any]:
        """Masked settings plus configured flags for the settings UI."""
        return {
            **cls.public_settings(),
            "secrets_configured": secrets_configured(cls.settings),
        }

    @classmethod
    def identity(cls) -> dict[str, Any]:
        return {
            "boot_id": cls.boot_id,
            "started_at": cls.started_at,
            "last_saved_at": cls.last_saved_at,
            "settings_path": str(SETTINGS_PATH),
            "loaded_from_disk": cls.loaded_from_disk,
            "settings_version": SETTINGS_VERSION,
        }


def kernel_startup() -> None:
    """Load persisted settings and apply to Config (same state after every restart)."""
    from core.database import get_database

    get_database().init_db()
    _maybe_auto_resume_on_api_boot()
    AppState.reset_runtime_identity()
    envelope = AppState.load_settings_file()
    stored = envelope.get("settings", {}) if envelope else {}
    if stored:
        AppState.loaded_from_disk = True
        AppState.settings = dict(stored)
        Config.update_from_settings(stored)
        logger.info(
            "Kernel startup: restored %d settings from %s (boot_id=%s)",
            len(stored),
            SETTINGS_PATH,
            AppState.boot_id,
        )
    else:
        AppState.settings = Config.to_settings_dict()
        logger.info("Kernel startup: no settings file; using env defaults (boot_id=%s)", AppState.boot_id)
    _register_shutdown_hook()


def kernel_shutdown() -> None:
    """Persist current Config back to settings.json."""
    AppState.sync_from_config()
    AppState.save_settings_file()
    logger.info("Kernel shutdown complete (boot_id=%s)", AppState.boot_id)


def _maybe_auto_resume_on_api_boot() -> None:
    """When API runs without Celery workers, still recover interrupted builds once."""
    from core.config import Config

    if Config.USE_CELERY:
        return
    try:
        from core.stage_coordinator import auto_resume_on_startup, try_startup_recovery_lock

        if try_startup_recovery_lock():
            auto_resume_on_startup()
    except Exception as exc:
        logger.warning("Inline auto-resume skipped: %s", exc)


def _register_shutdown_hook() -> None:
    global _atexit_registered
    if _atexit_registered:
        return
    atexit.register(kernel_shutdown)
    _atexit_registered = True
