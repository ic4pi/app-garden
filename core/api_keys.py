"""Simple API key store and rotation helpers.

This stores keys in data/api_keys.json and exposes helpers to get/rotate
keys for providers like 'openrouter' and 'nvidia'. Designed to be lightweight
and filesystem-backed for local development.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional
import threading

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
KEYS_PATH = DATA_DIR / "api_keys.json"
_lock = threading.Lock()


def _ensure_file() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not KEYS_PATH.exists():
        KEYS_PATH.write_text(json.dumps({"nvidia": [], "openrouter": [], "openrouter_paid": []}, indent=2))


def read_keys() -> Dict[str, List[str]]:
    _ensure_file()
    try:
        return json.loads(KEYS_PATH.read_text())
    except Exception:
        return {"nvidia": [], "openrouter": [], "openrouter_paid": []}


def write_keys(data: Dict[str, List[str]]) -> None:
    _ensure_file()
    with _lock:
        KEYS_PATH.write_text(json.dumps(data, indent=2))


def add_key(provider: str, key: str, paid: bool = False) -> None:
    data = read_keys()
    bucket = provider.lower()
    if bucket not in data:
        data[bucket] = []
    if paid and bucket == "openrouter":
        # ensure paid list exists
        data.setdefault("openrouter_paid", [])
        if key not in data["openrouter_paid"]:
            data["openrouter_paid"].append(key)
    else:
        if key not in data[bucket]:
            data[bucket].append(key)
    write_keys(data)


def remove_key(provider: str, key: str) -> None:
    data = read_keys()
    bucket = provider.lower()
    for k in (bucket, f"{bucket}_paid"):
        if k in data and key in data[k]:
            data[k].remove(key)
    write_keys(data)


def get_all() -> Dict[str, List[str]]:
    return read_keys()


def get_next_key(provider: str, paid_fallback: bool = False) -> Optional[str]:
    """Return next available key for provider. If none and paid_fallback True,
    try openrouter_paid bucket for paid key.
    """
    data = read_keys()
    bucket = provider.lower()
    keys = data.get(bucket, [])
    if keys:
        # simple rotation: pop first and append to end
        key = keys.pop(0)
        keys.append(key)
        data[bucket] = keys
        write_keys(data)
        return key
    if paid_fallback and bucket == "openrouter":
        paid = data.get("openrouter_paid", [])
        if paid:
            k = paid.pop(0)
            paid.append(k)
            data["openrouter_paid"] = paid
            write_keys(data)
            return k
    return None
