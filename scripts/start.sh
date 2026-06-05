#!/usr/bin/env bash
# Bootstrap + start script for App Garden.
# Handles: venv creation, dependency install, Redis check, and app launch.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"

# ── Venv ──────────────────────────────────────────────────────────────────────
if [ ! -d "venv" ]; then
  echo ">> Creating venv..."
  python3 -m venv venv
fi

# Always activate (safe to call if already active)
source venv/bin/activate

# ── Dependencies ──────────────────────────────────────────────────────────────
# Quick spot-check: if pydantic is missing, assume a fresh venv and reinstall.
if ! python3 -c "import pydantic" 2>/dev/null; then
  echo ">> Installing dependencies from requirements.txt..."
  pip install -q -r requirements.txt
  echo ">> Done. Verifying critical packages..."
  python3 -c "import pydantic, httpx, fastapi, celery; print('  All good.')"
fi

# ── Environment ──────────────────────────────────────────────────────────────
export PYTHONPATH="${PYTHONPATH:+$PYTHONPATH:}${ROOT}"
export CELERY_BROKER_URL="${CELERY_BROKER_URL:-redis://127.0.0.1:6379/0}"
export CELERY_RESULT_BACKEND="${CELERY_RESULT_BACKEND:-redis://127.0.0.1:6379/1}"
export USE_CELERY="${USE_CELERY:-true}"

# ── Redis check ──────────────────────────────────────────────────────────────
if python3 -c "import redis as r; r.Redis.from_url('${CELERY_BROKER_URL}').ping()" 2>/dev/null; then
  REDIS_OK=true
else
  REDIS_OK=false
  echo ">> WARNING: Redis not reachable at ${CELERY_BROKER_URL}"
  echo "   Start it with:  docker run -d -p 6379:6379 redis:7-alpine"
  echo "   Or:             sudo systemctl start redis"
  echo ""
fi

# ── Launch ───────────────────────────────────────────────────────────────────
MODE="${1:-full}"

if [ "$MODE" = "api" ]; then
  echo ">> Starting API server only..."
  python3 app.py
elif [ "$MODE" = "workers" ]; then
  if [ "$REDIS_OK" = "false" ]; then
    echo "ERROR: Celery workers require Redis. Aborting."
    exit 1
  fi
  echo ">> Starting Celery workers..."
  exec bash scripts/run_workers.sh
else
  if [ "$REDIS_OK" = "true" ]; then
    echo ">> Starting API server + Celery workers..."
    bash scripts/run_workers.sh &
    python3 app.py
  else
    echo ">> Starting API server in NO-CELERY mode (tasks run inline)..."
    USE_CELERY=false python3 app.py
  fi
fi
