#!/usr/bin/env bash
# Bootstrap + start script for App Garden.
# Handles: venv creation, dependency install, Redis check, and app launch.
set -euo pipefail
cd "$(dirname "$0")/.."

# ── Venv ──────────────────────────────────────────────────────────────────────
if [ ! -d "venv" ]; then
  echo "No venv found — creating one..."
  python3 -m venv venv
fi

source venv/bin/activate

# ── Dependencies ──────────────────────────────────────────────────────────────
# Quick check: if pydantic is missing, re-install everything.
if ! python3 -c "import pydantic" 2>/dev/null; then
  echo "Dependencies missing — installing from requirements.txt..."
  pip install -q -r requirements.txt
fi

# ── Environment ──────────────────────────────────────────────────────────────
export PYTHONPATH="${PYTHONPATH:+$PYTHONPATH:}$(pwd)"
export CELERY_BROKER_URL="${CELERY_BROKER_URL:-redis://127.0.0.1:6379/0}"
export CELERY_RESULT_BACKEND="${CELERY_RESULT_BACKEND:-redis://127.0.0.1:6379/1}"
export USE_CELERY="${USE_CELERY:-true}"

# ── Redis check ──────────────────────────────────────────────────────────────
if ! python3 -c "import redis; r=redis.Redis(); r.ping()" 2>/dev/null; then
  echo "WARNING: Redis not reachable at ${CELERY_BROKER_URL}"
  echo "  Start it with: docker run -d -p 6379:6379 redis:7-alpine"
  echo "  Or: sudo systemctl start redis"
  echo ""
  echo "Starting in NO-CELERY mode (tasks run inline)..."
  export USE_CELERY="false"
  python3 app.py
  exit $?
fi

# ── Launch ───────────────────────────────────────────────────────────────────
MODE="${1:-full}"

if [ "$MODE" = "api" ]; then
  echo "Starting API server only..."
  python3 app.py
elif [ "$MODE" = "workers" ]; then
  echo "Starting Celery workers..."
  exec bash scripts/run_workers.sh
else
  echo "Starting API server + Celery workers..."
  bash scripts/run_workers.sh &
  python3 app.py
fi
