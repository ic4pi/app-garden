#!/usr/bin/env bash
# Start all Celery worker pools (run from repo root with Redis available).
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH="${PYTHONPATH:+$PYTHONPATH:}$(pwd)"

export CELERY_BROKER_URL="${CELERY_BROKER_URL:-redis://127.0.0.1:6379/0}"
export CELERY_RESULT_BACKEND="${CELERY_RESULT_BACKEND:-redis://127.0.0.1:6379/1}"
export USE_CELERY="${USE_CELERY:-true}"

if ! command -v celery >/dev/null 2>&1; then
  echo "celery not found; pip install -r requirements.txt"
  exit 1
fi

echo "Broker: $CELERY_BROKER_URL"
echo "Starting planner, builder, repair, reviewer, ranker workers..."

celery -A worker.celery_app worker -Q planner -c 2 -n planner@%h --loglevel=info &
celery -A worker.celery_app worker -Q builder -c 4 -n builder@%h --loglevel=info &
celery -A worker.celery_app worker -Q repair -c 2 -n repair@%h --loglevel=info &
celery -A worker.celery_app worker -Q reviewer -c 2 -n reviewer@%h --loglevel=info &
celery -A worker.celery_app worker -Q ranker -c 2 -n ranker@%h --loglevel=info &

wait
