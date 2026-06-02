# Worker Queue System (Celery + Redis)

App Garden runs pipeline stages on separate Celery worker pools so multiple builds can execute concurrently without sharing the API process event loop.

## Queues

| Queue | Stage | Agent |
|-------|--------|--------|
| `planner` | planning | Planner |
| `builder` | generating (build) | Builder |
| `repair` | validation / fallbacks | Repair |
| `reviewer` | reviewing | Reviewer |
| `ranker` | ranking, novelty, packaging, leaderboard, finalize | Ranker + tail stages |

## Local development

1. Start Redis: `docker run -d -p 6379:6379 redis:7-alpine`
2. Install deps: `pip install -r requirements.txt`
3. Start API: `uvicorn app:app --reload`
4. Start workers: `bash scripts/run_workers.sh`

Environment:

```bash
export USE_CELERY=true
export CELERY_BROKER_URL=redis://127.0.0.1:6379/0
```

Set `USE_CELERY=false` to run the full pipeline in-process (background task on the API event loop) without Redis.

## Reliability (Step 9+)

- **Build/stage locks**: SQLite table `pipeline_stage_runs` — only one `running` lock per `(build_id, stage)` until TTL expires.
- **Idempotency**: Stages are done when required checkpoint keys exist (`core/stage_execution.py`); completed rows must match artifacts.
- **Auto-resume on startup**: `worker.tasks.startup_auto_resume` (Celery) or API `kernel_startup` when inline — scans interrupted/stale builds and re-dispatches.
- **Stuck recovery loop**: Celery Beat runs `worker.tasks.recover_stuck_builds` every `RECOVERY_INTERVAL_SECONDS` (default 300).

### Environment

| Variable | Default | Meaning |
|----------|---------|---------|
| `STAGE_LOCK_TTL_SECONDS` | 3600 | Lock expiry for crashed workers |
| `STUCK_BUILD_THRESHOLD_MINUTES` | 30 | No progress → eligible for recovery |
| `RECOVERY_INTERVAL_SECONDS` | 300 | Beat interval for stuck sweep |
| `AUTO_RESUME_ON_STARTUP` | true | Scan DB on worker/API boot |

**Redis deleted?** Jobs remain in SQLite; startup recovery + beat re-enqueue missing stages. In-flight Celery messages are lost until recovery runs.

## Docker Compose

```bash
docker compose up --build
```

This starts Redis, the API, and one service per worker pool.

## API

- `POST /api/build` — creates build row, enqueues `planning`
- `POST /api/builds/{id}/resume` — re-enqueues from last checkpoint
- `POST /api/builds/{id}/resume-sync` — legacy blocking in-process resume
