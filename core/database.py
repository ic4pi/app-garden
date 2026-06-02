"""SQLite persistence: builds, logs, leaderboard, pipeline state."""

from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

from core.config import DATA_DIR, Config

logger = logging.getLogger("app_garden.database")

SCHEMA_VERSION = 3

PIPELINE_STATUSES = (
    "queued",
    "planning",
    "generating",
    "reviewing",
    "ranking",
    "packaging",
    "complete",
    "failed",
    "interrupted",
)

PHASE_TO_STATUS = {
    "factory_builder": "planning",
    "factory_review": "planning",
    "builders": "generating",
    "responsible_builder": "generating",
    "creative_builder": "generating",
    "builder_review": "generating",
    "app_review": "reviewing",
    "reviewer": "reviewing",
    "ranker": "ranking",
    "novelty": "ranking",
    "packaging": "packaging",
    "leaderboard": "packaging",
    "complete": "complete",
    "failed": "failed",
}

RESUMABLE_STATUSES = frozenset(
    {"queued", "planning", "generating", "reviewing", "ranking", "packaging", "interrupted"}
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_dumps(value: Any) -> str:
    return json.dumps(value, default=str)


def _json_loads(text: Optional[str], default: Any = None) -> Any:
    if not text:
        return default
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return default


class AppDatabase:
    """Single SQLite store for builds, pipeline state, logs, and leaderboard."""

    def __init__(self, db_path: Optional[str | Path] = None) -> None:
        raw = db_path or Config.DB_PATH
        self.db_path = Path(raw)
        if not self.db_path.is_absolute():
            self.db_path = DATA_DIR / self.db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS schema_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS builds (
                    id TEXT PRIMARY KEY,
                    code_type TEXT NOT NULL,
                    description TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    created_at TEXT NOT NULL,
                    completed_at TEXT,
                    output_path TEXT,
                    request_json TEXT NOT NULL,
                    results_json TEXT,
                    error_text TEXT
                );

                CREATE TABLE IF NOT EXISTS pipeline_state (
                    build_id TEXT PRIMARY KEY REFERENCES builds(id) ON DELETE CASCADE,
                    pipeline_status TEXT NOT NULL DEFAULT 'queued',
                    phase TEXT NOT NULL DEFAULT '',
                    message TEXT NOT NULL DEFAULT '',
                    percent REAL NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL,
                    checkpoint_json TEXT NOT NULL DEFAULT '{}',
                    resume_phase TEXT
                );

                CREATE TABLE IF NOT EXISTS pipeline_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    build_id TEXT NOT NULL REFERENCES builds(id) ON DELETE CASCADE,
                    timestamp TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    level TEXT NOT NULL DEFAULT 'info',
                    message TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS leaderboard_entries (
                    entry_id TEXT PRIMARY KEY,
                    build_id TEXT,
                    project_name TEXT NOT NULL,
                    code_type TEXT NOT NULL,
                    score REAL NOT NULL,
                    novelty_rating INTEGER NOT NULL,
                    tool_stack TEXT NOT NULL,
                    build_time_seconds REAL NOT NULL,
                    user_rating INTEGER,
                    created_at TEXT NOT NULL,
                    download_path TEXT,
                    model_used TEXT,
                    trait_vector TEXT,
                    trait_vector_version TEXT DEFAULT '2.1',
                    dominant_traits TEXT,
                    weak_traits TEXT,
                    builder_traits TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_builds_status ON builds(status);
                CREATE INDEX IF NOT EXISTS idx_builds_created ON builds(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_logs_build ON pipeline_logs(build_id, id);
                CREATE INDEX IF NOT EXISTS idx_leaderboard_created ON leaderboard_entries(created_at);
                CREATE INDEX IF NOT EXISTS idx_leaderboard_score ON leaderboard_entries(score DESC);

                CREATE TABLE IF NOT EXISTS pipeline_transitions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    build_id TEXT NOT NULL REFERENCES builds(id) ON DELETE CASCADE,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    phase TEXT NOT NULL DEFAULT '',
                    event TEXT NOT NULL DEFAULT 'transition',
                    message TEXT NOT NULL DEFAULT '',
                    percent REAL,
                    checkpoint_keys TEXT,
                    error_text TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_transitions_build
                    ON pipeline_transitions(build_id, id);

                CREATE TABLE IF NOT EXISTS pipeline_stage_runs (
                    build_id TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    worker_id TEXT,
                    lock_token TEXT,
                    started_at TEXT,
                    completed_at TEXT,
                    expires_at TEXT,
                    attempt INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    PRIMARY KEY (build_id, stage),
                    FOREIGN KEY (build_id) REFERENCES builds(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_stage_runs_status
                    ON pipeline_stage_runs(status, expires_at);

                CREATE TABLE IF NOT EXISTS cluster_locks (
                    name TEXT PRIMARY KEY,
                    holder TEXT NOT NULL,
                    expires_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS pipeline_dispatch (
                    build_id TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    task_id TEXT,
                    created_at TEXT NOT NULL,
                    consumed_at TEXT,
                    PRIMARY KEY (build_id, stage)
                );
                """
            )
            conn.execute(
                "INSERT OR REPLACE INTO schema_meta (key, value) VALUES (?, ?)",
                ("version", str(SCHEMA_VERSION)),
            )
            self._migrate_legacy_leaderboard(conn)
            self._ensure_stage_tables(conn)
            self.mark_stale_running_builds(conn)

        logger.info("Database initialized at %s", self.db_path)

    def _ensure_stage_tables(self, conn: sqlite3.Connection) -> None:
        """Idempotent schema for stage locks (existing DBs upgrading to v3)."""
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS pipeline_stage_runs (
                build_id TEXT NOT NULL,
                stage TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                worker_id TEXT,
                lock_token TEXT,
                started_at TEXT,
                completed_at TEXT,
                expires_at TEXT,
                attempt INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                PRIMARY KEY (build_id, stage),
                FOREIGN KEY (build_id) REFERENCES builds(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_stage_runs_status
                ON pipeline_stage_runs(status, expires_at);
            CREATE TABLE IF NOT EXISTS cluster_locks (
                name TEXT PRIMARY KEY,
                holder TEXT NOT NULL,
                expires_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS pipeline_dispatch (
                build_id TEXT NOT NULL,
                stage TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                task_id TEXT,
                created_at TEXT NOT NULL,
                consumed_at TEXT,
                PRIMARY KEY (build_id, stage)
            );
            """
        )

    def _migrate_legacy_leaderboard(self, conn: sqlite3.Connection) -> None:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        if "leaderboard" not in tables:
            return
        count = conn.execute("SELECT COUNT(*) FROM leaderboard_entries").fetchone()[0]
        if count > 0:
            return
        conn.execute(
            """
            INSERT OR IGNORE INTO leaderboard_entries (
                entry_id, build_id, project_name, code_type, score, novelty_rating,
                tool_stack, build_time_seconds, user_rating, created_at, download_path,
                model_used, trait_vector, trait_vector_version, dominant_traits,
                weak_traits, builder_traits
            )
            SELECT
                entry_id, NULL, project_name, code_type, score, novelty_rating,
                tool_stack, build_time_seconds, user_rating, created_at, download_path,
                model_used, trait_vector, trait_vector_version, dominant_traits,
                weak_traits, builder_traits
            FROM leaderboard
            """
        )
        logger.info("Migrated legacy leaderboard rows into leaderboard_entries")

    def mark_stale_running_builds(self, conn: Optional[sqlite3.Connection] = None) -> int:
        """Mark in-flight builds as interrupted after a server restart."""
        active = tuple(s for s in PIPELINE_STATUSES if s not in ("complete", "failed", "interrupted"))
        placeholders = ",".join("?" * len(active))

        def _run(c: sqlite3.Connection) -> int:
            now = _utc_now()
            stale = c.execute(
                f"""
                SELECT build_id, pipeline_status, phase, message, percent
                FROM pipeline_state
                WHERE pipeline_status IN ({placeholders})
                """,
                active,
            ).fetchall()
            cur = c.execute(
                f"""
                UPDATE builds SET status = 'interrupted'
                WHERE status IN ({placeholders})
                """,
                active,
            )
            c.execute(
                f"""
                UPDATE pipeline_state
                SET pipeline_status = 'interrupted',
                    message = message || ' [server restarted — resume manually]',
                    updated_at = ?
                WHERE pipeline_status IN ({placeholders})
                """,
                (now, *active),
            )
            for row in stale:
                c.execute(
                    """
                    INSERT INTO pipeline_transitions (
                        build_id, from_status, to_status, phase, event,
                        message, percent, created_at
                    ) VALUES (?, ?, 'interrupted', ?, 'interrupt', ?, ?, ?)
                    """,
                    (
                        row["build_id"],
                        row["pipeline_status"],
                        row["phase"] or "",
                        (row["message"] or "") + " [server restarted]",
                        row["percent"],
                        now,
                    ),
                )
            return cur.rowcount

        if conn is not None:
            return _run(conn)
        with self._connect() as c:
            return _run(c)

    # ── Builds ───────────────────────────────────────────────────────────────

    def create_build(self, build_id: str, request: dict[str, Any]) -> None:
        code_type = request.get("code_type", "website")
        description = request.get("description", "")
        now = _utc_now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO builds (
                    id, code_type, description, status, created_at, request_json
                ) VALUES (?, ?, ?, 'queued', ?, ?)
                """,
                (build_id, code_type, description, now, _json_dumps(request)),
            )
            conn.execute(
                """
                INSERT INTO pipeline_state (
                    build_id, pipeline_status, phase, message, percent, updated_at
                ) VALUES (?, 'queued', '', 'Build queued', 0, ?)
                """,
                (build_id, now),
            )
        self.append_log(build_id, "queued", "Build created", level="info")
        self.record_transition(
            build_id,
            from_status=None,
            to_status="queued",
            phase="queued",
            event="create",
            message="Build queued",
            percent=0.0,
        )

    def update_build(
        self,
        build_id: str,
        *,
        status: Optional[str] = None,
        output_path: Optional[str] = None,
        results: Optional[dict[str, Any]] = None,
        error: Optional[str] = None,
        completed: bool = False,
    ) -> None:
        sets: list[str] = []
        params: list[Any] = []
        if status is not None:
            sets.append("status = ?")
            params.append(status)
        if output_path is not None:
            sets.append("output_path = ?")
            params.append(output_path)
        if results is not None:
            sets.append("results_json = ?")
            params.append(_json_dumps(results))
        if error is not None:
            sets.append("error_text = ?")
            params.append(error)
        if completed:
            sets.append("completed_at = ?")
            params.append(_utc_now())
        if not sets:
            return
        params.append(build_id)
        with self._connect() as conn:
            conn.execute(f"UPDATE builds SET {', '.join(sets)} WHERE id = ?", params)

    def get_build(self, build_id: str) -> Optional[dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM builds WHERE id = ?", (build_id,)).fetchone()
        if not row:
            return None
        return self._row_to_build(row)

    def list_builds(
        self,
        *,
        status: Optional[str] = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        query = "SELECT * FROM builds"
        params: list[Any] = []
        if status:
            query += " WHERE status = ?"
            params.append(status)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_build(r) for r in rows]

    def _row_to_build(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "code_type": row["code_type"],
            "description": row["description"],
            "status": row["status"],
            "created_at": row["created_at"],
            "completed_at": row["completed_at"],
            "output_path": row["output_path"],
            "request": _json_loads(row["request_json"], {}),
            "results": _json_loads(row["results_json"]),
            "error": row["error_text"],
        }

    def save_results(self, build_id: str, results: dict[str, Any]) -> None:
        status = results.get("status", "complete")
        output = None
        apps = results.get("apps") or {}
        if isinstance(apps, dict):
            output = apps.get("download_path")
        self.update_build(
            build_id,
            status="complete" if status == "success" else "failed",
            results=results,
            output_path=output,
            completed=True,
        )

    def get_results(self, build_id: str) -> Optional[dict[str, Any]]:
        build = self.get_build(build_id)
        if not build:
            return None
        return build.get("results")

    # ── Pipeline state, transitions & progress ─────────────────────────────────

    def record_transition(
        self,
        build_id: str,
        *,
        from_status: Optional[str],
        to_status: str,
        phase: str = "",
        event: str = "transition",
        message: str = "",
        percent: Optional[float] = None,
        checkpoint_keys: Optional[list[str]] = None,
        error_text: Optional[str] = None,
    ) -> int:
        """Append an auditable row to the pipeline lifecycle log."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO pipeline_transitions (
                    build_id, from_status, to_status, phase, event, message,
                    percent, checkpoint_keys, error_text, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    build_id,
                    from_status,
                    to_status,
                    phase,
                    event,
                    message,
                    percent,
                    _json_dumps(checkpoint_keys) if checkpoint_keys else None,
                    error_text,
                    _utc_now(),
                ),
            )
            return int(cur.lastrowid)

    def get_lifecycle(self, build_id: str) -> list[dict[str, Any]]:
        """Ordered transition history for a build."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, from_status, to_status, phase, event, message,
                       percent, checkpoint_keys, error_text, created_at
                FROM pipeline_transitions
                WHERE build_id = ?
                ORDER BY id ASC
                """,
                (build_id,),
            ).fetchall()
        return [
            {
                "id": row["id"],
                "from_status": row["from_status"],
                "to_status": row["to_status"],
                "phase": row["phase"],
                "event": row["event"],
                "message": row["message"],
                "percent": row["percent"],
                "checkpoint_keys": _json_loads(row["checkpoint_keys"], []),
                "error": row["error_text"],
                "timestamp": row["created_at"],
            }
            for row in rows
        ]

    def apply_pipeline_state(
        self,
        build_id: str,
        *,
        pipeline_status: str,
        phase: str,
        message: str,
        percent: float,
    ) -> None:
        """Update current pipeline_state row (called by FSM after validated transition)."""
        status = (
            pipeline_status.value
            if hasattr(pipeline_status, "value")
            else str(pipeline_status)
        )
        now = _utc_now()
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT percent FROM pipeline_state WHERE build_id = ?",
                (build_id,),
            ).fetchone()
            if existing is not None and existing["percent"] is not None:
                percent = max(percent, existing["percent"])
            conn.execute(
                """
                UPDATE pipeline_state SET
                    pipeline_status = ?,
                    phase = ?,
                    message = ?,
                    percent = ?,
                    updated_at = ?,
                    resume_phase = ?
                WHERE build_id = ?
                """,
                (status, phase, message, percent, now, phase, build_id),
            )
            if status in PIPELINE_STATUSES:
                conn.execute(
                    "UPDATE builds SET status = ? WHERE id = ?",
                    (status, build_id),
                )

    def update_pipeline_state(
        self,
        build_id: str,
        *,
        phase: str,
        message: str,
        percent: float,
        status: Optional[str] = None,
    ) -> None:
        """Legacy helper — prefer PipelineStateMachine."""
        pipeline_status = status or PHASE_TO_STATUS.get(phase, "generating")
        self.apply_pipeline_state(
            build_id,
            pipeline_status=pipeline_status,
            phase=phase,
            message=message,
            percent=percent,
        )

    def get_progress(self, build_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM pipeline_state WHERE build_id = ?", (build_id,)
            ).fetchone()
        if not row:
            return {
                "status": "unknown",
                "phase": "unknown",
                "message": "Build not found",
                "percent": 0,
                "pipeline_status": "unknown",
            }
        pipeline_status = row["pipeline_status"]
        if pipeline_status == "complete":
            ui_status = "complete"
        elif pipeline_status == "failed":
            ui_status = "failed"
        elif pipeline_status == "interrupted":
            ui_status = "interrupted"
        else:
            ui_status = "running"
        return {
            "status": ui_status,
            "pipeline_status": pipeline_status,
            "phase": row["phase"],
            "message": row["message"],
            "percent": row["percent"],
            "timestamp": row["updated_at"],
            "resume_phase": row["resume_phase"],
            "can_resume": pipeline_status in RESUMABLE_STATUSES,
        }

    def save_checkpoint(self, build_id: str, checkpoint: dict[str, Any]) -> None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT checkpoint_json FROM pipeline_state WHERE build_id = ?",
                (build_id,),
            ).fetchone()
            current = _json_loads(row["checkpoint_json"] if row else None, {})
            current.update(checkpoint)
            conn.execute(
                """
                UPDATE pipeline_state SET checkpoint_json = ?, updated_at = ?
                WHERE build_id = ?
                """,
                (_json_dumps(current), _utc_now(), build_id),
            )

    def get_checkpoint(self, build_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT checkpoint_json, resume_phase FROM pipeline_state WHERE build_id = ?",
                (build_id,),
            ).fetchone()
        if not row:
            return {}
        checkpoint = _json_loads(row["checkpoint_json"], {})
        checkpoint["_resume_phase"] = row["resume_phase"]
        return checkpoint

    def _load_plan_field(self, build_id: str, field: str):
        row = self.get_checkpoint(build_id)
        return row.get(field) if isinstance(row, dict) else None

    # ── Logs ─────────────────────────────────────────────────────────────────

    def append_log(
        self,
        build_id: str,
        stage: str,
        message: str,
        *,
        level: str = "info",
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO pipeline_logs (build_id, timestamp, stage, level, message)
                VALUES (?, ?, ?, ?, ?)
                """,
                (build_id, _utc_now(), stage, level, message),
            )

    def get_logs(self, build_id: str, *, limit: int = 200) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT timestamp, stage, level, message
                FROM pipeline_logs
                WHERE build_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (build_id, limit),
            ).fetchall()
        return [
            {
                "timestamp": r["timestamp"],
                "stage": r["stage"],
                "level": r["level"],
                "message": r["message"],
            }
            for r in reversed(rows)
        ]

    # ── Leaderboard ──────────────────────────────────────────────────────────

    def add_leaderboard_entry(
        self,
        entry: Any,
        *,
        build_id: Optional[str] = None,
        trait_vector: Any = None,
        dominant_traits: Any = None,
        weak_traits: Any = None,
        builder_traits: Any = None,
    ) -> str:
        tv_json = _json_dumps(trait_vector) if trait_vector else None
        dom_json = _json_dumps(dominant_traits) if dominant_traits else None
        weak_json = _json_dumps(weak_traits) if weak_traits else None
        bt_json = _json_dumps(builder_traits) if builder_traits else None
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO leaderboard_entries (
                    entry_id, build_id, project_name, code_type, score, novelty_rating,
                    tool_stack, build_time_seconds, user_rating, created_at, download_path,
                    model_used, trait_vector, trait_vector_version, dominant_traits,
                    weak_traits, builder_traits
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    entry.entry_id,
                    build_id,
                    entry.project_name,
                    entry.code_type,
                    entry.score,
                    entry.novelty_rating,
                    entry.tool_stack,
                    entry.build_time_seconds,
                    entry.user_rating,
                    entry.created_at,
                    entry.download_path,
                    entry.model_used,
                    tv_json,
                    "2.1",
                    dom_json,
                    weak_json,
                    bt_json,
                ),
            )
        return entry.entry_id

    def get_leaderboard_entries(
        self,
        timeframe: str = "all_time",
        code_type: Optional[str] = None,
        sort_by: str = "score",
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        from datetime import datetime, timedelta

        now = datetime.now()
        if timeframe == "current_month":
            start_date = now.replace(day=1).isoformat()
        elif timeframe == "past_year":
            start_date = (now - timedelta(days=365)).isoformat()
        else:
            start_date = "1970-01-01"

        query = "SELECT * FROM leaderboard_entries WHERE created_at >= ?"
        params: list[Any] = [start_date]
        if code_type:
            query += " AND code_type = ?"
            params.append(code_type)
        sort_column = "score" if sort_by == "score" else "created_at"
        query += f" ORDER BY {sort_column} DESC LIMIT ?"
        params.append(limit)

        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()

        entries = []
        for row in rows:
            entries.append(
                {
                    "entry_id": row["entry_id"],
                    "build_id": row["build_id"],
                    "project_name": row["project_name"],
                    "code_type": row["code_type"],
                    "score": row["score"],
                    "novelty_rating": row["novelty_rating"],
                    "tool_stack": row["tool_stack"],
                    "build_time_seconds": row["build_time_seconds"],
                    "user_rating": row["user_rating"],
                    "created_at": row["created_at"],
                    "download_path": row["download_path"],
                    "model_used": row["model_used"],
                    "trait_vector": _json_loads(row["trait_vector"]),
                    "dominant_traits": _json_loads(row["dominant_traits"], []),
                    "weak_traits": _json_loads(row["weak_traits"], []),
                    "builder_traits": _json_loads(row["builder_traits"]),
                }
            )
        return entries

    def get_leaderboard_stats(self) -> dict[str, Any]:
        with self._connect() as conn:
            total = conn.execute("SELECT COUNT(*) FROM leaderboard_entries").fetchone()[0]
            avg_score = conn.execute(
                "SELECT AVG(score) FROM leaderboard_entries"
            ).fetchone()[0] or 0
            max_score = conn.execute(
                "SELECT MAX(score) FROM leaderboard_entries"
            ).fetchone()[0] or 0
            by_type = {
                row[0]: row[1]
                for row in conn.execute(
                    "SELECT code_type, COUNT(*) FROM leaderboard_entries GROUP BY code_type"
                ).fetchall()
            }
            top_stacks = [
                {"stack": row[0], "count": row[1]}
                for row in conn.execute(
                    """
                    SELECT tool_stack, COUNT(*) AS c FROM leaderboard_entries
                    GROUP BY tool_stack ORDER BY c DESC LIMIT 5
                    """
                ).fetchall()
            ]
        return {
            "total_entries": total,
            "average_score": round(avg_score, 2),
            "highest_score": max_score,
            "by_type": by_type,
            "top_stacks": top_stacks,
        }

    def rate_leaderboard_entry(self, entry_id: str, rating: int) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE leaderboard_entries SET user_rating = ? WHERE entry_id = ?",
                (rating, entry_id),
            )
            return cur.rowcount > 0

    # ── Stage locks & idempotency ─────────────────────────────────────────────

    def get_stage_run(self, build_id: str, stage: str) -> Optional[dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT build_id, stage, status, worker_id, lock_token,
                       started_at, completed_at, expires_at, attempt, last_error
                FROM pipeline_stage_runs
                WHERE build_id = ? AND stage = ?
                """,
                (build_id, stage),
            ).fetchone()
        if not row:
            return None
        return dict(row)

    def _get_stage_run_status(self, build_id: str, stage: str) -> Optional[str]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT status FROM pipeline_stage_runs WHERE build_id = ? AND stage = ?",
                (build_id, stage),
            ).fetchone()
        if not row:
            return None
        return row["status"]

    def _stage_transition_allowed(
        self,
        current_status: Optional[str],
        next_status: str,
        *,
        recovery: bool = False,
    ) -> bool:
        if current_status is None:
            return next_status in ("running", "completed", "failed")
        if current_status in ("completed", "failed"):
            return current_status == next_status or (next_status == "pending" and recovery)
        if current_status == "pending" and next_status in ("running", "completed", "failed"):
            return True
        if current_status == "running" and next_status in ("completed", "failed"):
            return True
        if current_status == "running" and next_status == "pending":
            return recovery
        if current_status == "failed" and next_status == "pending" and recovery:
            return True
        return False

    def try_acquire_stage_lock(
        self,
        build_id: str,
        stage: str,
        worker_id: str,
        *,
        ttl_seconds: int = 3600,
    ) -> tuple[bool, Optional[str]]:
        """Atomically acquire (build_id, stage). Returns (ok, lock_token)."""
        import uuid

        token = uuid.uuid4().hex
        now = _utc_now()
        expires = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
        max_attempts = int(getattr(Config, "STAGE_MAX_ATTEMPTS", 3))

        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO pipeline_stage_runs (
                    build_id, stage, status, worker_id, lock_token,
                    started_at, expires_at, attempt
                ) VALUES (?, ?, 'running', ?, ?, ?, ?, 1)
                ON CONFLICT(build_id, stage) DO UPDATE SET
                    status = 'running',
                    worker_id = excluded.worker_id,
                    lock_token = excluded.lock_token,
                    started_at = excluded.started_at,
                    expires_at = excluded.expires_at,
                    attempt = pipeline_stage_runs.attempt + 1,
                    last_error = NULL
                WHERE pipeline_stage_runs.status = 'pending'
                   OR (
                       pipeline_stage_runs.status = 'running'
                       AND pipeline_stage_runs.expires_at <= ?
                       AND pipeline_stage_runs.attempt < ?
                   )
                """,
                (
                    build_id,
                    stage,
                    worker_id,
                    token,
                    now,
                    expires,
                    now,
                    max_attempts,
                ),
            )
            if cur.rowcount == 0:
                return False, None
        return True, token

    def complete_stage_run(
        self,
        build_id: str,
        stage: str,
        *,
        lock_token: Optional[str] = None,
    ) -> None:
        now = _utc_now()
        current_status = self._get_stage_run_status(build_id, stage)
        if current_status in ("completed", "failed"):
            return
        if not self._stage_transition_allowed(current_status, "completed"):
            return
        with self._connect() as conn:
            if lock_token:
                cur = conn.execute(
                    """
                    UPDATE pipeline_stage_runs
                    SET status = 'completed', completed_at = ?, lock_token = NULL,
                        last_error = NULL
                    WHERE build_id = ? AND stage = ? AND lock_token = ?
                    """,
                    (now, build_id, stage, lock_token),
                )
            else:
                cur = conn.execute(
                    """
                    UPDATE pipeline_stage_runs
                    SET status = 'completed', completed_at = ?, lock_token = NULL,
                        last_error = NULL
                    WHERE build_id = ? AND stage = ?
                    """,
                    (now, build_id, stage),
                )
            if cur.rowcount == 0 and current_status is None:
                conn.execute(
                    """
                    INSERT INTO pipeline_stage_runs (
                        build_id, stage, status, completed_at, started_at, attempt
                    ) VALUES (?, ?, 'completed', ?, ?, 1)
                    """,
                    (build_id, stage, now, now),
                )

    def complete_stage_with_checkpoint(
        self,
        build_id: str,
        stage: str,
        *,
        lock_token: Optional[str] = None,
    ) -> None:
        """Atomically mark a stage completed and persist the checkpoint marker.

        This ensures the checkpoint and the stage_run row are updated in a
        single DB transaction so late exceptions cannot observe a half-committed
        state.
        """
        now = _utc_now()
        with self._connect() as conn:
            # load and update checkpoint JSON in the same transaction
            row = conn.execute(
                "SELECT checkpoint_json FROM pipeline_state WHERE build_id = ?",
                (build_id,),
            ).fetchone()
            checkpoint = _json_loads(row["checkpoint_json"] if row else None, {})
            checkpoint.update({f"_stage_{stage}_done": True})

            # ensure planning artifacts are persisted when marking planning complete
            if stage.upper() == "PLANNING":
                checkpoint.update({
                    "tool_combinations": self._load_plan_field(build_id, "tool_combinations"),
                    "factory_review": self._load_plan_field(build_id, "factory_review"),
                })
            conn.execute(
                """
                UPDATE pipeline_state SET checkpoint_json = ?, updated_at = ?
                WHERE build_id = ?
                """,
                (_json_dumps(checkpoint), now, build_id),
            )

            current_status = None
            r = conn.execute(
                "SELECT status FROM pipeline_stage_runs WHERE build_id = ? AND stage = ?",
                (build_id, stage),
            ).fetchone()
            if r:
                current_status = r["status"]
            if current_status in ("completed", "failed"):
                return

            # perform the same completion update logic as complete_stage_run
            if lock_token:
                cur = conn.execute(
                    """
                    UPDATE pipeline_stage_runs
                    SET status = 'completed', completed_at = ?, lock_token = NULL,
                        last_error = NULL
                    WHERE build_id = ? AND stage = ? AND lock_token = ?
                    """,
                    (now, build_id, stage, lock_token),
                )
            else:
                cur = conn.execute(
                    """
                    UPDATE pipeline_stage_runs
                    SET status = 'completed', completed_at = ?, lock_token = NULL,
                        last_error = NULL
                    WHERE build_id = ? AND stage = ?
                    """,
                    (now, build_id, stage),
                )
            if cur.rowcount == 0 and current_status is None:
                conn.execute(
                    """
                    INSERT INTO pipeline_stage_runs (
                        build_id, stage, status, completed_at, started_at, attempt
                    ) VALUES (?, ?, 'completed', ?, ?, 1)
                    """,
                    (build_id, stage, now, now),
                )

    def fail_stage_run(
        self,
        build_id: str,
        stage: str,
        *,
        lock_token: Optional[str] = None,
        error: str = "",
    ) -> None:
        now = _utc_now()
        current_status = self._get_stage_run_status(build_id, stage)
        if current_status in ("completed", "failed"):
            return
        if not self._stage_transition_allowed(current_status, "failed"):
            return
        with self._connect() as conn:
            if lock_token:
                cur = conn.execute(
                    """
                    UPDATE pipeline_stage_runs
                    SET status = 'failed', lock_token = NULL, last_error = ?
                    WHERE build_id = ? AND stage = ? AND lock_token = ?
                    """,
                    (error[:2000], build_id, stage, lock_token),
                )
            else:
                cur = conn.execute(
                    """
                    UPDATE pipeline_stage_runs
                    SET status = 'failed', lock_token = NULL, last_error = ?
                    WHERE build_id = ? AND stage = ?
                    """,
                    (error[:2000], build_id, stage),
                )
            if cur.rowcount == 0 and current_status is None:
                conn.execute(
                    """
                    INSERT INTO pipeline_stage_runs (
                        build_id, stage, status, started_at, completed_at,
                        attempt, last_error
                    ) VALUES (?, ?, 'failed', ?, ?, 1, ?)
                    """,
                    (build_id, stage, now, now, error[:2000]),
                )

    def reset_expired_stage_run(self, build_id: str, stage: str) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE pipeline_stage_runs
                SET status = 'pending', lock_token = NULL, expires_at = NULL
                WHERE build_id = ? AND stage = ? AND status = 'running'
                """,
                (build_id, stage),
            )
        return cur.rowcount

    def reset_stage_run(self, build_id: str, stage: str, *, statuses: tuple[str, ...] = ("completed", "failed")) -> int:
        placeholders = ", ".join("?" for _ in statuses)
        sql = f"""
                UPDATE pipeline_stage_runs
                SET status = 'pending', lock_token = NULL, expires_at = NULL,
                    started_at = NULL, completed_at = NULL, last_error = NULL
                WHERE build_id = ? AND stage = ? AND status IN ({placeholders})
                """
        with self._connect() as conn:
            cur = conn.execute(sql, (build_id, stage, *statuses))
        return cur.rowcount

    def list_expired_running_stage_runs(
        self,
        *,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        cutoff = _utc_now()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT build_id, stage, status, worker_id, lock_token,
                       started_at, completed_at, expires_at, attempt, last_error
                FROM pipeline_stage_runs
                WHERE status = 'running' AND expires_at <= ?
                ORDER BY expires_at ASC
                LIMIT ?
                """,
                (cutoff, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def release_stage_lock(
        self,
        build_id: str,
        stage: str,
        *,
        lock_token: Optional[str] = None,
    ) -> None:
        with self._connect() as conn:
            if lock_token:
                conn.execute(
                    """
                    UPDATE pipeline_stage_runs
                    SET status = 'pending', lock_token = NULL
                    WHERE build_id = ? AND stage = ? AND lock_token = ?
                      AND status = 'running'
                    """,
                    (build_id, stage, lock_token),
                )
            else:
                conn.execute(
                    """
                    UPDATE pipeline_stage_runs
                    SET status = 'pending', lock_token = NULL
                    WHERE build_id = ? AND stage = ? AND status = 'running'
                    """,
                    (build_id, stage),
                )

    def release_expired_stage_locks(self) -> int:
        now = _utc_now()
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE pipeline_stage_runs
                SET status = 'pending', lock_token = NULL
                WHERE status = 'running' AND expires_at IS NOT NULL AND expires_at <= ?
                """,
                (now,),
            )
            return cur.rowcount

    def list_stale_active_builds(
        self,
        *,
        stuck_minutes: int = 30,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=stuck_minutes)).isoformat()
        active = tuple(ACTIVE_PIPELINE_STATUSES)
        placeholders = ",".join("?" * len(active))
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT ps.build_id, ps.pipeline_status, ps.phase, ps.updated_at
                FROM pipeline_state ps
                JOIN builds b ON b.id = ps.build_id
                WHERE ps.pipeline_status IN ({placeholders})
                  AND b.status NOT IN ('complete', 'failed')
                  AND ps.updated_at < ?
                ORDER BY ps.updated_at ASC
                LIMIT ?
                """,
                (*active, cutoff, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def list_resumable_builds(self, *, limit: int = 100) -> list[dict[str, Any]]:
        """Interrupted or stale active builds — excludes fresh queued (avoid double dispatch)."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT b.id, b.status, ps.pipeline_status, ps.updated_at
                FROM builds b
                JOIN pipeline_state ps ON ps.build_id = b.id
                WHERE b.status NOT IN ('complete', 'failed')
                  AND (
                    ps.pipeline_status = 'interrupted'
                    OR ps.pipeline_status IN (
                        'planning', 'generating', 'reviewing', 'ranking', 'packaging'
                    )
                  )
                ORDER BY ps.updated_at ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ── Dispatch deduplication ───────────────────────────────────────────────

    def try_claim_dispatch(
        self,
        build_id: str,
        stage: str,
        *,
        stale_seconds: int = 300,
    ) -> bool:
        """
        Claim exclusive dispatch slot for (build_id, stage).
        Returns False if another pending dispatch is still valid.
        """
        now = _utc_now()
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=stale_seconds)).isoformat()

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT status, created_at FROM pipeline_dispatch
                WHERE build_id = ? AND stage = ?
                """,
                (build_id, stage),
            ).fetchone()

            if row:
                if row["status"] == "pending" and row["created_at"] > cutoff:
                    return False
                conn.execute(
                    "DELETE FROM pipeline_dispatch WHERE build_id = ? AND stage = ?",
                    (build_id, stage),
                )

            conn.execute(
                """
                INSERT INTO pipeline_dispatch (build_id, stage, status, created_at)
                VALUES (?, ?, 'pending', ?)
                """,
                (build_id, stage, now),
            )
        return True

    def register_dispatch_task(
        self,
        build_id: str,
        stage: str,
        task_id: str,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE pipeline_dispatch
                SET task_id = ?, created_at = ?
                WHERE build_id = ? AND stage = ? AND status = 'pending'
                """,
                (task_id, _utc_now(), build_id, stage),
            )

    def consume_dispatch(self, build_id: str, stage: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE pipeline_dispatch
                SET status = 'consumed', consumed_at = ?
                WHERE build_id = ? AND stage = ?
                """,
                (_utc_now(), build_id, stage),
            )

    def clear_dispatch(self, build_id: str, stage: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM pipeline_dispatch WHERE build_id = ? AND stage = ?",
                (build_id, stage),
            )

    def try_acquire_cluster_lock(self, name: str, *, ttl_seconds: int = 120) -> bool:
        import uuid

        holder = f"{name}:{uuid.uuid4().hex[:8]}"
        now = datetime.now(timezone.utc)
        now_s = now.isoformat()
        expires = (now + timedelta(seconds=ttl_seconds)).isoformat()

        with self._connect() as conn:
            conn.execute(
                "DELETE FROM cluster_locks WHERE name = ? AND expires_at <= ?",
                (name, now_s),
            )
            row = conn.execute(
                "SELECT expires_at FROM cluster_locks WHERE name = ?", (name,)
            ).fetchone()
            if row and row["expires_at"] > now_s:
                return False
            conn.execute(
                "INSERT OR REPLACE INTO cluster_locks (name, holder, expires_at) VALUES (?, ?, ?)",
                (name, holder, expires),
            )
        return True


ACTIVE_PIPELINE_STATUSES = frozenset(
    {
        "queued",
        "planning",
        "generating",
        "reviewing",
        "ranking",
        "packaging",
        "interrupted",
    }
)


_db: Optional[AppDatabase] = None


def get_database() -> AppDatabase:
    global _db
    if _db is None:
        _db = AppDatabase()
    return _db
