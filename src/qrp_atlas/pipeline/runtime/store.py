"""SQLite WAL storage for Pipeline scheduling, runs, stages, and resource leases."""

from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .models import (
    OverlapPolicy,
    PipelineDefinition,
    PipelineRun,
    PipelineStatus,
    StageRun,
    assert_status_transition,
)


_STATUS_SQL = ", ".join(f"'{status.value}'" for status in PipelineStatus)


class RunClaimFailure(RuntimeError):
    """A fail-closed reason returned by the atomic runner claim operation."""

    def __init__(self, code: str, detail: str | None = None) -> None:
        self.code = code
        self.detail = detail
        message = code if detail is None else f"{code}: {detail}"
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class SchedulerCursor:
    scheduler_id: str
    last_scanned_at: datetime
    created_at: datetime
    updated_at: datetime


def utc_now() -> datetime:
    return datetime.now(UTC)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("runtime timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _timestamp(value: datetime | None) -> str | None:
    return _as_utc(value).isoformat() if value is not None else None


def _parse_timestamp(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value).astimezone(UTC) if value else None


class PipelineRuntimeStore:
    """Owns a single isolated SQLite runtime database, never a market database."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path).resolve(strict=False)

    def _connect(self) -> sqlite3.Connection:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(
            self.database_path,
            timeout=5.0,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    @contextmanager
    def _transaction(self):
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        """Create the independent runtime schema idempotently."""

        connection = self._connect()
        try:
            connection.executescript(
                f"""
                CREATE TABLE IF NOT EXISTS pipeline_run (
                    run_id TEXT PRIMARY KEY,
                    pipeline_id TEXT NOT NULL,
                    definition_version TEXT NOT NULL,
                    scheduled_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    status TEXT NOT NULL CHECK(status IN ({_STATUS_SQL})),
                    attempt INTEGER NOT NULL CHECK(attempt >= 1),
                    exit_code INTEGER,
                    timed_out INTEGER NOT NULL DEFAULT 0 CHECK(timed_out IN (0, 1)),
                    trigger_type TEXT NOT NULL,
                    stdout_path TEXT,
                    stderr_path TEXT,
                    error_summary TEXT,
                    heartbeat_at TEXT,
                    wall_duration_ms INTEGER,
                    user_cpu_ms INTEGER,
                    system_cpu_ms INTEGER,
                    peak_rss_kb INTEGER,
                    retry_of_run_id TEXT REFERENCES pipeline_run(run_id),
                    created_at TEXT NOT NULL,
                    UNIQUE(pipeline_id, scheduled_at, attempt)
                );
                CREATE INDEX IF NOT EXISTS pipeline_run_status_idx
                    ON pipeline_run(status, scheduled_at);
                CREATE INDEX IF NOT EXISTS pipeline_run_pipeline_idx
                    ON pipeline_run(pipeline_id, scheduled_at DESC, attempt DESC);
                CREATE TABLE IF NOT EXISTS stage_run (
                    run_id TEXT NOT NULL REFERENCES pipeline_run(run_id),
                    stage_name TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    duration_ms INTEGER,
                    status TEXT NOT NULL CHECK(status IN ({_STATUS_SQL})),
                    input_rows INTEGER,
                    output_rows INTEGER,
                    metadata_json TEXT NOT NULL,
                    PRIMARY KEY(run_id, stage_name)
                );
                CREATE TABLE IF NOT EXISTS resource_lock (
                    resource_name TEXT PRIMARY KEY,
                    owner_run_id TEXT NOT NULL REFERENCES pipeline_run(run_id),
                    acquired_at TEXT NOT NULL,
                    heartbeat_at TEXT NOT NULL,
                    lease_expires_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS resource_lock_expiry_idx
                    ON resource_lock(lease_expires_at);
                CREATE TABLE IF NOT EXISTS scheduler_cursor (
                    scheduler_id TEXT PRIMARY KEY,
                    last_scanned_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS pipeline_result (
                    run_id TEXT PRIMARY KEY REFERENCES pipeline_run(run_id),
                    result_json TEXT NOT NULL,
                    recorded_at TEXT NOT NULL
                );
                """
            )
        finally:
            connection.close()

    def _run_from_row(self, row: sqlite3.Row) -> PipelineRun:
        return PipelineRun(
            run_id=row["run_id"],
            pipeline_id=row["pipeline_id"],
            definition_version=row["definition_version"],
            scheduled_at=_parse_timestamp(row["scheduled_at"]),  # type: ignore[arg-type]
            started_at=_parse_timestamp(row["started_at"]),
            finished_at=_parse_timestamp(row["finished_at"]),
            status=PipelineStatus(row["status"]),
            attempt=row["attempt"],
            exit_code=row["exit_code"],
            timed_out=bool(row["timed_out"]),
            trigger_type=row["trigger_type"],
            stdout_path=Path(row["stdout_path"]) if row["stdout_path"] else None,
            stderr_path=Path(row["stderr_path"]) if row["stderr_path"] else None,
            error_summary=row["error_summary"],
            heartbeat_at=_parse_timestamp(row["heartbeat_at"]),
            wall_duration_ms=row["wall_duration_ms"],
            user_cpu_ms=row["user_cpu_ms"],
            system_cpu_ms=row["system_cpu_ms"],
            peak_rss_kb=row["peak_rss_kb"],
            retry_of_run_id=row["retry_of_run_id"],
        )

    def get_run(self, run_id: str) -> PipelineRun | None:
        connection = self._connect()
        try:
            row = connection.execute("SELECT * FROM pipeline_run WHERE run_id = ?", [run_id]).fetchone()
            return self._run_from_row(row) if row is not None else None
        finally:
            connection.close()

    def record_result(self, run_id: str, payload: Mapping[str, object], *, now: datetime | None = None) -> None:
        """Persist a formal PipelineResult payload with the existing run evidence."""

        timestamp = now or utc_now()
        serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        with self._transaction() as connection:
            exists = connection.execute("SELECT 1 FROM pipeline_run WHERE run_id = ?", [run_id]).fetchone()
            if exists is None:
                raise KeyError(f"unknown pipeline run {run_id}")
            connection.execute(
                """
                INSERT INTO pipeline_result(run_id, result_json, recorded_at)
                VALUES (?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET result_json = excluded.result_json, recorded_at = excluded.recorded_at
                """,
                [run_id, serialized, _timestamp(timestamp)],
            )

    def get_result(self, run_id: str) -> Mapping[str, object] | None:
        connection = self._connect()
        try:
            row = connection.execute("SELECT result_json FROM pipeline_result WHERE run_id = ?", [run_id]).fetchone()
            if row is None:
                return None
            payload = json.loads(row["result_json"])
            if not isinstance(payload, dict):
                raise RuntimeError(f"stored pipeline result for {run_id} is not an object")
            return payload
        finally:
            connection.close()

    def list_runs(
        self,
        *,
        pipeline_id: str | None = None,
        status: PipelineStatus | None = None,
        limit: int = 100,
    ) -> list[PipelineRun]:
        clauses: list[str] = []
        values: list[object] = []
        if pipeline_id is not None:
            clauses.append("pipeline_id = ?")
            values.append(pipeline_id)
        if status is not None:
            clauses.append("status = ?")
            values.append(status.value)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        values.append(limit)
        connection = self._connect()
        try:
            rows = connection.execute(
                f"SELECT * FROM pipeline_run {where} ORDER BY scheduled_at DESC, attempt DESC LIMIT ?",
                values,
            ).fetchall()
            return [self._run_from_row(row) for row in rows]
        finally:
            connection.close()

    def create_scheduled_run(
        self,
        definition: PipelineDefinition,
        *,
        scheduled_at: datetime,
        trigger_type: str = "SCHEDULED",
        status: PipelineStatus = PipelineStatus.PENDING,
        error_summary: str | None = None,
    ) -> tuple[PipelineRun, bool]:
        """Insert the first attempt once; concurrent scans return the same row."""

        if status not in {PipelineStatus.PENDING, PipelineStatus.BLOCKED, PipelineStatus.SKIPPED}:
            raise ValueError("scheduled runs must start as PENDING, BLOCKED, or SKIPPED")
        self.initialize()
        now = utc_now()
        with self._transaction() as connection:
            return self._create_scheduled_run_in_transaction(
                connection,
                definition,
                scheduled_at=scheduled_at,
                trigger_type=trigger_type,
                status=status,
                error_summary=error_summary,
                created_at=now,
            )

    def _create_scheduled_run_in_transaction(
        self,
        connection: sqlite3.Connection,
        definition: PipelineDefinition,
        *,
        scheduled_at: datetime,
        trigger_type: str,
        status: PipelineStatus,
        error_summary: str | None,
        created_at: datetime,
    ) -> tuple[PipelineRun, bool]:
        scheduled_text = _timestamp(scheduled_at)
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO pipeline_run (
                run_id, pipeline_id, definition_version, scheduled_at, status, attempt,
                timed_out, trigger_type, error_summary, created_at
            ) VALUES (?, ?, ?, ?, ?, 1, 0, ?, ?, ?)
            """,
            [
                str(uuid.uuid4()),
                definition.pipeline_id,
                definition.definition_version,
                scheduled_text,
                status.value,
                trigger_type,
                error_summary,
                _timestamp(created_at),
            ],
        )
        row = connection.execute(
            "SELECT * FROM pipeline_run WHERE pipeline_id = ? AND scheduled_at = ? AND attempt = 1",
            [definition.pipeline_id, scheduled_text],
        ).fetchone()
        if row is None:
            raise RuntimeError("failed to create or retrieve scheduled run")
        return self._run_from_row(row), cursor.rowcount == 1

    def get_scheduler_cursor(self, scheduler_id: str) -> SchedulerCursor | None:
        self.initialize()
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM scheduler_cursor WHERE scheduler_id = ?", [scheduler_id]
            ).fetchone()
            if row is None:
                return None
            last_scanned_at = _parse_timestamp(row["last_scanned_at"])
            created_at = _parse_timestamp(row["created_at"])
            updated_at = _parse_timestamp(row["updated_at"])
            if last_scanned_at is None or created_at is None or updated_at is None:
                raise RuntimeError("scheduler cursor contains an invalid timestamp")
            return SchedulerCursor(
                scheduler_id=row["scheduler_id"],
                last_scanned_at=last_scanned_at,
                created_at=created_at,
                updated_at=updated_at,
            )
        finally:
            connection.close()

    def commit_scheduler_scan(
        self,
        *,
        scheduler_id: str,
        expected_last_scanned_at: datetime | None,
        scanned_through_at: datetime,
        candidates: Iterable[tuple[PipelineDefinition, datetime, PipelineStatus, str | None]],
        now: datetime | None = None,
    ) -> list[PipelineRun] | None:
        """Persist a complete scan interval and cursor advance in one transaction.

        ``None`` means another scheduler committed a newer cursor after this
        scanner calculated its interval.  Callers must reread the cursor and
        replay from it; no partially scanned interval is committed here.
        """

        self.initialize()
        timestamp = now or utc_now()
        expected_text = _timestamp(expected_last_scanned_at)
        scanned_text = _timestamp(scanned_through_at)
        with self._transaction() as connection:
            current = connection.execute(
                "SELECT last_scanned_at FROM scheduler_cursor WHERE scheduler_id = ?", [scheduler_id]
            ).fetchone()
            current_text = current["last_scanned_at"] if current is not None else None
            if current_text != expected_text:
                return None
            created: list[PipelineRun] = []
            for definition, scheduled_at, status, error_summary in candidates:
                run, inserted = self._create_scheduled_run_in_transaction(
                    connection,
                    definition,
                    scheduled_at=scheduled_at,
                    trigger_type="SCHEDULED",
                    status=status,
                    error_summary=error_summary,
                    created_at=timestamp,
                )
                if inserted:
                    created.append(run)
            if current is None:
                connection.execute(
                    """
                    INSERT INTO scheduler_cursor(scheduler_id, last_scanned_at, created_at, updated_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    [scheduler_id, scanned_text, _timestamp(timestamp), _timestamp(timestamp)],
                )
            else:
                connection.execute(
                    """
                    UPDATE scheduler_cursor
                    SET last_scanned_at = ?, updated_at = ?
                    WHERE scheduler_id = ? AND last_scanned_at = ?
                    """,
                    [scanned_text, _timestamp(timestamp), scheduler_id, expected_text],
                )
            return created

    def latest_run_before(self, pipeline_id: str, scheduled_at: datetime) -> PipelineRun | None:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT * FROM pipeline_run
                WHERE pipeline_id = ? AND scheduled_at <= ?
                ORDER BY scheduled_at DESC, attempt DESC LIMIT 1
                """,
                [pipeline_id, _timestamp(scheduled_at)],
            ).fetchone()
            return self._run_from_row(row) if row is not None else None
        finally:
            connection.close()

    def has_active_pipeline_run(self, pipeline_id: str) -> bool:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT 1 FROM pipeline_run WHERE pipeline_id = ? AND status IN (?, ?) LIMIT 1",
                [pipeline_id, PipelineStatus.PENDING.value, PipelineStatus.RUNNING.value],
            ).fetchone()
            return row is not None
        finally:
            connection.close()

    def has_active_resource_lock(self, resource_name: str, *, now: datetime | None = None) -> bool:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT 1 FROM resource_lock WHERE resource_name = ? AND lease_expires_at > ?",
                [resource_name, _timestamp(now or utc_now())],
            ).fetchone()
            return row is not None
        finally:
            connection.close()

    def claim_run(
        self,
        run_id: str,
        *,
        pipeline_id: str,
        definition_version: str,
        overlap_policy: OverlapPolicy,
        resource_locks: Iterable[str],
        stdout_path: Path,
        stderr_path: Path,
        lease_seconds: int,
        now: datetime | None = None,
    ) -> PipelineRun:
        """Atomically validate, claim, and lease one PENDING run.

        Every rejection raises :class:`RunClaimFailure` while the surrounding
        ``BEGIN IMMEDIATE`` transaction rolls back. Scheduler eligibility
        checks are only advisory; this is the final concurrency gate.
        """

        self.initialize()
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        timestamp = now or utc_now()
        expires = timestamp + timedelta(seconds=lease_seconds)
        lock_names = tuple(sorted(set(resource_locks)))
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM pipeline_run WHERE run_id = ?", [run_id]).fetchone()
            if row is None:
                raise RunClaimFailure("RUN_NOT_FOUND", run_id)
            if PipelineStatus(row["status"]) is not PipelineStatus.PENDING:
                raise RunClaimFailure("RUN_NOT_PENDING", PipelineStatus(row["status"]).value)
            if row["pipeline_id"] != pipeline_id:
                raise RunClaimFailure("PIPELINE_ID_MISMATCH", row["pipeline_id"])
            if row["definition_version"] != definition_version:
                raise RunClaimFailure("DEFINITION_VERSION_MISMATCH", row["definition_version"])
            if overlap_policy is OverlapPolicy.FORBID:
                overlap = connection.execute(
                    """
                    SELECT run_id FROM pipeline_run
                    WHERE pipeline_id = ? AND status = ? AND run_id != ?
                    LIMIT 1
                    """,
                    [pipeline_id, PipelineStatus.RUNNING.value, run_id],
                ).fetchone()
                if overlap is not None:
                    raise RunClaimFailure("OVERLAP_FORBIDDEN", overlap["run_id"])
            connection.execute("DELETE FROM resource_lock WHERE lease_expires_at <= ?", [_timestamp(timestamp)])
            if lock_names:
                placeholders = ", ".join("?" for _ in lock_names)
                conflict = connection.execute(
                    f"SELECT resource_name FROM resource_lock WHERE resource_name IN ({placeholders}) LIMIT 1",
                    list(lock_names),
                ).fetchone()
                if conflict is not None:
                    raise RunClaimFailure("RESOURCE_LOCK_UNAVAILABLE", conflict["resource_name"])
            assert_status_transition(PipelineStatus.PENDING, PipelineStatus.RUNNING)
            connection.execute(
                """
                UPDATE pipeline_run
                SET status = ?, started_at = ?, heartbeat_at = ?, stdout_path = ?, stderr_path = ?
                WHERE run_id = ?
                """,
                [
                    PipelineStatus.RUNNING.value,
                    _timestamp(timestamp),
                    _timestamp(timestamp),
                    str(stdout_path),
                    str(stderr_path),
                    run_id,
                ],
            )
            for resource_name in lock_names:
                connection.execute(
                    """
                    INSERT INTO resource_lock(resource_name, owner_run_id, acquired_at, heartbeat_at, lease_expires_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        resource_name,
                        run_id,
                        _timestamp(timestamp),
                        _timestamp(timestamp),
                        _timestamp(expires),
                    ],
                )
            claimed = connection.execute("SELECT * FROM pipeline_run WHERE run_id = ?", [run_id]).fetchone()
            return self._run_from_row(claimed) if claimed is not None else None

    def heartbeat(self, run_id: str, *, lease_seconds: int, now: datetime | None = None) -> bool:
        timestamp = now or utc_now()
        with self._transaction() as connection:
            cursor = connection.execute(
                "UPDATE pipeline_run SET heartbeat_at = ? WHERE run_id = ? AND status = ?",
                [_timestamp(timestamp), run_id, PipelineStatus.RUNNING.value],
            )
            if cursor.rowcount != 1:
                return False
            connection.execute(
                """
                UPDATE resource_lock SET heartbeat_at = ?, lease_expires_at = ?
                WHERE owner_run_id = ?
                """,
                [_timestamp(timestamp), _timestamp(timestamp + timedelta(seconds=lease_seconds)), run_id],
            )
            return True

    def finish_run(
        self,
        run_id: str,
        *,
        status: PipelineStatus,
        exit_code: int | None,
        timed_out: bool,
        error_summary: str | None,
        wall_duration_ms: int,
        user_cpu_ms: int | None,
        system_cpu_ms: int | None,
        peak_rss_kb: int | None,
        now: datetime | None = None,
    ) -> PipelineRun:
        if status not in {
            PipelineStatus.SUCCESS,
            PipelineStatus.FAILED,
            PipelineStatus.TIMED_OUT,
            PipelineStatus.CANCELLED,
        }:
            raise ValueError("finish_run requires a terminal execution status")
        timestamp = now or utc_now()
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM pipeline_run WHERE run_id = ?", [run_id]).fetchone()
            if row is None:
                raise KeyError(f"unknown pipeline run {run_id}")
            current = PipelineStatus(row["status"])
            assert_status_transition(current, status)
            connection.execute(
                """
                UPDATE pipeline_run
                SET status = ?, finished_at = ?, heartbeat_at = ?, exit_code = ?, timed_out = ?,
                    error_summary = ?, wall_duration_ms = ?, user_cpu_ms = ?, system_cpu_ms = ?,
                    peak_rss_kb = ?
                WHERE run_id = ?
                """,
                [
                    status.value,
                    _timestamp(timestamp),
                    _timestamp(timestamp),
                    exit_code,
                    int(timed_out),
                    error_summary,
                    wall_duration_ms,
                    user_cpu_ms,
                    system_cpu_ms,
                    peak_rss_kb,
                    run_id,
                ],
            )
            connection.execute("DELETE FROM resource_lock WHERE owner_run_id = ?", [run_id])
            result = connection.execute("SELECT * FROM pipeline_run WHERE run_id = ?", [run_id]).fetchone()
            if result is None:
                raise RuntimeError("run disappeared while being finalized")
            return self._run_from_row(result)

    def retry_run(
        self,
        run_id: str,
        *,
        max_retries: int | None = None,
        now: datetime | None = None,
    ) -> PipelineRun:
        """Create another attempt without changing the failed run evidence."""

        timestamp = now or utc_now()
        with self._transaction() as connection:
            previous = connection.execute("SELECT * FROM pipeline_run WHERE run_id = ?", [run_id]).fetchone()
            if previous is None:
                raise KeyError(f"unknown pipeline run {run_id}")
            if PipelineStatus(previous["status"]) not in {PipelineStatus.FAILED, PipelineStatus.TIMED_OUT}:
                raise ValueError("only FAILED or TIMED_OUT runs may be retried")
            next_attempt = connection.execute(
                "SELECT COALESCE(MAX(attempt), 0) + 1 FROM pipeline_run WHERE pipeline_id = ? AND scheduled_at = ?",
                [previous["pipeline_id"], previous["scheduled_at"]],
            ).fetchone()[0]
            if max_retries is not None and next_attempt > max_retries + 1:
                raise ValueError(f"max_retries={max_retries} exhausted for {previous['pipeline_id']}")
            retry_id = str(uuid.uuid4())
            connection.execute(
                """
                INSERT INTO pipeline_run (
                    run_id, pipeline_id, definition_version, scheduled_at, status, attempt,
                    timed_out, trigger_type, retry_of_run_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
                """,
                [
                    retry_id,
                    previous["pipeline_id"],
                    previous["definition_version"],
                    previous["scheduled_at"],
                    PipelineStatus.PENDING.value,
                    next_attempt,
                    "RETRY",
                    run_id,
                    _timestamp(timestamp),
                ],
            )
            row = connection.execute("SELECT * FROM pipeline_run WHERE run_id = ?", [retry_id]).fetchone()
            if row is None:
                raise RuntimeError("retry run was not created")
            return self._run_from_row(row)

    def unblock_run(self, run_id: str) -> PipelineRun | None:
        """Return a still-relevant BLOCKED record to PENDING without duplicating it."""

        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM pipeline_run WHERE run_id = ?", [run_id]).fetchone()
            if row is None or PipelineStatus(row["status"]) is not PipelineStatus.BLOCKED:
                return None
            assert_status_transition(PipelineStatus.BLOCKED, PipelineStatus.PENDING)
            connection.execute(
                "UPDATE pipeline_run SET status = ?, error_summary = NULL WHERE run_id = ?",
                [PipelineStatus.PENDING.value, run_id],
            )
            result = connection.execute("SELECT * FROM pipeline_run WHERE run_id = ?", [run_id]).fetchone()
            return self._run_from_row(result) if result is not None else None

    def recover_stale(self, *, stale_after_seconds: int, now: datetime | None = None) -> tuple[int, int]:
        """Fail zombie RUNNING rows and reclaim expired resource leases."""

        if stale_after_seconds <= 0:
            raise ValueError("stale_after_seconds must be positive")
        timestamp = now or utc_now()
        cutoff = timestamp - timedelta(seconds=stale_after_seconds)
        with self._transaction() as connection:
            stale = connection.execute(
                """
                SELECT run_id FROM pipeline_run
                WHERE status = ? AND (heartbeat_at IS NULL OR heartbeat_at < ?)
                """,
                [PipelineStatus.RUNNING.value, _timestamp(cutoff)],
            ).fetchall()
            for row in stale:
                connection.execute(
                    """
                    UPDATE pipeline_run
                    SET status = ?, finished_at = ?, error_summary = ?
                    WHERE run_id = ?
                    """,
                    [
                        PipelineStatus.FAILED.value,
                        _timestamp(timestamp),
                        "stale heartbeat recovery",
                        row["run_id"],
                    ],
                )
                connection.execute("DELETE FROM resource_lock WHERE owner_run_id = ?", [row["run_id"]])
            deleted = connection.execute(
                "DELETE FROM resource_lock WHERE lease_expires_at <= ?", [_timestamp(timestamp)]
            ).rowcount
            return len(stale), max(deleted, 0)

    def start_stage(
        self,
        run_id: str,
        stage_name: str,
        *,
        input_rows: int | None = None,
        metadata: Mapping[str, object] | None = None,
        now: datetime | None = None,
    ) -> StageRun:
        timestamp = now or utc_now()
        with self._transaction() as connection:
            connection.execute(
                """
                INSERT INTO stage_run(run_id, stage_name, started_at, status, input_rows, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    stage_name,
                    _timestamp(timestamp),
                    PipelineStatus.RUNNING.value,
                    input_rows,
                    json.dumps(metadata or {}, sort_keys=True),
                ],
            )
        return StageRun(
            run_id=run_id,
            stage_name=stage_name,
            started_at=timestamp,
            finished_at=None,
            duration_ms=None,
            status=PipelineStatus.RUNNING,
            input_rows=input_rows,
            output_rows=None,
            metadata=metadata or {},
        )

    def finish_stage(
        self,
        run_id: str,
        stage_name: str,
        *,
        status: PipelineStatus,
        output_rows: int | None = None,
        metadata: Mapping[str, object] | None = None,
        now: datetime | None = None,
    ) -> StageRun:
        if status not in {PipelineStatus.SUCCESS, PipelineStatus.FAILED, PipelineStatus.SKIPPED}:
            raise ValueError("stage completion must be SUCCESS, FAILED, or SKIPPED")
        timestamp = now or utc_now()
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM stage_run WHERE run_id = ? AND stage_name = ?", [run_id, stage_name]
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown stage {stage_name!r} for run {run_id}")
            started = _parse_timestamp(row["started_at"])
            assert started is not None
            duration_ms = max(0, int((timestamp - started).total_seconds() * 1000))
            result_metadata = metadata if metadata is not None else json.loads(row["metadata_json"])
            connection.execute(
                """
                UPDATE stage_run
                SET finished_at = ?, duration_ms = ?, status = ?, output_rows = ?, metadata_json = ?
                WHERE run_id = ? AND stage_name = ?
                """,
                [
                    _timestamp(timestamp),
                    duration_ms,
                    status.value,
                    output_rows,
                    json.dumps(result_metadata, sort_keys=True),
                    run_id,
                    stage_name,
                ],
            )
        return StageRun(
            run_id=run_id,
            stage_name=stage_name,
            started_at=started,
            finished_at=timestamp,
            duration_ms=duration_ms,
            status=status,
            input_rows=row["input_rows"],
            output_rows=output_rows,
            metadata=result_metadata,
        )
