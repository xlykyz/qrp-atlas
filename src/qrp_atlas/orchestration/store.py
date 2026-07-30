"""SQLite WAL storage for Job scheduling, runs, stages, and resource leases."""

from __future__ import annotations

import json
import re
import sqlite3
import uuid
from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from .models import (
    OverlapPolicy,
    JobDefinition,
    JobRun,
    JobStatus,
    JobStageRun,
    assert_status_transition,
)


_STATUS_SQL = ", ".join(f"'{status.value}'" for status in JobStatus)


class JobClaimFailure(RuntimeError):
    """A fail-closed reason returned by the atomic runner claim operation."""

    def __init__(self, code: str, detail: str | None = None) -> None:
        self.code = code
        self.detail = detail
        message = code if detail is None else f"{code}: {detail}"
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class JobSchedulerCursor:
    scheduler_id: str
    last_scanned_at: datetime
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class JobServiceLease:
    """One live scheduler service identity stored in the isolated runtime DB."""

    service_name: str
    owner_id: str
    started_at: datetime
    heartbeat_at: datetime
    lease_expires_at: datetime
    last_error: str | None


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


def _encode_parameter_overrides(value: Mapping[str, Any] | None) -> str:
    if value is None:
        return "{}"
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise ValueError("parameter_overrides must be a string-keyed mapping")
    sensitive = [
        key
        for key in value
        if re.search(r"(?:api[_-]?key|token|password|passwd|secret|credential)", key, re.IGNORECASE)
    ]
    if sensitive:
        raise ValueError("parameter_overrides may not contain credential-like keys")
    try:
        encoded = json.dumps(dict(value), ensure_ascii=False, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError("parameter_overrides must contain JSON-compatible values") from exc
    if len(encoded) > 100_000:
        raise ValueError("parameter_overrides exceeds the 100000-character limit")
    return encoded


def _decode_parameter_overrides(value: str | None) -> Mapping[str, Any]:
    if not value:
        return {}
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Job run contains invalid parameter_overrides_json") from exc
    if not isinstance(decoded, dict) or any(not isinstance(key, str) for key in decoded):
        raise RuntimeError("Job run parameter_overrides_json must be an object")
    return decoded


_SECRET_VALUE_PATTERN = re.compile(
    r"(?i)(api[_-]?key|token|password|passwd|secret|credential)(\s*[:=]\s*)[^\s,;]+"
)


def _bounded_error_summary(value: str | None) -> str | None:
    """Keep persisted failure evidence single-line, bounded, and redacted."""

    if value is None:
        return None
    normalized = " ".join(str(value).replace("\x00", " ").split())
    normalized = _SECRET_VALUE_PATTERN.sub(r"\1\2[REDACTED]", normalized)
    return normalized[:500]


def _resource_scope(resource_name: str) -> tuple[str, str | None] | None:
    """Return a DuckDB database/object scope for conflict arbitration.

    ``duckdb://<database>#<object>`` is the explicit form.  Existing managed
    names such as ``quant_db_writer`` remain database-wide resources, so old
    definitions continue to serialize safely with table-scoped declarations.
    """

    if resource_name.startswith("duckdb://"):
        value = resource_name[len("duckdb://") :]
        database, separator, object_name = value.partition("#")
        if database and separator and object_name:
            return database, object_name
        if database:
            return database, None
    if resource_name.endswith("_writer"):
        database = resource_name[: -len("_writer")]
        if database:
            return database, None
    if resource_name.endswith("_db"):
        return resource_name, None
    return None


def _resources_conflict(left: str, right: str) -> bool:
    if left == right:
        return True
    left_scope = _resource_scope(left)
    right_scope = _resource_scope(right)
    if left_scope is None or right_scope is None or left_scope[0] != right_scope[0]:
        return False
    left_object, right_object = left_scope[1], right_scope[1]
    return left_object is None or right_object is None or left_object == right_object


class JobRuntimeStore:
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
                CREATE TABLE IF NOT EXISTS job_run (
                    run_id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
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
                    retry_of_run_id TEXT REFERENCES job_run(run_id),
                    trade_date_override TEXT,
                    parameter_overrides_json TEXT NOT NULL DEFAULT '{{}}',
                    created_at TEXT NOT NULL,
                    UNIQUE(job_id, scheduled_at, attempt)
                );
                CREATE INDEX IF NOT EXISTS job_run_status_idx
                    ON job_run(status, scheduled_at);
                CREATE INDEX IF NOT EXISTS job_run_job_idx
                    ON job_run(job_id, scheduled_at DESC, attempt DESC);
                CREATE TABLE IF NOT EXISTS stage_run (
                    run_id TEXT NOT NULL REFERENCES job_run(run_id),
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
                    owner_run_id TEXT NOT NULL REFERENCES job_run(run_id),
                    acquired_at TEXT NOT NULL,
                    heartbeat_at TEXT NOT NULL,
                    lease_expires_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS resource_lock_expiry_idx
                    ON resource_lock(lease_expires_at);
                CREATE TABLE IF NOT EXISTS resource_read_lease (
                    resource_name TEXT NOT NULL,
                    owner_run_id TEXT NOT NULL REFERENCES job_run(run_id),
                    acquired_at TEXT NOT NULL,
                    heartbeat_at TEXT NOT NULL,
                    lease_expires_at TEXT NOT NULL,
                    PRIMARY KEY(resource_name, owner_run_id)
                );
                CREATE INDEX IF NOT EXISTS resource_read_lease_expiry_idx
                    ON resource_read_lease(resource_name, lease_expires_at);
                CREATE TABLE IF NOT EXISTS scheduler_cursor (
                    scheduler_id TEXT PRIMARY KEY,
                    last_scanned_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS job_result (
                    run_id TEXT PRIMARY KEY REFERENCES job_run(run_id),
                    result_json TEXT NOT NULL,
                    recorded_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS job_service_lease (
                    service_name TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    heartbeat_at TEXT NOT NULL,
                    lease_expires_at TEXT NOT NULL,
                    last_error TEXT
                );
                CREATE INDEX IF NOT EXISTS job_service_lease_expiry_idx
                    ON job_service_lease(lease_expires_at);
                """
            )
            columns = {row["name"] for row in connection.execute("PRAGMA table_info(job_run)").fetchall()}
            if "trade_date_override" not in columns:
                connection.execute("ALTER TABLE job_run ADD COLUMN trade_date_override TEXT")
            if "parameter_overrides_json" not in columns:
                connection.execute(
                    "ALTER TABLE job_run ADD COLUMN parameter_overrides_json TEXT NOT NULL DEFAULT '{}'"
                )
            connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS job_run_target_date_idx
                ON job_run(job_id, definition_version, trade_date_override, attempt)
                WHERE trade_date_override IS NOT NULL
                """
            )
        finally:
            connection.close()

    def _run_from_row(self, row: sqlite3.Row) -> JobRun:
        return JobRun(
            run_id=row["run_id"],
            job_id=row["job_id"],
            definition_version=row["definition_version"],
            scheduled_at=_parse_timestamp(row["scheduled_at"]),  # type: ignore[arg-type]
            started_at=_parse_timestamp(row["started_at"]),
            finished_at=_parse_timestamp(row["finished_at"]),
            status=JobStatus(row["status"]),
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
            trade_date_override=date.fromisoformat(row["trade_date_override"]) if row["trade_date_override"] else None,
            parameter_overrides=_decode_parameter_overrides(row["parameter_overrides_json"]),
        )

    def get_run(self, run_id: str) -> JobRun | None:
        connection = self._connect()
        try:
            row = connection.execute("SELECT * FROM job_run WHERE run_id = ?", [run_id]).fetchone()
            return self._run_from_row(row) if row is not None else None
        finally:
            connection.close()

    def record_result(self, run_id: str, payload: Mapping[str, object], *, now: datetime | None = None) -> None:
        """Persist a structured Job result payload with the existing run evidence."""

        timestamp = now or utc_now()
        serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        with self._transaction() as connection:
            exists = connection.execute("SELECT 1 FROM job_run WHERE run_id = ?", [run_id]).fetchone()
            if exists is None:
                raise KeyError(f"unknown Job run {run_id}")
            connection.execute(
                """
                INSERT INTO job_result(run_id, result_json, recorded_at)
                VALUES (?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET result_json = excluded.result_json, recorded_at = excluded.recorded_at
                """,
                [run_id, serialized, _timestamp(timestamp)],
            )

    def get_result(self, run_id: str) -> Mapping[str, object] | None:
        connection = self._connect()
        try:
            row = connection.execute("SELECT result_json FROM job_result WHERE run_id = ?", [run_id]).fetchone()
            if row is None:
                return None
            payload = json.loads(row["result_json"])
            if not isinstance(payload, dict):
                raise RuntimeError(f"stored Job result for {run_id} is not an object")
            return payload
        finally:
            connection.close()

    def list_runs(
        self,
        *,
        job_id: str | None = None,
        status: JobStatus | None = None,
        limit: int = 100,
    ) -> list[JobRun]:
        clauses: list[str] = []
        values: list[object] = []
        if job_id is not None:
            clauses.append("job_id = ?")
            values.append(job_id)
        if status is not None:
            clauses.append("status = ?")
            values.append(status.value)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        values.append(limit)
        connection = self._connect()
        try:
            rows = connection.execute(
                f"SELECT * FROM job_run {where} ORDER BY scheduled_at DESC, attempt DESC LIMIT ?",
                values,
            ).fetchall()
            return [self._run_from_row(row) for row in rows]
        finally:
            connection.close()

    def create_scheduled_run(
        self,
        definition: JobDefinition,
        *,
        scheduled_at: datetime,
        trigger_type: str = "SCHEDULED",
        status: JobStatus = JobStatus.PENDING,
        error_summary: str | None = None,
        trade_date_override: date | None = None,
        parameter_overrides: Mapping[str, Any] | None = None,
    ) -> tuple[JobRun, bool]:
        """Insert the first attempt once; concurrent scans return the same row."""

        if status not in {JobStatus.PENDING, JobStatus.BLOCKED, JobStatus.SKIPPED}:
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
                trade_date_override=trade_date_override,
                parameter_overrides=parameter_overrides,
                created_at=now,
            )

    def _create_scheduled_run_in_transaction(
        self,
        connection: sqlite3.Connection,
        definition: JobDefinition,
        *,
        scheduled_at: datetime,
        trigger_type: str,
        status: JobStatus,
        error_summary: str | None,
        trade_date_override: date | None,
        parameter_overrides: Mapping[str, Any] | None,
        created_at: datetime,
    ) -> tuple[JobRun, bool]:
        scheduled_text = _timestamp(scheduled_at)
        parameter_overrides_json = _encode_parameter_overrides(parameter_overrides)
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO job_run (
                run_id, job_id, definition_version, scheduled_at, status, attempt,
                timed_out, trigger_type, error_summary, trade_date_override,
                parameter_overrides_json, created_at
            ) VALUES (?, ?, ?, ?, ?, 1, 0, ?, ?, ?, ?, ?)
            """,
            [
                str(uuid.uuid4()),
                definition.job_id,
                definition.definition_version,
                scheduled_text,
                status.value,
                trigger_type,
                error_summary,
                trade_date_override.isoformat() if trade_date_override is not None else None,
                parameter_overrides_json,
                _timestamp(created_at),
            ],
        )
        if cursor.rowcount == 1 or trade_date_override is None:
            row = connection.execute(
                "SELECT * FROM job_run WHERE job_id = ? AND scheduled_at = ? AND attempt = 1",
                [definition.job_id, scheduled_text],
            ).fetchone()
        else:
            row = connection.execute(
                """
                SELECT * FROM job_run
                WHERE job_id = ? AND definition_version = ?
                  AND trade_date_override = ? AND attempt = 1
                ORDER BY created_at ASC
                LIMIT 1
                """,
                [definition.job_id, definition.definition_version, trade_date_override.isoformat()],
            ).fetchone()
        if row is None:
            raise RuntimeError("failed to create or retrieve scheduled run")
        return self._run_from_row(row), cursor.rowcount == 1

    def get_scheduler_cursor(self, scheduler_id: str) -> JobSchedulerCursor | None:
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
            return JobSchedulerCursor(
                scheduler_id=row["scheduler_id"],
                last_scanned_at=last_scanned_at,
                created_at=created_at,
                updated_at=updated_at,
            )
        finally:
            connection.close()

    @staticmethod
    def _service_lease_from_row(row: sqlite3.Row) -> JobServiceLease:
        started_at = _parse_timestamp(row["started_at"])
        heartbeat_at = _parse_timestamp(row["heartbeat_at"])
        lease_expires_at = _parse_timestamp(row["lease_expires_at"])
        if started_at is None or heartbeat_at is None or lease_expires_at is None:
            raise RuntimeError("Job service lease contains an invalid timestamp")
        return JobServiceLease(
            service_name=row["service_name"],
            owner_id=row["owner_id"],
            started_at=started_at,
            heartbeat_at=heartbeat_at,
            lease_expires_at=lease_expires_at,
            last_error=row["last_error"],
        )

    def get_service_lease(self, service_name: str) -> JobServiceLease | None:
        """Return the live or expired service lease without changing it."""

        self.initialize()
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM job_service_lease WHERE service_name = ?", [service_name]
            ).fetchone()
            return self._service_lease_from_row(row) if row is not None else None
        finally:
            connection.close()

    def claim_service_lease(
        self,
        *,
        service_name: str,
        owner_id: str,
        lease_seconds: int,
        now: datetime | None = None,
    ) -> JobServiceLease:
        """Atomically claim the scheduler-service lease or fail without takeover."""

        if not service_name.strip() or not owner_id.strip():
            raise ValueError("service_name and owner_id must be non-empty")
        if lease_seconds <= 0:
            raise ValueError("service lease_seconds must be positive")
        timestamp = now or utc_now()
        expires_at = timestamp + timedelta(seconds=lease_seconds)
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM job_service_lease WHERE service_name = ?", [service_name]
            ).fetchone()
            if row is not None:
                existing = self._service_lease_from_row(row)
                if existing.lease_expires_at > timestamp and existing.owner_id != owner_id:
                    raise JobClaimFailure("SCHEDULER_SERVICE_ACTIVE", existing.owner_id)
                connection.execute(
                    """
                    UPDATE job_service_lease
                    SET owner_id = ?, started_at = ?, heartbeat_at = ?, lease_expires_at = ?, last_error = NULL
                    WHERE service_name = ?
                    """,
                    [
                        owner_id,
                        _timestamp(timestamp),
                        _timestamp(timestamp),
                        _timestamp(expires_at),
                        service_name,
                    ],
                )
            else:
                connection.execute(
                    """
                    INSERT INTO job_service_lease(
                        service_name, owner_id, started_at, heartbeat_at, lease_expires_at, last_error
                    ) VALUES (?, ?, ?, ?, ?, NULL)
                    """,
                    [
                        service_name,
                        owner_id,
                        _timestamp(timestamp),
                        _timestamp(timestamp),
                        _timestamp(expires_at),
                    ],
                )
            updated = connection.execute(
                "SELECT * FROM job_service_lease WHERE service_name = ?", [service_name]
            ).fetchone()
            if updated is None:
                raise RuntimeError("service lease disappeared while being claimed")
            return self._service_lease_from_row(updated)

    def heartbeat_service_lease(
        self,
        *,
        service_name: str,
        owner_id: str,
        lease_seconds: int,
        last_error: str | None = None,
        now: datetime | None = None,
    ) -> bool:
        """Renew only the owning service's lease; a false return loses leadership."""

        if lease_seconds <= 0:
            raise ValueError("service lease_seconds must be positive")
        timestamp = now or utc_now()
        with self._transaction() as connection:
            updated = connection.execute(
                """
                UPDATE job_service_lease
                SET heartbeat_at = ?, lease_expires_at = ?, last_error = ?
                WHERE service_name = ? AND owner_id = ? AND lease_expires_at > ?
                """,
                [
                    _timestamp(timestamp),
                    _timestamp(timestamp + timedelta(seconds=lease_seconds)),
                    last_error,
                    service_name,
                    owner_id,
                    _timestamp(timestamp),
                ],
            ).rowcount
            return updated == 1

    def release_service_lease(self, *, service_name: str, owner_id: str) -> bool:
        """Release a lease during graceful shutdown without touching another owner."""

        with self._transaction() as connection:
            deleted = connection.execute(
                "DELETE FROM job_service_lease WHERE service_name = ? AND owner_id = ?",
                [service_name, owner_id],
            ).rowcount
            return deleted == 1

    def commit_scheduler_scan(
        self,
        *,
        scheduler_id: str,
        expected_last_scanned_at: datetime | None,
        scanned_through_at: datetime,
        candidates: Iterable[tuple[JobDefinition, datetime, JobStatus, str | None]],
        now: datetime | None = None,
    ) -> list[JobRun] | None:
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
            created: list[JobRun] = []
            for definition, scheduled_at, status, error_summary in candidates:
                run, inserted = self._create_scheduled_run_in_transaction(
                    connection,
                    definition,
                    scheduled_at=scheduled_at,
                    trigger_type="SCHEDULED",
                    status=status,
                    error_summary=error_summary,
                    trade_date_override=None,
                    parameter_overrides=None,
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

    def latest_run_before(self, job_id: str, scheduled_at: datetime) -> JobRun | None:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT * FROM job_run
                WHERE job_id = ? AND scheduled_at <= ?
                ORDER BY scheduled_at DESC, attempt DESC LIMIT 1
                """,
                [job_id, _timestamp(scheduled_at)],
            ).fetchone()
            return self._run_from_row(row) if row is not None else None
        finally:
            connection.close()

    def has_active_job_run(self, job_id: str) -> bool:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT 1 FROM job_run WHERE job_id = ? AND status IN (?, ?) LIMIT 1",
                [job_id, JobStatus.PENDING.value, JobStatus.RUNNING.value],
            ).fetchone()
            return row is not None
        finally:
            connection.close()

    def has_active_resource_lock(self, resource_name: str, *, now: datetime | None = None) -> bool:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT resource_name FROM resource_lock WHERE lease_expires_at > ?",
                [_timestamp(now or utc_now())],
            ).fetchall()
            return any(_resources_conflict(resource_name, row["resource_name"]) for row in rows)
        finally:
            connection.close()

    def has_active_resource_read_lease(self, resource_name: str, *, now: datetime | None = None) -> bool:
        """Return whether a live shared reader prevents an exclusive writer."""

        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT resource_name FROM resource_read_lease WHERE lease_expires_at > ?",
                [_timestamp(now or utc_now())],
            ).fetchall()
            return any(_resources_conflict(resource_name, row["resource_name"]) for row in rows)
        finally:
            connection.close()

    def has_active_service_lease(self, *, now: datetime | None = None) -> bool:
        """True when any live scheduler service owns this runtime database."""

        self.initialize()
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT 1 FROM job_service_lease WHERE lease_expires_at > ? LIMIT 1",
                [_timestamp(now or utc_now())],
            ).fetchone()
            return row is not None
        finally:
            connection.close()

    def claim_run(
        self,
        run_id: str,
        *,
        job_id: str,
        definition_version: str,
        overlap_policy: OverlapPolicy,
        resource_locks: Iterable[str],
        resource_reads: Iterable[str] = (),
        stdout_path: Path | None = None,
        stderr_path: Path | None = None,
        lease_seconds: int,
        now: datetime | None = None,
    ) -> JobRun:
        """Atomically validate, claim, and lease one PENDING run.

        Every rejection raises :class:`JobClaimFailure` while the surrounding
        ``BEGIN IMMEDIATE`` transaction rolls back. Scheduler eligibility
        checks are only advisory; this is the final concurrency gate.
        """

        self.initialize()
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        timestamp = now or utc_now()
        expires = timestamp + timedelta(seconds=lease_seconds)
        lock_names = tuple(sorted(set(resource_locks)))
        read_names = tuple(sorted(set(resource_reads) - set(lock_names)))
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM job_run WHERE run_id = ?", [run_id]).fetchone()
            if row is None:
                raise JobClaimFailure("RUN_NOT_FOUND", run_id)
            if JobStatus(row["status"]) is not JobStatus.PENDING:
                raise JobClaimFailure("RUN_NOT_PENDING", JobStatus(row["status"]).value)
            if row["job_id"] != job_id:
                raise JobClaimFailure("JOB_ID_MISMATCH", row["job_id"])
            if row["definition_version"] != definition_version:
                raise JobClaimFailure("DEFINITION_VERSION_MISMATCH", row["definition_version"])
            if overlap_policy is OverlapPolicy.FORBID:
                overlap = connection.execute(
                    """
                    SELECT run_id FROM job_run
                    WHERE job_id = ? AND status = ? AND run_id != ?
                    LIMIT 1
                    """,
                    [job_id, JobStatus.RUNNING.value, run_id],
                ).fetchone()
                if overlap is not None:
                    raise JobClaimFailure("OVERLAP_FORBIDDEN", overlap["run_id"])
            connection.execute("DELETE FROM resource_lock WHERE lease_expires_at <= ?", [_timestamp(timestamp)])
            connection.execute("DELETE FROM resource_read_lease WHERE lease_expires_at <= ?", [_timestamp(timestamp)])
            if lock_names:
                active_writers = connection.execute(
                    "SELECT resource_name FROM resource_lock WHERE lease_expires_at > ?",
                    [_timestamp(timestamp)],
                ).fetchall()
                conflict = next(
                    (
                        row["resource_name"]
                        for requested in lock_names
                        for row in active_writers
                        if _resources_conflict(requested, row["resource_name"])
                    ),
                    None,
                )
                if conflict is not None:
                    raise JobClaimFailure("RESOURCE_LOCK_UNAVAILABLE", conflict)
                active_readers = connection.execute(
                    "SELECT resource_name FROM resource_read_lease WHERE lease_expires_at > ?",
                    [_timestamp(timestamp)],
                ).fetchall()
                reader_conflict = next(
                    (
                        row["resource_name"]
                        for requested in lock_names
                        for row in active_readers
                        if _resources_conflict(requested, row["resource_name"])
                    ),
                    None,
                )
                if reader_conflict is not None:
                    raise JobClaimFailure("RESOURCE_READERS_ACTIVE", reader_conflict)
            if read_names:
                active_writers = connection.execute(
                    "SELECT resource_name FROM resource_lock WHERE lease_expires_at > ?",
                    [_timestamp(timestamp)],
                ).fetchall()
                writer_conflict = next(
                    (
                        row["resource_name"]
                        for requested in read_names
                        for row in active_writers
                        if _resources_conflict(requested, row["resource_name"])
                    ),
                    None,
                )
                if writer_conflict is not None:
                    raise JobClaimFailure("RESOURCE_WRITER_ACTIVE", writer_conflict)
            assert_status_transition(JobStatus.PENDING, JobStatus.RUNNING)
            connection.execute(
                """
                UPDATE job_run
                SET status = ?, started_at = ?, heartbeat_at = ?, stdout_path = ?, stderr_path = ?
                WHERE run_id = ?
                """,
                [
                    JobStatus.RUNNING.value,
                    _timestamp(timestamp),
                    _timestamp(timestamp),
                    str(stdout_path) if stdout_path is not None else None,
                    str(stderr_path) if stderr_path is not None else None,
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
            for resource_name in read_names:
                connection.execute(
                    """
                    INSERT INTO resource_read_lease(resource_name, owner_run_id, acquired_at, heartbeat_at, lease_expires_at)
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
            claimed = connection.execute("SELECT * FROM job_run WHERE run_id = ?", [run_id]).fetchone()
            return self._run_from_row(claimed) if claimed is not None else None

    def heartbeat(self, run_id: str, *, lease_seconds: int, now: datetime | None = None) -> bool:
        timestamp = now or utc_now()
        with self._transaction() as connection:
            cursor = connection.execute(
                "UPDATE job_run SET heartbeat_at = ? WHERE run_id = ? AND status = ?",
                [_timestamp(timestamp), run_id, JobStatus.RUNNING.value],
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
            connection.execute(
                """
                UPDATE resource_read_lease SET heartbeat_at = ?, lease_expires_at = ?
                WHERE owner_run_id = ?
                """,
                [_timestamp(timestamp), _timestamp(timestamp + timedelta(seconds=lease_seconds)), run_id],
            )
            return True

    def finish_run(
        self,
        run_id: str,
        *,
        status: JobStatus,
        exit_code: int | None,
        timed_out: bool,
        error_summary: str | None,
        wall_duration_ms: int,
        user_cpu_ms: int | None,
        system_cpu_ms: int | None,
        peak_rss_kb: int | None,
        now: datetime | None = None,
    ) -> JobRun:
        if status not in {
            JobStatus.SUCCESS,
            JobStatus.FAILED,
            JobStatus.TIMED_OUT,
            JobStatus.CANCELLED,
        }:
            raise ValueError("finish_run requires a terminal execution status")
        timestamp = now or utc_now()
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM job_run WHERE run_id = ?", [run_id]).fetchone()
            if row is None:
                raise KeyError(f"unknown Job run {run_id}")
            current = JobStatus(row["status"])
            assert_status_transition(current, status)
            connection.execute(
                """
                UPDATE job_run
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
                    _bounded_error_summary(error_summary),
                    wall_duration_ms,
                    user_cpu_ms,
                    system_cpu_ms,
                    peak_rss_kb,
                    run_id,
                ],
            )
            connection.execute("DELETE FROM resource_lock WHERE owner_run_id = ?", [run_id])
            connection.execute("DELETE FROM resource_read_lease WHERE owner_run_id = ?", [run_id])
            result = connection.execute("SELECT * FROM job_run WHERE run_id = ?", [run_id]).fetchone()
            if result is None:
                raise RuntimeError("run disappeared while being finalized")
            return self._run_from_row(result)

    def retry_run(
        self,
        run_id: str,
        *,
        max_retries: int | None = None,
        now: datetime | None = None,
    ) -> JobRun:
        """Create another attempt without changing the failed run evidence."""

        timestamp = now or utc_now()
        with self._transaction() as connection:
            previous = connection.execute("SELECT * FROM job_run WHERE run_id = ?", [run_id]).fetchone()
            if previous is None:
                raise KeyError(f"unknown Job run {run_id}")
            if JobStatus(previous["status"]) not in {JobStatus.FAILED, JobStatus.TIMED_OUT}:
                raise ValueError("only FAILED or TIMED_OUT runs may be retried")
            next_attempt = connection.execute(
                "SELECT COALESCE(MAX(attempt), 0) + 1 FROM job_run WHERE job_id = ? AND scheduled_at = ?",
                [previous["job_id"], previous["scheduled_at"]],
            ).fetchone()[0]
            if max_retries is not None and next_attempt > max_retries + 1:
                raise ValueError(f"max_retries={max_retries} exhausted for {previous['job_id']}")
            retry_id = str(uuid.uuid4())
            connection.execute(
                """
                INSERT INTO job_run (
                    run_id, job_id, definition_version, scheduled_at, status, attempt,
                    timed_out, trigger_type, retry_of_run_id, trade_date_override,
                    parameter_overrides_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)
                """,
                [
                    retry_id,
                    previous["job_id"],
                    previous["definition_version"],
                    previous["scheduled_at"],
                    JobStatus.PENDING.value,
                    next_attempt,
                    "RETRY",
                    run_id,
                    previous["trade_date_override"],
                    previous["parameter_overrides_json"] or "{}",
                    _timestamp(timestamp),
                ],
            )
            row = connection.execute("SELECT * FROM job_run WHERE run_id = ?", [retry_id]).fetchone()
            if row is None:
                raise RuntimeError("retry run was not created")
            return self._run_from_row(row)

    def unblock_run(self, run_id: str) -> JobRun | None:
        """Return a still-relevant BLOCKED record to PENDING without duplicating it."""

        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM job_run WHERE run_id = ?", [run_id]).fetchone()
            if row is None or JobStatus(row["status"]) is not JobStatus.BLOCKED:
                return None
            assert_status_transition(JobStatus.BLOCKED, JobStatus.PENDING)
            connection.execute(
                "UPDATE job_run SET status = ?, error_summary = NULL WHERE run_id = ?",
                [JobStatus.PENDING.value, run_id],
            )
            result = connection.execute("SELECT * FROM job_run WHERE run_id = ?", [run_id]).fetchone()
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
                SELECT run_id FROM job_run
                WHERE status = ? AND (heartbeat_at IS NULL OR heartbeat_at < ?)
                """,
                [JobStatus.RUNNING.value, _timestamp(cutoff)],
            ).fetchall()
            for row in stale:
                connection.execute(
                    """
                    UPDATE job_run
                    SET status = ?, finished_at = ?, error_summary = ?
                    WHERE run_id = ?
                    """,
                    [
                        JobStatus.FAILED.value,
                        _timestamp(timestamp),
                        "stale heartbeat recovery",
                        row["run_id"],
                    ],
                )
                connection.execute("DELETE FROM resource_lock WHERE owner_run_id = ?", [row["run_id"]])
                connection.execute("DELETE FROM resource_read_lease WHERE owner_run_id = ?", [row["run_id"]])
            deleted_writes = connection.execute(
                "DELETE FROM resource_lock WHERE lease_expires_at <= ?", [_timestamp(timestamp)]
            ).rowcount
            deleted_reads = connection.execute(
                "DELETE FROM resource_read_lease WHERE lease_expires_at <= ?", [_timestamp(timestamp)]
            ).rowcount
            return len(stale), max(deleted_writes, 0) + max(deleted_reads, 0)

    def start_stage(
        self,
        run_id: str,
        stage_name: str,
        *,
        input_rows: int | None = None,
        metadata: Mapping[str, object] | None = None,
        now: datetime | None = None,
    ) -> JobStageRun:
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
                    JobStatus.RUNNING.value,
                    input_rows,
                    json.dumps(metadata or {}, sort_keys=True),
                ],
            )
        return JobStageRun(
            run_id=run_id,
            stage_name=stage_name,
            started_at=timestamp,
            finished_at=None,
            duration_ms=None,
            status=JobStatus.RUNNING,
            input_rows=input_rows,
            output_rows=None,
            metadata=metadata or {},
        )

    def finish_stage(
        self,
        run_id: str,
        stage_name: str,
        *,
        status: JobStatus,
        output_rows: int | None = None,
        metadata: Mapping[str, object] | None = None,
        now: datetime | None = None,
    ) -> JobStageRun:
        if status not in {JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.SKIPPED}:
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
        return JobStageRun(
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
