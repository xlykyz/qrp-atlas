"""Non-destructive runtime initialization and deployment diagnostics."""

from __future__ import annotations

import os
import platform
import sqlite3
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from qrp_atlas.config.settings import AppSettings, AuthMode, RuntimeEnvironment
from qrp_atlas.contracts import (
    ALL_TABLES,
    IRM_INTERACTION_QA,
    SYSTEM_B_EPISODE_OBSERVATION_TABLE,
    SYSTEM_B_EPISODE_TABLE,
    SYSTEM_B_POOL_MEMBERSHIP_TABLE,
    SYSTEM_B_POOL_RUN_TABLE,
)


class CheckLevel(StrEnum):
    OK = "ok"
    WARNING = "warning"
    FAILURE = "failure"


@dataclass(frozen=True, slots=True)
class CheckResult:
    level: CheckLevel
    name: str
    message: str


class InitStatus(StrEnum):
    CREATED = "created"
    EXISTS = "exists"
    SKIPPED = "skipped"
    FAILURE = "failure"


@dataclass(frozen=True, slots=True)
class InitResult:
    status: InitStatus
    path: Path
    message: str


@dataclass(frozen=True, slots=True)
class DatabaseAuditResult:
    """Read-only schema and path evidence for one configured database."""

    level: CheckLevel
    database_id: str
    backend: str
    path: Path | None
    source: str
    expected_tables: tuple[str, ...]
    actual_tables: tuple[str, ...]
    missing_tables: tuple[str, ...]
    message: str

    def as_dict(self) -> dict[str, object]:
        return {
            "level": self.level.value,
            "database_id": self.database_id,
            "backend": self.backend,
            "path": str(self.path) if self.path is not None else None,
            "source": self.source,
            "expected_tables": list(self.expected_tables),
            "actual_tables": list(self.actual_tables),
            "missing_tables": list(self.missing_tables),
            "message": self.message,
        }


def _unique_paths(paths: tuple[Path, ...]) -> tuple[Path, ...]:
    seen: set[Path] = set()
    result: list[Path] = []
    for path in paths:
        if path not in seen:
            seen.add(path)
            result.append(path)
    return tuple(result)


_MAIN_DATABASE_TABLES = tuple(
    table.name for table in ALL_TABLES if table is not IRM_INTERACTION_QA
)
_IRM_DATABASE_TABLES = (IRM_INTERACTION_QA.name,)
_EPISODE_DATABASE_TABLES = (
    SYSTEM_B_EPISODE_TABLE,
    SYSTEM_B_EPISODE_OBSERVATION_TABLE,
)
_POOL_DATABASE_TABLES = (
    SYSTEM_B_POOL_MEMBERSHIP_TABLE,
    SYSTEM_B_POOL_RUN_TABLE,
)
_JOB_RUNTIME_TABLES = (
    "job_run",
    "stage_run",
    "resource_lock",
    "resource_read_lease",
    "scheduler_cursor",
    "job_result",
    "job_service_lease",
)


def _audit_duckdb(
    database_id: str,
    path: Path | None,
    *,
    source: str,
    expected_tables: tuple[str, ...],
    required: bool,
) -> DatabaseAuditResult:
    if path is None:
        return DatabaseAuditResult(
            CheckLevel.WARNING,
            database_id,
            "duckdb",
            None,
            source,
            expected_tables,
            (),
            expected_tables,
            "database path is not configured",
        )
    if not path.exists():
        level = CheckLevel.FAILURE if required else CheckLevel.WARNING
        return DatabaseAuditResult(
            level,
            database_id,
            "duckdb",
            path,
            source,
            expected_tables,
            (),
            expected_tables,
            "database file does not exist",
        )
    if not path.is_file():
        return DatabaseAuditResult(
            CheckLevel.FAILURE,
            database_id,
            "duckdb",
            path,
            source,
            expected_tables,
            (),
            expected_tables,
            "database path is not a regular file",
        )

    try:
        import duckdb

        connection = duckdb.connect(str(path), read_only=True)
        try:
            actual_tables = tuple(
                row[0]
                for row in connection.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'main' ORDER BY table_name"
                ).fetchall()
            )
        finally:
            connection.close()
    except Exception as exc:
        return DatabaseAuditResult(
            CheckLevel.FAILURE,
            database_id,
            "duckdb",
            path,
            source,
            expected_tables,
            (),
            expected_tables,
            f"database could not be opened read-only: {type(exc).__name__}",
        )

    missing_tables = tuple(table for table in expected_tables if table not in actual_tables)
    level = CheckLevel.FAILURE if missing_tables else CheckLevel.OK
    message = (
        f"readable; missing tables: {', '.join(missing_tables)}"
        if missing_tables
        else f"readable; {len(actual_tables)} tables present"
    )
    return DatabaseAuditResult(
        level,
        database_id,
        "duckdb",
        path,
        source,
        expected_tables,
        actual_tables,
        missing_tables,
        message,
    )


def _audit_sqlite(
    database_id: str,
    path: Path,
    *,
    source: str,
    expected_tables: tuple[str, ...],
) -> DatabaseAuditResult:
    if not path.exists():
        return DatabaseAuditResult(
            CheckLevel.WARNING,
            database_id,
            "sqlite",
            path,
            source,
            expected_tables,
            (),
            expected_tables,
            "runtime database file does not exist yet",
        )
    if not path.is_file():
        return DatabaseAuditResult(
            CheckLevel.FAILURE,
            database_id,
            "sqlite",
            path,
            source,
            expected_tables,
            (),
            expected_tables,
            "runtime database path is not a regular file",
        )

    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            actual_tables = tuple(
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type = 'table' ORDER BY name"
                ).fetchall()
            )
        finally:
            connection.close()
    except Exception as exc:
        return DatabaseAuditResult(
            CheckLevel.FAILURE,
            database_id,
            "sqlite",
            path,
            source,
            expected_tables,
            (),
            expected_tables,
            f"runtime database could not be opened read-only: {type(exc).__name__}",
        )

    missing_tables = tuple(table for table in expected_tables if table not in actual_tables)
    level = CheckLevel.FAILURE if missing_tables else CheckLevel.OK
    message = (
        f"readable; missing tables: {', '.join(missing_tables)}"
        if missing_tables
        else f"readable; {len(actual_tables)} tables present"
    )
    return DatabaseAuditResult(
        level,
        database_id,
        "sqlite",
        path,
        source,
        expected_tables,
        actual_tables,
        missing_tables,
        message,
    )


def audit_databases(settings: AppSettings) -> list[DatabaseAuditResult]:
    """Audit configured database paths and required contract tables read-only."""

    sources = settings.sources
    return [
        _audit_duckdb(
            "canonical",
            settings.paths.duckdb_path,
            source=sources.get("QRP_DUCKDB_PATH", "default"),
            expected_tables=_MAIN_DATABASE_TABLES,
            required=True,
        ),
        _audit_duckdb(
            "irm_qa",
            settings.paths.irm_qa_duckdb_path,
            source=sources.get("QRP_IRM_QA_DUCKDB_PATH", "default"),
            expected_tables=_IRM_DATABASE_TABLES,
            required=False,
        ),
        _audit_duckdb(
            "system_b_episode",
            settings.paths.episode_db_path,
            source=sources.get("QRP_EPISODE_DB_PATH", "default"),
            expected_tables=_EPISODE_DATABASE_TABLES,
            required=settings.paths.episode_db_path is not None,
        ),
        _audit_duckdb(
            "system_b_pools",
            settings.paths.pool_db_path,
            source=sources.get("QRP_POOL_DB_PATH", "default"),
            expected_tables=_POOL_DATABASE_TABLES,
            required=settings.paths.pool_db_path is not None,
        ),
        _audit_sqlite(
            "job_runtime",
            settings.paths.job_runtime_db_path,
            source=sources.get("QRP_JOB_RUNTIME_DB_PATH", "default"),
            expected_tables=_JOB_RUNTIME_TABLES,
        ),
    ]


def database_cleanup_candidates(settings: AppSettings) -> tuple[Path, ...]:
    """List stale database-looking files without deleting or changing them."""

    active_paths = {
        path.resolve(strict=False)
        for path in (
            settings.paths.duckdb_path,
            settings.paths.irm_qa_duckdb_path,
            settings.paths.episode_db_path,
            settings.paths.pool_db_path,
            settings.paths.job_runtime_db_path,
        )
        if path is not None
    }
    candidates: set[Path] = set()
    stale_paths = (
        settings.paths.data_dir / "quant.db",
        settings.paths.canonical_dir / "quant.db",
    )
    for path in stale_paths:
        if path.is_file() and path.resolve(strict=False) not in active_paths:
            candidates.add(path)

    db_dir = settings.paths.db_dir
    if db_dir.is_dir():
        for path in db_dir.iterdir():
            if not path.is_file() or path.resolve(strict=False) in active_paths:
                continue
            lower_name = path.name.lower()
            if (
                "backup" in lower_name
                or lower_name.endswith((".bak", ".old", ".orig", "~"))
            ) and "schema_migration_backup_" not in lower_name:
                candidates.add(path)
    return tuple(sorted(candidates))


def initialize_runtime(settings: AppSettings) -> list[InitResult]:
    """Create configured directories without creating or replacing databases."""

    directories = _unique_paths(
        settings.paths.persistent_directories()
        + settings.paths.runtime_directories()
    )
    results: list[InitResult] = []
    for path in directories:
        if path.exists():
            if path.is_dir():
                results.append(InitResult(InitStatus.EXISTS, path, "directory exists"))
            else:
                results.append(
                    InitResult(InitStatus.FAILURE, path, "path exists but is not a directory")
                )
            continue
        if settings.runtime.read_only:
            results.append(
                InitResult(
                    InitStatus.FAILURE,
                    path,
                    "read-only mode forbids creating the missing directory",
                )
            )
            continue
        try:
            path.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            results.append(
                InitResult(InitStatus.FAILURE, path, f"could not create directory: {exc}")
            )
        else:
            results.append(InitResult(InitStatus.CREATED, path, "directory created"))

    database = settings.paths.duckdb_path
    if database.exists():
        status = InitStatus.EXISTS if database.is_file() else InitStatus.FAILURE
        message = "database preserved" if database.is_file() else "database path is not a file"
    else:
        status = InitStatus.SKIPPED
        message = "database not created; use the existing schema initializer when needed"
    results.append(InitResult(status, database, message))

    irm_qa_database = settings.paths.irm_qa_duckdb_path
    if irm_qa_database.exists():
        status = (
            InitStatus.EXISTS
            if irm_qa_database.is_file()
            else InitStatus.FAILURE
        )
        message = (
            "IRM database preserved"
            if irm_qa_database.is_file()
            else "IRM database path is not a file"
        )
    else:
        status = InitStatus.SKIPPED
        message = (
            "IRM database not created; create it with the IRM migration tool "
            "or schema initializer when needed"
        )
    results.append(InitResult(status, irm_qa_database, message))
    return results


def _nearest_existing_parent(path: Path) -> Path | None:
    candidate = path
    while not candidate.exists() and candidate != candidate.parent:
        candidate = candidate.parent
    return candidate if candidate.exists() else None


def _check_directory(path: Path, *, read_only: bool) -> CheckResult:
    if path.exists():
        if not path.is_dir():
            return CheckResult(CheckLevel.FAILURE, "directory", f"not a directory: {path}")
        if not os.access(path, os.R_OK):
            return CheckResult(CheckLevel.FAILURE, "directory", f"not readable: {path}")
        if not read_only and not os.access(path, os.W_OK):
            return CheckResult(CheckLevel.FAILURE, "directory", f"not writable: {path}")
        return CheckResult(CheckLevel.OK, "directory", f"available: {path}")

    if read_only:
        return CheckResult(
            CheckLevel.FAILURE,
            "directory",
            f"missing in read-only mode: {path}",
        )
    parent = _nearest_existing_parent(path.parent)
    if parent is None or not parent.is_dir() or not os.access(parent, os.W_OK):
        return CheckResult(
            CheckLevel.FAILURE,
            "directory",
            f"missing and parent is not writable: {path}",
        )
    return CheckResult(CheckLevel.OK, "directory", f"missing but creatable: {path}")


def doctor(settings: AppSettings) -> list[CheckResult]:
    """Inspect effective configuration without creating or modifying files."""

    results: list[CheckResult] = [
        CheckResult(
            CheckLevel.OK,
            "platform",
            f"{platform.system()} {platform.release()} ({os.name})",
        ),
        CheckResult(
            CheckLevel.OK,
            "configuration",
            "configuration parsed successfully",
        ),
    ]

    directories = _unique_paths(
        settings.paths.persistent_directories()
        + settings.paths.runtime_directories()
    )
    results.extend(
        _check_directory(path, read_only=settings.runtime.read_only)
        for path in directories
    )

    database = settings.paths.duckdb_path
    if database.exists() and not database.is_file():
        results.append(
            CheckResult(
                CheckLevel.FAILURE,
                "duckdb",
                f"database path is not a file: {database}",
            )
        )
    elif not database.exists():
        level = CheckLevel.FAILURE if settings.database.read_only else CheckLevel.WARNING
        results.append(
            CheckResult(
                level,
                "duckdb",
                f"database file does not exist: {database}",
            )
        )
    else:
        try:
            import duckdb

            connection = duckdb.connect(str(database), read_only=True)
            try:
                connection.execute("SELECT 1").fetchone()
            finally:
                connection.close()
        except Exception as exc:
            results.append(
                CheckResult(
                    CheckLevel.FAILURE,
                    "duckdb",
                    f"database could not be opened read-only: {type(exc).__name__}",
                )
            )
        else:
            results.append(
                CheckResult(CheckLevel.OK, "duckdb", f"database is readable: {database}")
            )

    irm_qa_database = settings.paths.irm_qa_duckdb_path
    if irm_qa_database.exists() and not irm_qa_database.is_file():
        results.append(
            CheckResult(
                CheckLevel.FAILURE,
                "irm_qa_duckdb",
                f"IRM database path is not a file: {irm_qa_database}",
            )
        )
    elif not irm_qa_database.exists():
        results.append(
            CheckResult(
                CheckLevel.WARNING,
                "irm_qa_duckdb",
                f"IRM database file does not exist: {irm_qa_database}",
            )
        )
    else:
        try:
            import duckdb

            connection = duckdb.connect(str(irm_qa_database), read_only=True)
            try:
                connection.execute("SELECT 1").fetchone()
            finally:
                connection.close()
        except Exception as exc:
            results.append(
                CheckResult(
                    CheckLevel.FAILURE,
                    "irm_qa_duckdb",
                    f"IRM database could not be opened read-only: {type(exc).__name__}",
                )
            )
        else:
            results.append(
                CheckResult(
                    CheckLevel.OK,
                    "irm_qa_duckdb",
                    f"IRM database is readable: {irm_qa_database}",
                )
            )

    auth = settings.authentication
    if auth.mode is AuthMode.DATABASE:
        results.append(
            CheckResult(
                CheckLevel.OK,
                "authentication",
                "database mode has a configured PostgreSQL DSN",
            )
        )
    else:
        level = (
            CheckLevel.WARNING
            if settings.runtime.environment is RuntimeEnvironment.PRODUCTION
            else CheckLevel.OK
        )
        results.append(
            CheckResult(level, "authentication", "local authentication mode is active")
        )

    if settings.external_services.tushare_token:
        results.append(
            CheckResult(CheckLevel.OK, "tushare", "TUSHARE_TOKEN is configured")
        )
    else:
        results.append(
            CheckResult(
                CheckLevel.WARNING,
                "tushare",
                "TUSHARE_TOKEN is not configured; Tushare pipelines are unavailable",
            )
        )

    if (
        settings.runtime.environment is RuntimeEnvironment.PRODUCTION
        and "*" in settings.api.cors_origins
    ):
        results.append(
            CheckResult(
                CheckLevel.WARNING,
                "api",
                "wildcard CORS is enabled in production",
            )
        )
    else:
        results.append(
            CheckResult(
                CheckLevel.OK,
                "api",
                f"API endpoint configured for {settings.api.host}:{settings.api.port}",
            )
        )
    return results


def has_failures(results: list[CheckResult] | list[InitResult]) -> bool:
    return any(
        getattr(item, "level", None) is CheckLevel.FAILURE
        or getattr(item, "status", None) is InitStatus.FAILURE
        for item in results
    )
