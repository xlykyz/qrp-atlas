"""DuckDB connection helpers backed by unified runtime settings."""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb
from fastapi import HTTPException

from qrp_atlas.config.settings import AppSettings, get_settings

logger = logging.getLogger(__name__)

_AUXILIARY_DATABASES = frozenset({"episode_db", "pool_db", "irm_qa_db"})
_AUXILIARY_DATABASE_DETAILS = {
    "episode_db": "EPISODE_DB_NOT_AVAILABLE",
    "pool_db": "POOL_DB_NOT_AVAILABLE",
    "irm_qa_db": "IRM_QA_DB_NOT_AVAILABLE",
}


def attach_readonly_database(
    connection: duckdb.DuckDBPyConnection,
    alias: str,
    db_path: Path | None,
) -> str:
    """ATTACH one configured auxiliary DuckDB file in read-only mode.

    Missing configuration, missing files, and DuckDB ATTACH failures are
    explicit API availability errors.  They must not be treated as empty
    auxiliary datasets.
    """
    if alias not in _AUXILIARY_DATABASES:
        raise ValueError(f"unsupported auxiliary database alias: {alias}")
    detail = _AUXILIARY_DATABASE_DETAILS[alias]
    if db_path is None or not db_path.is_file():
        logger.warning("Auxiliary database %s is unavailable at %s", alias, db_path)
        raise HTTPException(status_code=503, detail=detail)
    try:
        safe_path = str(db_path).replace("'", "''")
        connection.execute(f"ATTACH '{safe_path}' AS {alias} (READ_ONLY)")
        logger.debug("ATTACH'd %s from %s", alias, db_path)
    except Exception as exc:
        logger.warning("Failed to ATTACH %s from %s: %s", alias, db_path, exc)
        raise HTTPException(status_code=503, detail=detail) from exc
    return alias


def detach_database_if_attached(
    connection: duckdb.DuckDBPyConnection,
    alias: str,
) -> None:
    """DETACH a database mounted by this request.

    Cleanup must not replace an exception raised by the business query.  The
    connection is closed by the caller even when DuckDB reports a cleanup
    error, and the failure is left in the service log for diagnosis.
    """
    if alias not in _AUXILIARY_DATABASES:
        raise ValueError(f"unsupported auxiliary database alias: {alias}")
    try:
        connection.execute(f"DETACH {alias}")
    except Exception:
        logger.exception("Failed to DETACH auxiliary database %s", alias)


def require_episode_db(connection: duckdb.DuckDBPyConnection) -> str:
    """Attach the configured Episode database for the current request."""
    return attach_readonly_database(
        connection,
        "episode_db",
        get_settings().paths.episode_db_path,
    )


def require_pool_db(connection: duckdb.DuckDBPyConnection) -> str:
    """Attach the configured Pool database for the current request."""
    return attach_readonly_database(
        connection,
        "pool_db",
        get_settings().paths.pool_db_path,
    )


def require_irm_qa_db(connection: duckdb.DuckDBPyConnection) -> str:
    """Attach the configured IRM Q&A database for the current request."""
    return attach_readonly_database(
        connection,
        "irm_qa_db",
        get_settings().paths.irm_qa_duckdb_path,
    )


def get_db(read_only: bool = True):
    """Open only the configured main DuckDB database.

    Explicit write requests are rejected when QRP_READ_ONLY is enabled. Parent
    directories are created only for writable connections.

    Endpoints that need a separate Episode or Pool database must explicitly
    attach it for the duration of that request.
    """

    settings: AppSettings = get_settings()
    if settings.database.read_only and not read_only:
        raise RuntimeError("QRP_READ_ONLY forbids opening DuckDB in write mode")
    path = settings.paths.duckdb_path
    if not read_only:
        path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path), read_only=read_only)


def get_db_path() -> Path:
    return get_settings().paths.duckdb_path
