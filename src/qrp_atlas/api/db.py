"""DuckDB connection helpers backed by unified runtime settings.

In addition to opening the main ``quant.db`` database, ``get_db()`` also
ATTACHes the episode database and pool database in read-only mode when they
are configured (``QRP_EPISODE_DB_PATH`` / ``QRP_POOL_DB_PATH``).  This enables
cross-database JOINs (e.g. episode observations joined with stock_info) without
any data duplication.

Graceful degradation: if a path is not configured, the file does not exist, or
ATTACH fails for any reason, the main connection remains usable.  Endpoints
that depend on episode/pool data will detect the missing attachment and return
a clear HTTP 503 error.

NOTE: The actual episode-db / pool-db file paths must be configured on the
Linux server via environment variables.  See ``.env.example``.
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from qrp_atlas.config.settings import AppSettings, get_settings

logger = logging.getLogger(__name__)


def _attach_auxiliary_database(
    connection: duckdb.DuckDBPyConnection,
    alias: str,
    db_path: Path | None,
) -> None:
    """ATTACH an auxiliary DuckDB file in read-only mode.

    Silently skips when *db_path* is ``None`` or the file does not exist.
    Logs a warning (not an error) when ATTACH fails, so the main connection
    stays functional for endpoints that do not need the auxiliary database.
    """
    if db_path is None:
        return
    if not db_path.exists():
        logger.warning(
            "Auxiliary database %s not found at %s; skipping ATTACH as %s",
            alias,
            db_path,
            alias,
        )
        return
    try:
        safe_path = str(db_path).replace("'", "''")
        connection.execute(f"ATTACH '{safe_path}' AS {alias} (READ_ONLY)")
        logger.debug("ATTACH'd %s from %s", alias, db_path)
    except Exception as exc:
        # Graceful degradation: the main connection is still usable.
        logger.warning(
            "Failed to ATTACH %s from %s: %s. Endpoints requiring %s will "
            "return 503.",
            alias,
            db_path,
            exc,
            alias,
        )


def get_db(read_only: bool = True):
    """Open the configured DuckDB database with auxiliary databases attached.

    Explicit write requests are rejected when QRP_READ_ONLY is enabled. Parent
    directories are created only for writable connections.

    After opening the main database, the episode database (``episode_db``) and
    pool database (``pool_db``) are ATTACH'd in read-only mode if configured.
    """

    settings: AppSettings = get_settings()
    if settings.database.read_only and not read_only:
        raise RuntimeError("QRP_READ_ONLY forbids opening DuckDB in write mode")
    path = settings.paths.duckdb_path
    if not read_only:
        path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(path), read_only=read_only)
    _attach_auxiliary_database(connection, "episode_db", settings.paths.episode_db_path)
    _attach_auxiliary_database(connection, "pool_db", settings.paths.pool_db_path)
    return connection


def get_db_path() -> Path:
    return get_settings().paths.duckdb_path
