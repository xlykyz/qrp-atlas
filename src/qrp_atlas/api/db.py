"""DuckDB connection helpers backed by unified runtime settings."""

from __future__ import annotations

from pathlib import Path

import duckdb

from qrp_atlas.config.settings import get_settings


def get_db(read_only: bool = True):
    """Open the configured DuckDB database.

    Explicit write requests are rejected when QRP_READ_ONLY is enabled. Parent
    directories are created only for writable connections.
    """

    settings = get_settings()
    if settings.database.read_only and not read_only:
        raise RuntimeError("QRP_READ_ONLY forbids opening DuckDB in write mode")
    path = settings.paths.duckdb_path
    if not read_only:
        path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path), read_only=read_only)


def get_db_path() -> Path:
    return get_settings().paths.duckdb_path
