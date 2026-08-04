#!/usr/bin/env python3
"""Safely add the three Tushare snapshot tables to canonical ``quant.db``.

The default path keeps one source DuckDB connection open for the complete
operation.  It captures a pre-migration baseline, forces a checkpoint, makes a
native DuckDB database copy, validates that copy, and only then creates missing
tables in the source transaction.  ``--no-backup`` is intentionally explicit
for isolated tests or callers that already hold an external reliable backup.
"""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import duckdb

from qrp_atlas.contracts import LIMIT_STEP, STK_HIGH_SHOCK, THS_DAILY

TABLES = (LIMIT_STEP, THS_DAILY, STK_HIGH_SHOCK)
DatabaseSnapshot = dict[str, tuple[tuple[object, ...], int]]
LOGGER = logging.getLogger(__name__)


class BackupValidationError(RuntimeError):
    """Raised when the native backup does not match the source baseline."""


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _quote_path(value: Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _relation(database_alias: str, table_name: str) -> str:
    return (
        f"{_quote_identifier(database_alias)}."
        f"{_quote_identifier('main')}."
        f"{_quote_identifier(table_name)}"
    )


def _table_names(
    connection: duckdb.DuckDBPyConnection,
    database_alias: str,
    *,
    source_alias: str,
) -> tuple[str, ...]:
    if database_alias == source_alias:
        rows = connection.execute("SHOW TABLES").fetchall()
    else:
        rows = connection.execute(
            f"SHOW TABLES FROM {_quote_identifier(database_alias)}"
        ).fetchall()
    return tuple(sorted(str(row[0]) for row in rows))


def _capture_snapshot(
    connection: duckdb.DuckDBPyConnection,
    database_alias: str,
    *,
    source_alias: str,
) -> DatabaseSnapshot:
    snapshot: DatabaseSnapshot = {}
    for table_name in _table_names(connection, database_alias, source_alias=source_alias):
        description = tuple(
            connection.execute(
                f"DESCRIBE {_relation(database_alias, table_name)}"
            ).fetchall()
        )
        row_count = int(
            connection.execute(
                f"SELECT COUNT(*) FROM {_relation(database_alias, table_name)}"
            ).fetchone()[0]
        )
        snapshot[table_name] = (description, row_count)
    return snapshot


def _new_backup_path(db_path: Path) -> Path:
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    token = uuid4().hex
    return db_path.with_name(
        f"{db_path.stem}.backup_tushare_snapshots_{timestamp}_{token}{db_path.suffix}"
    )


def _remove_backup(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def validate_backup(
    connection: duckdb.DuckDBPyConnection,
    backup_alias: str,
    baseline: DatabaseSnapshot,
    *,
    source_alias: str,
) -> None:
    """Verify backup tables, column descriptions, and row counts."""

    actual = _capture_snapshot(
        connection,
        backup_alias,
        source_alias=source_alias,
    )
    if actual != baseline:
        raise BackupValidationError(
            "validated Tushare snapshot backup does not match the source "
            "table set, column structure, or row counts"
        )


def _create_validated_backup(
    connection: duckdb.DuckDBPyConnection,
    db_path: Path,
    baseline: DatabaseSnapshot,
    *,
    source_alias: str,
) -> Path:
    backup_path = _new_backup_path(db_path)
    backup_alias = f"tushare_backup_{uuid4().hex}"
    attached = False
    try:
        connection.execute(
            f"ATTACH {_quote_path(backup_path)} AS {_quote_identifier(backup_alias)}"
        )
        attached = True
        connection.execute(
            "COPY FROM DATABASE "
            f"{_quote_identifier(source_alias)} TO {_quote_identifier(backup_alias)}"
        )
        validate_backup(
            connection,
            backup_alias,
            baseline,
            source_alias=source_alias,
        )
        connection.execute(f"DETACH {_quote_identifier(backup_alias)}")
        attached = False
        return backup_path
    except BaseException:
        if attached:
            try:
                connection.execute(f"DETACH {_quote_identifier(backup_alias)}")
            except duckdb.Error as detach_error:
                LOGGER.debug("failed to detach invalid backup", exc_info=detach_error)
        _remove_backup(backup_path)
        raise


def migrate(db_path: Path, *, do_backup: bool = True) -> dict[str, object]:
    """Validate a native backup, then create missing endpoint tables atomically."""

    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"canonical database does not exist: {db_path}")
    if not db_path.is_file():
        raise ValueError(f"canonical database is not a regular file: {db_path}")

    connection = duckdb.connect(str(db_path))
    backup_path: Path | None = None
    try:
        source_alias = str(connection.execute("SELECT current_database()").fetchone()[0])
        baseline = _capture_snapshot(
            connection,
            source_alias,
            source_alias=source_alias,
        )
        missing = [table for table in TABLES if table.name not in baseline]

        connection.execute("FORCE CHECKPOINT")
        if do_backup:
            backup_path = _create_validated_backup(
                connection,
                db_path,
                baseline,
                source_alias=source_alias,
            )

        if missing:
            transaction_open = False
            try:
                connection.execute("BEGIN TRANSACTION")
                transaction_open = True
                for table in missing:
                    connection.execute(table.duckdb_create_sql())
                connection.execute("COMMIT")
                transaction_open = False
            except BaseException:
                if transaction_open:
                    try:
                        connection.execute("ROLLBACK")
                    except duckdb.Error as rollback_error:
                        LOGGER.debug("failed to roll back migration transaction", exc_info=rollback_error)
                raise

        return {
            "db_path": str(db_path),
            "action": "migrated" if missing else "noop",
            "backup": str(backup_path) if backup_path else None,
            "created_tables": [table.name for table in missing],
            "baseline_tables": sorted(baseline),
        }
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create formal Tushare snapshot tables in canonical quant.db"
    )
    parser.add_argument("--db-path", required=True, type=Path)
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="explicitly rely on an external reliable backup; skip native backup validation",
    )
    args = parser.parse_args()
    print(migrate(args.db_path, do_backup=not args.no_backup))


if __name__ == "__main__":
    main()
