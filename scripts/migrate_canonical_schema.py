#!/usr/bin/env python3
"""Safely add missing canonical contract tables to an existing DuckDB file."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import duckdb

from qrp_atlas.config.settings import AppSettings, ConfigError
from qrp_atlas.contracts import ALL_TABLES, IRM_INTERACTION_QA, init_database


class MigrationError(RuntimeError):
    """A fail-closed schema migration error."""


DatabaseSnapshot = dict[str, tuple[tuple[object, ...], int]]
LOGGER = logging.getLogger(__name__)


def _main_table_names() -> tuple[str, ...]:
    return tuple(table.name for table in ALL_TABLES if table is not IRM_INTERACTION_QA)


def _table_names(path: Path) -> tuple[str, ...]:
    connection = duckdb.connect(str(path), read_only=True)
    try:
        source_alias = str(connection.execute("SELECT current_database()").fetchone()[0])
        return _table_names_from_connection(
            connection,
            source_alias,
            source_alias=source_alias,
        )
    finally:
        connection.close()


def _table_names_from_connection(
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


def _capture_snapshot(
    connection: duckdb.DuckDBPyConnection,
    database_alias: str,
    *,
    source_alias: str,
) -> DatabaseSnapshot:
    snapshot: DatabaseSnapshot = {}
    for table_name in _table_names_from_connection(
        connection,
        database_alias,
        source_alias=source_alias,
    ):
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


def _default_backup_path(target: Path) -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f")
    return target.with_name(
        f"{target.stem}.schema_migration_backup_{stamp}_{uuid4().hex}{target.suffix}"
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
    """Verify backup table set, column descriptions, and row counts."""

    actual = _capture_snapshot(
        connection,
        backup_alias,
        source_alias=source_alias,
    )
    if actual == baseline:
        return
    missing = sorted(set(baseline) - set(actual))
    extra = sorted(set(actual) - set(baseline))
    mismatched = sorted(
        table_name
        for table_name in set(baseline) & set(actual)
        if baseline[table_name] != actual[table_name]
    )
    raise MigrationError(
        "canonical backup validation failed: "
        f"missing={missing}, extra={extra}, mismatched={mismatched}"
    )


def _create_validated_backup(
    connection: duckdb.DuckDBPyConnection,
    baseline: DatabaseSnapshot,
    backup: Path,
    *,
    source_alias: str,
) -> Path:
    backup_alias = f"canonical_backup_{uuid4().hex}"
    attached = False
    try:
        connection.execute(
            f"ATTACH {_quote_path(backup)} AS {_quote_identifier(backup_alias)}"
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
        return backup
    except BaseException:
        if attached:
            try:
                connection.execute(f"DETACH {_quote_identifier(backup_alias)}")
            except duckdb.Error as detach_error:
                LOGGER.debug("failed to detach invalid canonical backup", exc_info=detach_error)
        _remove_backup(backup)
        raise


def migrate(
    settings: AppSettings,
    *,
    apply: bool,
    backup_path: Path | None = None,
) -> dict[str, object]:
    """Create missing main-database tables after making a verified file backup.

    Existing tables and rows are left untouched.  ``apply`` is explicit so a
    caller cannot accidentally turn an inspection invocation into a write.
    """

    target = settings.paths.duckdb_path.resolve(strict=False)
    expected = _main_table_names()
    if not target.exists():
        raise MigrationError(f"canonical database does not exist: {target}")
    if not target.is_file():
        raise MigrationError(f"canonical database is not a regular file: {target}")
    if settings.database.read_only and apply:
        raise MigrationError("QRP_READ_ONLY forbids schema migration")

    if not apply:
        before = _table_names(target)
        missing = tuple(table for table in expected if table not in before)
        return {
            "status": "NOOP" if not missing else "PENDING",
            "database": str(target),
            "missing_tables": list(missing),
            "backup": None,
            "tables_after": list(before),
        }

    connection = duckdb.connect(str(target))
    backup: Path | None = None
    try:
        source_alias = str(connection.execute("SELECT current_database()").fetchone()[0])
        baseline = _capture_snapshot(
            connection,
            source_alias,
            source_alias=source_alias,
        )
        before = tuple(sorted(baseline))
        missing = tuple(table for table in expected if table not in baseline)
        result: dict[str, object] = {
            "status": "NOOP" if not missing else "PENDING",
            "database": str(target),
            "missing_tables": list(missing),
            "backup": None,
            "tables_after": list(before),
        }
        if not missing:
            return result

        backup = (backup_path or _default_backup_path(target)).resolve(strict=False)
        if backup == target:
            raise MigrationError("backup path must differ from the canonical database")
        if backup.exists():
            raise MigrationError(f"backup path already exists: {backup}")
        backup.parent.mkdir(parents=True, exist_ok=True)

        connection.execute("FORCE CHECKPOINT")
        _create_validated_backup(
            connection,
            baseline,
            backup,
            source_alias=source_alias,
        )

        transaction_open = False
        try:
            connection.execute("BEGIN TRANSACTION")
            transaction_open = True
            init_database(connection)
            connection.execute("COMMIT")
            transaction_open = False
        except BaseException:
            if transaction_open:
                try:
                    connection.execute("ROLLBACK")
                except duckdb.Error as rollback_error:
                    LOGGER.debug("failed to roll back canonical schema migration", exc_info=rollback_error)
            raise

        after = _capture_snapshot(
            connection,
            source_alias,
            source_alias=source_alias,
        )
        remaining = tuple(table for table in expected if table not in after)
        if remaining:
            raise MigrationError(
                "schema migration completed without all expected tables: "
                + ", ".join(remaining)
            )
        result.update(
            status="MIGRATED",
            backup=str(backup),
            tables_after=sorted(after),
        )
        return result
    finally:
        connection.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", help="explicit AppSettings environment file")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="create missing tables after a verified backup; omit for dry-run",
    )
    parser.add_argument("--backup-path", type=Path)
    args = parser.parse_args(argv)
    try:
        settings = AppSettings.load(env_file=args.env_file)
        result = migrate(settings, apply=args.apply, backup_path=args.backup_path)
    except (ConfigError, MigrationError, OSError, duckdb.Error) as exc:
        print(json.dumps({"status": "FAILED", "error": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
