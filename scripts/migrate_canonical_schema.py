#!/usr/bin/env python3
"""Safely add missing canonical contract tables to an existing DuckDB file."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import shutil

import duckdb

from qrp_atlas.config.settings import AppSettings, ConfigError
from qrp_atlas.contracts import ALL_TABLES, IRM_INTERACTION_QA, init_database


class MigrationError(RuntimeError):
    """A fail-closed schema migration error."""


def _main_table_names() -> tuple[str, ...]:
    return tuple(table.name for table in ALL_TABLES if table is not IRM_INTERACTION_QA)


def _table_names(path: Path) -> tuple[str, ...]:
    connection = duckdb.connect(str(path), read_only=True)
    try:
        return tuple(
            row[0]
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' ORDER BY table_name"
            ).fetchall()
        )
    finally:
        connection.close()


def _checkpoint(path: Path) -> None:
    connection = duckdb.connect(str(path))
    try:
        connection.execute("CHECKPOINT")
    finally:
        connection.close()


def _default_backup_path(target: Path) -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f")
    return target.with_name(f"{target.stem}.schema_migration_backup_{stamp}{target.suffix}")


def _verify_backup(path: Path) -> None:
    try:
        _table_names(path)
    except Exception as exc:
        raise MigrationError(
            f"backup verification failed ({type(exc).__name__}): {path}"
        ) from exc


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

    before = _table_names(target)
    missing = tuple(table for table in expected if table not in before)
    result: dict[str, object] = {
        "status": "NOOP" if not missing else "PENDING",
        "database": str(target),
        "missing_tables": list(missing),
        "backup": None,
        "tables_after": list(before),
    }
    if not missing:
        return result
    if not apply:
        return result

    backup = (backup_path or _default_backup_path(target)).resolve(strict=False)
    if backup == target:
        raise MigrationError("backup path must differ from the canonical database")
    if backup.exists():
        raise MigrationError(f"backup path already exists: {backup}")
    backup.parent.mkdir(parents=True, exist_ok=True)

    # Flush any pre-existing WAL before copying so the rollback artifact is a
    # self-contained DuckDB file.
    _checkpoint(target)
    shutil.copy2(target, backup)
    try:
        _verify_backup(backup)
    except Exception:
        backup.unlink(missing_ok=True)
        raise

    connection = duckdb.connect(str(target))
    try:
        connection.execute("BEGIN TRANSACTION")
        init_database(connection)
        connection.execute("COMMIT")
        connection.execute("CHECKPOINT")
    except Exception:
        try:
            connection.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        connection.close()

    after = _table_names(target)
    remaining = tuple(table for table in expected if table not in after)
    if remaining:
        raise MigrationError(
            "schema migration completed without all expected tables: "
            + ", ".join(remaining)
        )
    result.update(
        status="MIGRATED",
        backup=str(backup),
        tables_after=list(after),
    )
    return result


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
