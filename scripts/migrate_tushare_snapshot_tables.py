#!/usr/bin/env python3
"""Idempotently add the three Tushare snapshot tables to canonical quant.db.

The migration only creates missing tables from the contracts SSOT.  It never
deletes or rewrites existing rows.  A backup is made by default for an
existing database; pass --no-backup for an already backed-up test database.
"""

from __future__ import annotations

import argparse
import shutil
from datetime import datetime
from pathlib import Path

import duckdb

from qrp_atlas.contracts import LIMIT_STEP, STK_HIGH_SHOCK, THS_DAILY


TABLES = (LIMIT_STEP, THS_DAILY, STK_HIGH_SHOCK)


def backup_db(db_path: Path) -> Path | None:
    if not db_path.exists() or db_path.stat().st_size == 0:
        return None
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = db_path.with_name(
        f"{db_path.stem}.backup_tushare_snapshots_{timestamp}{db_path.suffix}"
    )
    counter = 1
    while backup.exists():
        backup = db_path.with_name(
            f"{db_path.stem}.backup_tushare_snapshots_{timestamp}_{counter}{db_path.suffix}"
        )
        counter += 1
    shutil.copy2(db_path, backup)
    return backup


def migrate(db_path: Path, *, do_backup: bool = True) -> dict[str, object]:
    """Create missing endpoint tables in one transaction and return an audit summary."""

    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"canonical database does not exist: {db_path}")
    if not db_path.is_file():
        raise ValueError(f"canonical database is not a regular file: {db_path}")

    connection = duckdb.connect(str(db_path))
    try:
        existing = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
    finally:
        connection.close()

    missing = [table for table in TABLES if table.name not in existing]
    if not missing:
        return {
            "db_path": str(db_path),
            "action": "noop",
            "backup": None,
            "created_tables": [],
        }

    backup = backup_db(db_path) if do_backup else None
    connection = duckdb.connect(str(db_path))
    try:
        connection.execute("BEGIN TRANSACTION")
        for table in missing:
            connection.execute(table.duckdb_create_sql())
        connection.execute("COMMIT")
    except BaseException:
        try:
            connection.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        connection.close()

    return {
        "db_path": str(db_path),
        "action": "migrated",
        "backup": str(backup) if backup else None,
        "created_tables": [table.name for table in missing],
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create the formal Tushare snapshot tables in canonical quant.db"
    )
    parser.add_argument("--db-path", required=True, type=Path)
    parser.add_argument("--no-backup", action="store_true")
    args = parser.parse_args()
    print(migrate(args.db_path, do_backup=not args.no_backup))


if __name__ == "__main__":
    main()
