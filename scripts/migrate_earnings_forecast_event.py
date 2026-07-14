#!/usr/bin/env python3
"""Idempotent migration: create earnings_forecast_event on local DuckDB.

Does not drop/rewrite existing tables or download any market data.
"""

from __future__ import annotations

import argparse
import shutil
from datetime import datetime
from pathlib import Path

import duckdb

from qrp_atlas.config import DB_PATH, ensure_dirs
from qrp_atlas.contracts import EARNINGS_FORECAST_EVENT

TABLE = EARNINGS_FORECAST_EVENT


def backup_db(db_path: Path) -> Path | None:
    if not db_path.exists() or db_path.stat().st_size == 0:
        return None
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = db_path.with_name(f"{db_path.stem}.backup_earnings_forecast_{ts}{db_path.suffix}")
    shutil.copy2(db_path, backup)
    return backup


def migrate(db_path: Path, *, do_backup: bool = True) -> dict:
    ensure_dirs()
    backup = backup_db(db_path) if do_backup else None
    con = duckdb.connect(str(db_path))
    try:
        before = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        con.execute(TABLE.duckdb_create_sql())
        # schema validation
        cols = {
            r[1]
            for r in con.execute(f"PRAGMA table_info('{TABLE.name}')").fetchall()
        }
        expected = set(TABLE.column_names())
        missing = sorted(expected - cols)
        after = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        return {
            "db_path": str(db_path),
            "backup": str(backup) if backup else None,
            "table": TABLE.name,
            "created": TABLE.name not in before and TABLE.name in after,
            "already_present": TABLE.name in before,
            "schema_ok": not missing,
            "missing_columns": missing,
            "primary_key": list(TABLE.primary_key),
        }
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate earnings_forecast_event table")
    parser.add_argument("--db-path", default=str(DB_PATH))
    parser.add_argument("--no-backup", action="store_true")
    args = parser.parse_args()
    result = migrate(Path(args.db_path), do_backup=not args.no_backup)
    print(result)
    if not result.get("schema_ok"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
