#!/usr/bin/env python3
"""Idempotent migration: create task 03-B PIT tables on local DuckDB.

Does not drop or rewrite existing tables/data.
"""

from __future__ import annotations

import argparse
import shutil
from datetime import datetime
from pathlib import Path

import duckdb

from qrp_atlas.config import DB_PATH, ensure_dirs
from qrp_atlas.contracts import (
    BALANCE_SHEET,
    CASHFLOW_STATEMENT,
    FINANCIAL_INDICATOR,
    INCOME_STATEMENT,
    INDEX_COMPONENT_HISTORY,
    INDUSTRY_MEMBERSHIP_HISTORY,
)

PIT_TABLES = (
    INCOME_STATEMENT,
    BALANCE_SHEET,
    CASHFLOW_STATEMENT,
    FINANCIAL_INDICATOR,
    INDUSTRY_MEMBERSHIP_HISTORY,
    INDEX_COMPONENT_HISTORY,
)


def backup_db(db_path: Path) -> Path | None:
    if not db_path.exists() or db_path.stat().st_size == 0:
        return None
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = db_path.with_name(f"{db_path.stem}.backup_pit_{ts}{db_path.suffix}")
    shutil.copy2(db_path, backup)
    return backup


def migrate(db_path: Path, *, do_backup: bool = True) -> dict:
    ensure_dirs()
    backup = backup_db(db_path) if do_backup else None
    con = duckdb.connect(str(db_path))
    try:
        before = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        for table in PIT_TABLES:
            con.execute(table.duckdb_create_sql())
        after = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        created = sorted(t.name for t in PIT_TABLES if t.name not in before and t.name in after)
        existing = sorted(t.name for t in PIT_TABLES if t.name in before)
        return {
            "db_path": str(db_path),
            "backup": str(backup) if backup else None,
            "created": created,
            "already_present": existing,
        }
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate PIT financial/industry/index tables")
    parser.add_argument("--db-path", default=str(DB_PATH))
    parser.add_argument("--no-backup", action="store_true")
    args = parser.parse_args()
    result = migrate(Path(args.db_path), do_backup=not args.no_backup)
    print(result)


if __name__ == "__main__":
    main()
