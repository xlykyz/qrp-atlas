#!/usr/bin/env python3
"""Idempotent migration: create Task04-B1 popularity tables (dc_hot, ths_hot) on local DuckDB.

Does not drop or rewrite existing tables/data.
"""

from __future__ import annotations

import argparse
import shutil
from datetime import datetime
from pathlib import Path

import duckdb

from qrp_atlas.config import DB_PATH, ensure_dirs
from qrp_atlas.contracts import DC_HOT, THS_HOT

POPULARITY_TABLES = (
    DC_HOT,
    THS_HOT,
)


def backup_db(db_path: Path) -> Path | None:
    if not db_path.exists() or db_path.stat().st_size == 0:
        return None
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = db_path.with_name(f"{db_path.stem}.backup_popularity_{ts}{db_path.suffix}")
    shutil.copy2(db_path, backup)
    return backup


def migrate(db_path: Path, *, do_backup: bool = True) -> dict:
    ensure_dirs()
    backup = backup_db(db_path) if do_backup else None
    con = duckdb.connect(str(db_path))
    try:
        before = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        for table in POPULARITY_TABLES:
            con.execute(table.duckdb_create_sql())
        after = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        created = sorted(t.name for t in POPULARITY_TABLES if t.name not in before and t.name in after)
        existing = sorted(t.name for t in POPULARITY_TABLES if t.name in before)
        return {
            "db_path": str(db_path),
            "backup": str(backup) if backup else None,
            "created": created,
            "already_present": existing,
        }
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate popularity tables (dc_hot, ths_hot)")
    parser.add_argument("--db-path", default=str(DB_PATH))
    parser.add_argument("--no-backup", action="store_true")
    args = parser.parse_args()
    result = migrate(Path(args.db_path), do_backup=not args.no_backup)
    print(result)


if __name__ == "__main__":
    main()
