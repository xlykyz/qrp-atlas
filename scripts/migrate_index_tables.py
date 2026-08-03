#!/usr/bin/env python3
"""Migrate the existing index tables to the Tushare-backed contracts.

The command is deliberately path-explicit. It creates ``index_basic``, adds the
new Tushare fields to ``index_daily``, and normalizes legacy index codes without
deleting rows. A collision between legacy and already-normalized keys aborts the
migration before any write.
"""

from __future__ import annotations

import argparse
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

from qrp_atlas.contracts import INDEX_BASIC, INDEX_DAILY, normalize_index_code


def backup_db(db_path: Path) -> Path | None:
    if not db_path.exists() or db_path.stat().st_size == 0:
        return None
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = db_path.with_name(f"{db_path.stem}.backup_index_tables_{timestamp}{db_path.suffix}")
    counter = 1
    while backup.exists():
        backup = db_path.with_name(
            f"{db_path.stem}.backup_index_tables_{timestamp}_{counter}{db_path.suffix}"
        )
        counter += 1
    shutil.copy2(db_path, backup)
    return backup


def _table_exists(connection: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    return table_name in {row[0] for row in connection.execute("SHOW TABLES").fetchall()}


def _columns(connection: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    return set(connection.execute(f"DESCRIBE {table_name}").fetchdf()["column_name"].tolist())


def _code_updates(connection: duckdb.DuckDBPyConnection) -> list[tuple[Any, str, str]]:
    if not _table_exists(connection, INDEX_DAILY.name):
        return []
    rows = connection.execute(
        f"SELECT trade_date, index_code FROM {INDEX_DAILY.name}"
    ).fetchall()
    updates: list[tuple[Any, str, str]] = []
    occupied: dict[tuple[Any, str], str] = {}
    for trade_date, old_code in rows:
        try:
            new_code = normalize_index_code(old_code)
        except ValueError:
            continue
        key = (trade_date, new_code)
        previous = occupied.get(key)
        if previous is not None and previous != old_code:
            raise RuntimeError(
                "index_daily key collision after code normalization: "
                f"trade_date={trade_date}, index_code={previous}/{old_code}, normalized={new_code}"
            )
        occupied[key] = old_code
        if new_code != old_code:
            updates.append((trade_date, old_code, new_code))
    return updates


def _migration_plan(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    index_basic_exists = _table_exists(connection, INDEX_BASIC.name)
    index_daily_exists = _table_exists(connection, INDEX_DAILY.name)
    daily_columns = _columns(connection, INDEX_DAILY.name) if index_daily_exists else set()
    missing_daily_columns = [] if not index_daily_exists else [
        column.name for column in INDEX_DAILY.columns if column.name not in daily_columns
    ]
    updates = _code_updates(connection)
    return {
        "create_index_basic": not index_basic_exists,
        "create_index_daily": not index_daily_exists,
        "missing_index_daily_columns": missing_daily_columns,
        "code_updates": updates,
    }


def migrate(db_path: Path, *, do_backup: bool = True) -> dict[str, Any]:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(str(db_path))
    try:
        plan = _migration_plan(connection)
    finally:
        connection.close()

    changed = bool(
        plan["create_index_basic"]
        or plan["create_index_daily"]
        or plan["missing_index_daily_columns"]
        or plan["code_updates"]
    )
    if not changed:
        return {
            "db_path": str(db_path),
            "action": "noop",
            "backup": None,
            "created_index_basic": False,
            "created_index_daily": False,
            "added_index_daily_columns": [],
            "normalized_index_daily_rows": 0,
        }

    backup = backup_db(db_path) if do_backup else None
    connection = duckdb.connect(str(db_path))
    try:
        connection.execute("BEGIN TRANSACTION")
        if plan["create_index_basic"]:
            connection.execute(INDEX_BASIC.duckdb_create_sql())
        if plan["create_index_daily"]:
            connection.execute(INDEX_DAILY.duckdb_create_sql())
        else:
            for column in INDEX_DAILY.columns:
                if column.name not in plan["missing_index_daily_columns"]:
                    continue
                definition = f"{column.name} {column.dtype}"
                if column.name == "created_at":
                    definition += " DEFAULT CURRENT_TIMESTAMP"
                connection.execute(f"ALTER TABLE {INDEX_DAILY.name} ADD COLUMN {definition}")

        for trade_date, old_code, new_code in plan["code_updates"]:
            connection.execute(
                f"UPDATE {INDEX_DAILY.name} SET index_code = ? WHERE trade_date = ? AND index_code = ?",
                [new_code, trade_date, old_code],
            )
        connection.execute("COMMIT")
    except Exception:
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
        "created_index_basic": bool(plan["create_index_basic"]),
        "created_index_daily": bool(plan["create_index_daily"]),
        "added_index_daily_columns": plan["missing_index_daily_columns"],
        "normalized_index_daily_rows": len(plan["code_updates"]),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate index_basic and index_daily to Tushare contracts")
    parser.add_argument("--db-path", required=True, type=Path)
    parser.add_argument("--no-backup", action="store_true")
    args = parser.parse_args()
    print(migrate(args.db_path, do_backup=not args.no_backup))


if __name__ == "__main__":
    main()
