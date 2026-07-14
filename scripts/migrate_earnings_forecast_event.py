#!/usr/bin/env python3
"""Idempotent migration: create earnings_forecast_event on local DuckDB.

Safety rules:
1. Audit whether the table already exists and whether schema matches.
2. If already present and fully compatible, return without backup.
3. Backup only when a real create/change is needed.
4. After migration, validate names/order/types/nullable/PK/defaults.
"""

from __future__ import annotations

import argparse
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

from qrp_atlas.config import DB_PATH, ensure_dirs
from qrp_atlas.contracts import EARNINGS_FORECAST_EVENT

TABLE = EARNINGS_FORECAST_EVENT


def backup_db(db_path: Path) -> Path | None:
    if not db_path.exists() or db_path.stat().st_size == 0:
        return None
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = db_path.with_name(f"{db_path.stem}.backup_earnings_forecast_{ts}{db_path.suffix}")
    # Avoid clobbering an existing backup path in the same second.
    n = 1
    while backup.exists():
        backup = db_path.with_name(
            f"{db_path.stem}.backup_earnings_forecast_{ts}_{n}{db_path.suffix}"
        )
        n += 1
    shutil.copy2(db_path, backup)
    return backup


def _normalize_dtype(dtype: str) -> str:
    text = str(dtype or "").strip().upper()
    # DuckDB may report TIMESTAMP_NS etc.
    if text.startswith("TIMESTAMP"):
        return "TIMESTAMP"
    if text in {"TEXT", "STRING"}:
        return "VARCHAR"
    if text.startswith("DECIMAL") or text.startswith("FLOAT") or text == "REAL":
        # contracts use DOUBLE for floating values
        if text == "DOUBLE":
            return "DOUBLE"
        if text.startswith("FLOAT") or text == "REAL":
            return "DOUBLE"
    return text


def inspect_table(con: duckdb.DuckDBPyConnection, table_name: str) -> dict[str, Any] | None:
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    if table_name not in tables:
        return None
    # PRAGMA table_info: cid, name, type, notnull, dflt_value, pk
    rows = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    columns = []
    pk_cols: list[tuple[int, str]] = []
    for cid, name, dtype, notnull, dflt, pk in rows:
        columns.append(
            {
                "cid": int(cid),
                "name": str(name),
                "dtype": _normalize_dtype(dtype),
                "nullable": not bool(notnull),
                "default": dflt,
                "pk": int(pk or 0),
            }
        )
        if pk:
            pk_cols.append((int(pk), str(name)))
    pk_cols.sort(key=lambda x: x[0])
    return {
        "columns": columns,
        "column_names": [c["name"] for c in columns],
        "primary_key": [name for _, name in pk_cols],
    }


def expected_schema() -> dict[str, Any]:
    """Expected *physical* schema produced by contracts CREATE SQL.

    Note: ``TableSchema.duckdb_create_sql`` currently emits NOT NULL only via
    PRIMARY KEY, so non-PK contract ``nullable=False`` is a contract-level rule
    and is not a physical DuckDB NOT NULL constraint.
    """
    cols = []
    for i, col in enumerate(TABLE.columns):
        is_pk = col.name in TABLE.primary_key
        cols.append(
            {
                "cid": i,
                "name": col.name,
                "dtype": _normalize_dtype(col.dtype),
                # Physical nullability after CREATE SQL.
                "nullable": False if is_pk else True,
                "contract_nullable": bool(col.nullable),
                "default": None,
                "pk": 1 if is_pk else 0,
            }
        )
    return {
        "columns": cols,
        "column_names": [c.name for c in TABLE.columns],
        "primary_key": list(TABLE.primary_key),
    }


def schema_diff(actual: dict[str, Any] | None, expected: dict[str, Any]) -> dict[str, Any]:
    if actual is None:
        return {
            "compatible": False,
            "missing_table": True,
            "missing_columns": list(expected["column_names"]),
            "extra_columns": [],
            "order_mismatch": False,
            "type_mismatches": [],
            "nullable_mismatches": [],
            "default_mismatches": [],
            "pk_mismatch": True,
        }

    exp_names = expected["column_names"]
    act_names = actual["column_names"]
    missing = [c for c in exp_names if c not in act_names]
    extra = [c for c in act_names if c not in exp_names]
    order_mismatch = act_names[: len(exp_names)] != exp_names if not missing and not extra else act_names != exp_names

    act_by_name = {c["name"]: c for c in actual["columns"]}
    type_mismatches = []
    nullable_mismatches = []
    default_mismatches = []
    for exp_col in expected["columns"]:
        name = exp_col["name"]
        if name not in act_by_name:
            continue
        act_col = act_by_name[name]
        if act_col["dtype"] != exp_col["dtype"]:
            type_mismatches.append(
                {"column": name, "expected": exp_col["dtype"], "actual": act_col["dtype"]}
            )
        if act_col["nullable"] != exp_col["nullable"]:
            nullable_mismatches.append(
                {
                    "column": name,
                    "expected_nullable": exp_col["nullable"],
                    "actual_nullable": act_col["nullable"],
                }
            )
        # contracts currently have no defaults; require actual default is None/NULL.
        if exp_col["default"] is None and act_col["default"] not in (None, "NULL"):
            default_mismatches.append(
                {
                    "column": name,
                    "expected_default": None,
                    "actual_default": act_col["default"],
                }
            )

    pk_mismatch = list(actual.get("primary_key") or []) != list(expected["primary_key"])
    compatible = not any(
        [
            missing,
            extra,
            order_mismatch,
            type_mismatches,
            nullable_mismatches,
            default_mismatches,
            pk_mismatch,
        ]
    )
    return {
        "compatible": compatible,
        "missing_table": False,
        "missing_columns": missing,
        "extra_columns": extra,
        "order_mismatch": order_mismatch,
        "type_mismatches": type_mismatches,
        "nullable_mismatches": nullable_mismatches,
        "default_mismatches": default_mismatches,
        "pk_mismatch": pk_mismatch,
        "actual_primary_key": list(actual.get("primary_key") or []),
        "expected_primary_key": list(expected["primary_key"]),
    }


def migrate(db_path: Path, *, do_backup: bool = True) -> dict:
    ensure_dirs()
    expected = expected_schema()
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    try:
        before = inspect_table(con, TABLE.name)
        diff_before = schema_diff(before, expected)
        if before is not None and diff_before["compatible"]:
            return {
                "db_path": str(db_path),
                "table": TABLE.name,
                "action": "noop",
                "backup": None,
                "created": False,
                "already_present": True,
                "schema_ok": True,
                "diff": diff_before,
                "primary_key": list(TABLE.primary_key),
                "column_names": expected["column_names"],
            }

        # Real change needed: optional backup then CREATE IF NOT EXISTS.
        # Note: CREATE IF NOT EXISTS cannot repair incompatible existing schema.
        if before is not None and not diff_before["compatible"]:
            return {
                "db_path": str(db_path),
                "table": TABLE.name,
                "action": "blocked_incompatible_schema",
                "backup": None,
                "created": False,
                "already_present": True,
                "schema_ok": False,
                "diff": diff_before,
                "primary_key": list(TABLE.primary_key),
                "error": (
                    "table exists with incompatible schema; refusing silent alter. "
                    "Resolve manually before re-run."
                ),
            }

        # Windows cannot copy a DuckDB file while this connection holds it open.
        if do_backup:
            con.close()
            backup = backup_db(db_path)
            con = duckdb.connect(str(db_path))
        else:
            backup = None
        con.execute(TABLE.duckdb_create_sql())
        after = inspect_table(con, TABLE.name)
        diff_after = schema_diff(after, expected)
        return {
            "db_path": str(db_path),
            "table": TABLE.name,
            "action": "created" if before is None else "unchanged",
            "backup": str(backup) if backup else None,
            "created": before is None and after is not None,
            "already_present": before is not None,
            "schema_ok": bool(diff_after["compatible"]),
            "diff": diff_after,
            "primary_key": list(TABLE.primary_key),
            "column_names": expected["column_names"],
            "column_order": after["column_names"] if after else [],
            "nullable": {c["name"]: c["nullable"] for c in (after or {}).get("columns", [])},
            "types": {c["name"]: c["dtype"] for c in (after or {}).get("columns", [])},
            "defaults": {c["name"]: c["default"] for c in (after or {}).get("columns", [])},
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
