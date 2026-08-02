#!/usr/bin/env python3
"""Idempotent migration: move irm_interaction_qa to the dedicated IRM DuckDB.

Source: the shared ``quant.db`` (``settings.paths.duckdb_path``), opened
read-only and never modified.
Target: the dedicated ``irm_qa.duckdb`` (``settings.paths.irm_qa_duckdb_path``),
created with the formal table Contract and copied inside one target-side
transaction.

Idempotency rules:
- target missing           -> create formal schema, copy, verify, commit
- target present, identical-> verify succeeds, no-op
- target present, empty    -> verify schema, full copy
- target present, diverged -> fail closed (no overwrite, no truncate) with a
                              difference summary
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import duckdb

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.contracts import IRM_INTERACTION_QA, init_irm_database

TABLE = IRM_INTERACTION_QA
TABLE_NAME = TABLE.name
PRIMARY_KEY = TABLE.primary_key
REQUIRED_COLUMNS = TABLE.column_names()


def load_settings(env_file: str | None) -> AppSettings:
    if env_file:
        return AppSettings.load(env_file=env_file)
    return AppSettings.load()


def _normalize_dtype(dtype: str) -> str:
    text = str(dtype or "").strip().upper()
    if text.startswith("TIMESTAMP"):
        return "TIMESTAMP"
    if text in {"TEXT", "STRING"}:
        return "VARCHAR"
    if text.startswith("DECIMAL") or text.startswith("FLOAT") or text == "REAL":
        return "DOUBLE" if text in {"DOUBLE", "FLOAT", "REAL"} else text
    return text


def inspect_table(con: duckdb.DuckDBPyConnection, table_name: str) -> dict[str, Any] | None:
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    if table_name not in tables:
        return None
    rows = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    columns = []
    pk_cols: list[tuple[int, str]] = []
    for cid, name, dtype, notnull, _dflt, pk in rows:
        columns.append(
            {
                "cid": int(cid),
                "name": str(name),
                "dtype": _normalize_dtype(dtype),
                "nullable": not bool(notnull),
                "pk": int(pk or 0),
            }
        )
        if pk:
            pk_cols.append((int(pk), str(name)))
    pk_cols.sort(key=lambda x: x[0])
    return {
        "column_names": [c["name"] for c in columns],
        "primary_key": [name for _, name in pk_cols],
        "columns": columns,
    }


def expected_schema() -> dict[str, Any]:
    """Expected physical schema produced by the formal table Contract."""
    columns = []
    for i, col in enumerate(TABLE.columns):
        is_pk = col.name in PRIMARY_KEY
        columns.append(
            {
                "cid": i,
                "name": col.name,
                "dtype": _normalize_dtype(col.dtype),
                "nullable": False if is_pk else True,
                "contract_nullable": bool(col.nullable),
                "pk": 1 if is_pk else 0,
            }
        )
    return {
        "column_names": [col.name for col in TABLE.columns],
        "primary_key": list(PRIMARY_KEY),
        "columns": columns,
    }


def schema_diff(actual: dict[str, Any] | None, expected: dict[str, Any]) -> dict[str, Any]:
    if actual is None:
        return {
            "compatible": False,
            "missing_table": True,
            "missing_columns": list(expected["column_names"]),
            "extra_columns": [],
            "type_mismatches": [],
            "nullable_mismatches": [],
            "pk_mismatch": True,
        }
    exp_names = expected["column_names"]
    act_names = actual["column_names"]
    missing = [c for c in exp_names if c not in act_names]
    extra = [c for c in act_names if c not in exp_names]
    act_by_name = {c["name"]: c for c in actual["columns"]}
    type_mismatches = []
    nullable_mismatches = []
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
    pk_mismatch = list(actual.get("primary_key") or []) != list(expected["primary_key"])
    return {
        "compatible": not any([missing, extra, type_mismatches, nullable_mismatches, pk_mismatch]),
        "missing_table": False,
        "missing_columns": missing,
        "extra_columns": extra,
        "type_mismatches": type_mismatches,
        "nullable_mismatches": nullable_mismatches,
        "pk_mismatch": pk_mismatch,
        "actual_primary_key": list(actual.get("primary_key") or []),
        "expected_primary_key": list(expected["primary_key"]),
    }


def content_metrics(con: duckdb.DuckDBPyConnection, table_name: str) -> dict[str, Any]:
    row = con.execute(
        f"""
        SELECT
            COUNT(*) AS total_rows,
            MIN(reply_time) AS min_reply_time,
            MAX(reply_time) AS max_reply_time
        FROM {table_name}
        """
    ).fetchone()
    dup_row = con.execute(
        f"""
        SELECT
            COUNT(*) AS distinct_pid,
            SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END) AS duplicate_pid
        FROM (
            SELECT {PRIMARY_KEY[0]} AS pid, COUNT(*) AS cnt
            FROM {table_name}
            GROUP BY {PRIMARY_KEY[0]}
        ) AS grouped
        """
    ).fetchone()
    fingerprint = con.execute(
        f"""
        SELECT md5(string_agg(pid || '|' || CAST(reply_time AS VARCHAR), ',' ORDER BY pid))
        FROM {table_name}
        """
    ).fetchone()[0]
    return {
        "total_rows": int(row[0]),
        "min_reply_time": row[1],
        "max_reply_time": row[2],
        "distinct_pid": int(dup_row[0]),
        "duplicate_pid": int(dup_row[1] or 0),
        "fingerprint": fingerprint,
    }


def metrics_diff(source: dict[str, Any], target: dict[str, Any]) -> dict[str, Any]:
    keys = ("total_rows", "distinct_pid", "duplicate_pid", "min_reply_time", "max_reply_time", "fingerprint")
    differences = {key: {"source": source[key], "target": target[key]} for key in keys if source[key] != target[key]}
    return {
        "identical": not differences,
        "differences": differences,
    }


def _copy_within_transaction(
    target_con: duckdb.DuckDBPyConnection,
    *,
    column_names: Sequence[str],
) -> None:
    """Create the formal schema and copy all rows in one target transaction."""
    init_irm_database(target_con)
    columns = ", ".join(column_names)
    target_con.execute(
        f"INSERT INTO {TABLE_NAME} ({columns}) "
        f"SELECT {columns} FROM source_irm.{TABLE_NAME}"
    )


def migrate(settings: AppSettings, *, source_path: Path | None = None, target_path: Path | None = None) -> dict[str, Any]:
    source_path = Path(source_path or settings.paths.duckdb_path)
    target_path = Path(target_path or settings.paths.irm_qa_duckdb_path)

    if not source_path.exists():
        raise RuntimeError(f"source database does not exist: {source_path}")
    if not source_path.is_file():
        raise RuntimeError(f"source database is not a file: {source_path}")

    expected = expected_schema()
    source_con = duckdb.connect(str(source_path), read_only=True)
    try:
        source_tables = {r[0] for r in source_con.execute("SHOW TABLES").fetchall()}
        if TABLE_NAME not in source_tables:
            raise RuntimeError(f"source database has no {TABLE_NAME} table: {source_path}")
        source_schema = inspect_table(source_con, TABLE_NAME)
        source_diff = schema_diff(source_schema, expected)
        if not source_diff["compatible"]:
            raise RuntimeError(
                f"source {TABLE_NAME} schema diverges from the formal Contract; refusing to copy: "
                f"{source_diff}"
            )
        source_metrics = content_metrics(source_con, TABLE_NAME)
        copy_columns = list(REQUIRED_COLUMNS)

        target_exists = target_path.exists() and target_path.stat().st_size > 0
        if not target_exists:
            # Target missing: create schema, copy, verify, commit.
            target_con = duckdb.connect(str(target_path))
            try:
                target_con.execute("BEGIN TRANSACTION")
                target_con.execute(
                    f"ATTACH '{str(source_path)}' AS source_irm (READ_ONLY)"
                )
                try:
                    _copy_within_transaction(target_con, column_names=copy_columns)
                    target_con.execute("COMMIT")
                except BaseException:
                    target_con.execute("ROLLBACK")
                    raise
            finally:
                target_con.close()
            result = {"action": "created", "target_created": True}
        else:
            target_con = duckdb.connect(str(target_path), read_only=True)
            try:
                actual = inspect_table(target_con, TABLE_NAME)
                diff = schema_diff(actual, expected)
                target_metrics = (
                    content_metrics(target_con, TABLE_NAME) if actual is not None else None
                )
            finally:
                target_con.close()

            if actual is None or target_metrics["total_rows"] == 0:
                # Target present but empty: verify schema, then full copy.
                if actual is not None and not diff["compatible"]:
                    raise RuntimeError(
                        "target table exists with incompatible schema; refusing to copy"
                    )
                target_con = duckdb.connect(str(target_path))
                try:
                    target_con.execute("BEGIN TRANSACTION")
                    target_con.execute(
                        f"ATTACH '{str(source_path)}' AS source_irm (READ_ONLY)"
                    )
                    try:
                        _copy_within_transaction(target_con, column_names=copy_columns)
                        target_con.execute("COMMIT")
                    except BaseException:
                        target_con.execute("ROLLBACK")
                        raise
                finally:
                    target_con.close()
                result = {"action": "copied_into_empty_target", "target_created": False}
            else:
                # Target present with rows: require full identity.
                if not diff["compatible"]:
                    raise RuntimeError(
                        f"target {TABLE_NAME} schema diverges from the formal Contract: {diff}"
                    )
                content_diff = metrics_diff(source_metrics, target_metrics)
                if not content_diff["identical"]:
                    raise RuntimeError(
                        f"target {TABLE_NAME} content diverges from source; refusing to overwrite: "
                        f"{content_diff['differences']}"
                    )
                result = {"action": "noop", "target_created": False}

        # Final verification after any write path.
        final_con = duckdb.connect(str(target_path), read_only=True)
        try:
            final_actual = inspect_table(final_con, TABLE_NAME)
            final_diff = schema_diff(final_actual, expected)
            final_metrics = content_metrics(final_con, TABLE_NAME)
        finally:
            final_con.close()
        if not final_diff["compatible"]:
            raise RuntimeError(f"post-migration schema check failed: {final_diff}")
        content_diff = metrics_diff(source_metrics, final_metrics)
        if not content_diff["identical"]:
            raise RuntimeError(f"post-migration content check failed: {content_diff['differences']}")

        result.update(
            {
                "source_path": str(source_path),
                "target_path": str(target_path),
                "table": TABLE_NAME,
                "source_metrics": source_metrics,
                "target_metrics": final_metrics,
                "schema_ok": True,
            }
        )
        return result
    finally:
        source_con.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Idempotently migrate irm_interaction_qa to the dedicated IRM DuckDB"
    )
    parser.add_argument("--env-file", help="explicit .env file for effective settings")
    args = parser.parse_args(argv)
    try:
        settings = load_settings(args.env_file)
        result = migrate(settings)
    except Exception as exc:  # noqa: BLE001
        print(f"migration failed: {exc}", file=sys.stderr)
        return 1
    print(
        f"action={result['action']} table={result['table']} "
        f"source_rows={result['source_metrics']['total_rows']} "
        f"target_rows={result['target_metrics']['total_rows']} "
        f"distinct_pid={result['target_metrics']['distinct_pid']} "
        f"duplicate_pid={result['target_metrics']['duplicate_pid']} "
        f"min_reply_time={result['target_metrics']['min_reply_time']} "
        f"max_reply_time={result['target_metrics']['max_reply_time']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
