from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from qrp_atlas.contracts import DC_HOT, THS_HOT, get_table
from scripts.migrate_popularity_tables import migrate


def test_migrate_popularity_tables_creates_tables_idempotently(tmp_path: Path) -> None:
    db_path = tmp_path / "quant.db"

    # First migration on empty database
    res1 = migrate(db_path, do_backup=False)
    assert set(res1["created"]) == {"dc_hot", "ths_hot"}
    assert res1["already_present"] == []

    connection = duckdb.connect(str(db_path), read_only=True)
    try:
        tables = {r[0] for r in connection.execute("SHOW TABLES").fetchall()}
        assert {"dc_hot", "ths_hot"} <= tables
    finally:
        connection.close()

    # Second migration should be a no-op
    res2 = migrate(db_path, do_backup=False)
    assert res2["created"] == []
    assert set(res2["already_present"]) == {"dc_hot", "ths_hot"}


def test_popularity_ddl_and_contracts_schema_consistency(tmp_path: Path) -> None:
    ddl_path = Path("deploy/duckdb/005_popularity_dc_ths_hot.sql")
    assert ddl_path.exists()
    sql_text = ddl_path.read_text(encoding="utf-8")

    db_path = tmp_path / "ddl_popularity_test.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        con.execute(sql_text)
        ddl_tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        expected_tables = {"dc_hot", "ths_hot"}
        assert expected_tables <= ddl_tables

        for table_schema in (DC_HOT, THS_HOT):
            table_name = table_schema.name
            info = con.execute(f"PRAGMA table_info({table_name})").fetchall()
            actual_col_names = [r[1] for r in info]
            expected_col_names = list(table_schema.column_names())
            assert actual_col_names == expected_col_names, (
                f"Table {table_name} columns mismatch: actual {actual_col_names} vs expected {expected_col_names}"
            )
            for r, col in zip(info, table_schema.columns):
                _, name, col_type, notnull, _, pk = r
                assert name == col.name
                assert col_type.upper() == col.dtype.upper(), (
                    f"Table {table_name}.{name} type mismatch: DDL {col_type} vs schema {col.dtype}"
                )
                expected_notnull = not col.nullable
                assert bool(notnull) == expected_notnull, (
                    f"Table {table_name}.{name} nullability mismatch: DDL notnull={notnull} vs expected {expected_notnull}"
                )
            actual_pk = tuple(r[1] for r in info if r[5])
            assert set(actual_pk) == set(table_schema.primary_key), (
                f"Table {table_name} PK mismatch: DDL {actual_pk} vs schema {table_schema.primary_key}"
            )
    finally:
        con.close()
