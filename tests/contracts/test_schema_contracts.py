"""contracts/schema 表结构契约测试。

覆盖：
- DAILY_MARKET_SNAPSHOT 主键为 (trade_date, ticker)
- ADJ_FACTOR_CHANGES 主键为 (ticker, trade_date)
- ADJ_FACTOR_CHANGES 至少包含 ticker / trade_date / adj_factor
- 所有 TableSchema.duckdb_create_sql() 都能在 DuckDB 中建表（含 IF NOT EXISTS 幂等）
- 所有主键字段必须 nullable=False
"""

from __future__ import annotations

import duckdb
import pytest

from qrp_atlas.contracts import (
    ADJ_FACTOR,
    ADJ_FACTOR_CHANGES,
    ALL_TABLES,
    DAILY_MARKET_SNAPSHOT,
    TICKER,
    TRADE_DATE,
    init_database,
    init_irm_database,
)


def test_daily_market_snapshot_primary_key():
    pk = DAILY_MARKET_SNAPSHOT.primary_key
    assert TRADE_DATE in pk
    assert TICKER in pk
    # 顺序按 SSOT 约定：trade_date 在前
    assert pk == (TRADE_DATE, TICKER)


def test_adj_factor_changes_primary_key():
    pk = ADJ_FACTOR_CHANGES.primary_key
    assert TICKER in pk
    assert TRADE_DATE in pk
    assert pk == (TICKER, TRADE_DATE)


def test_adj_factor_changes_has_required_columns():
    names = ADJ_FACTOR_CHANGES.column_names()
    assert TICKER in names
    assert TRADE_DATE in names
    assert ADJ_FACTOR in names


def test_all_tables_create_sql_executable_in_duckdb(tmp_path):
    """每张表的 duckdb_create_sql() 必须能在 DuckDB 中执行建表。"""
    db_path = tmp_path / "schema.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        for table in ALL_TABLES:
            sql = table.duckdb_create_sql()
            # 首次建表
            con.execute(sql)
            # 二次执行 IF NOT EXISTS 不应报错
            con.execute(sql)
            # 表确实存在
            row = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table.name],
            ).fetchone()
            assert row and row[0] == 1, f"table {table.name} not created"
    finally:
        con.close()


def test_init_database_creates_main_tables_without_irm(tmp_path):
    """init_database() 在全新 DuckDB 上建出除 IRM 外的全部契约表，且不创建可写 IRM 表。"""
    db_path = tmp_path / "init.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        init_database(con)
        expected = {t.name for t in ALL_TABLES} - {"irm_interaction_qa"}
        rows = con.execute("SHOW TABLES").fetchall()
        actual = {r[0] for r in rows}
        assert expected <= actual, (
            f"missing tables: {expected - actual}"
        )
        assert "irm_interaction_qa" not in actual
    finally:
        con.close()


def test_init_irm_database_creates_irm_table(tmp_path):
    """init_irm_database() 在独立库创建 irm_interaction_qa 正式契约表。"""
    db_path = tmp_path / "irm.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        init_irm_database(con)
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        assert tables == {"irm_interaction_qa"}
        rows = con.execute("PRAGMA table_info('irm_interaction_qa')").fetchall()
        names = {r[1] for r in rows}
        for required in ("pid", "reply_time", "reply_date"):
            assert required in names
    finally:
        con.close()


def test_primary_key_columns_are_not_nullable():
    """所有主键字段必须 nullable=False（SSOT 契约）。"""
    problems = []
    for table in ALL_TABLES:
        nullable_pk_cols = [
            col.name
            for col in table.columns
            if col.name in table.primary_key and col.nullable
        ]
        if nullable_pk_cols:
            problems.append((table.name, nullable_pk_cols))
    assert not problems, f"primary key columns nullable=True: {problems}"


def test_daily_market_snapshot_has_ohlcv_columns():
    """DAILY_MARKET_SNAPSHOT 应包含 OHLCV + pre_close 基本字段。"""
    names = set(DAILY_MARKET_SNAPSHOT.column_names())
    for required in ("open", "high", "low", "close", "volume", "amount", "pre_close"):
        assert required in names, f"missing column {required}"


@pytest.mark.parametrize(
    "table_name,expected_pk",
    [
        ("daily_market_snapshot", (TRADE_DATE, TICKER)),
        ("adj_factor_changes", (TICKER, TRADE_DATE)),
        ("trading_calendar", (TRADE_DATE,)),
        ("stock_info", (TICKER,)),
        ("market_phase", (TRADE_DATE,)),
        ("system_b_episode_segment", ("segment_id",)),
        ("stock_collection", ("collection_id", "revision_id")),
        ("theme", ("theme_id", "revision_id")),
        ("theme_membership_history", ("membership_id", "revision_id")),
        ("theme_custom_index_daily", ("theme_id", "trade_date")),
        ("theme_custom_index_state", ("theme_id", "trade_date")),
        ("theme_custom_index_episode", ("episode_id",)),
        ("theme_m4_observation", ("theme_id", "trade_date")),
        ("market_m6_observation", (TRADE_DATE, "market_scope")),
    ],
)
def test_known_primary_keys(table_name: str, expected_pk: tuple):
    from qrp_atlas.contracts import get_table

    table = get_table(table_name)
    assert table.primary_key == expected_pk


def test_init_stock_collections_database_creates_tables(tmp_path):
    """init_stock_collections_database() 在独立库创建 stock_collection/theme/theme_membership_history/theme_effective_member_daily 契约表。"""
    from qrp_atlas.contracts import init_stock_collections_database

    db_path = tmp_path / "stock_collections.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        init_stock_collections_database(con)
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        expected = {
            "stock_collection",
            "theme",
            "theme_membership_history",
            "theme_effective_member_daily",
        }
        assert tables == expected
    finally:
        con.close()


def test_all_tableschema_instances_are_registered_in_all_tables():
    """所有在 schema.py 中定义的 TableSchema 实例必须全部注册进 ALL_TABLES 和 TABLE_BY_NAME。"""
    import inspect
    from qrp_atlas.contracts import schema as schema_mod
    from qrp_atlas.contracts.schema import TableSchema, ALL_TABLES, TABLE_BY_NAME, get_table

    all_defined = [
        obj for name, obj in inspect.getmembers(schema_mod)
        if isinstance(obj, TableSchema)
    ]
    all_tables_set = set(ALL_TABLES)
    missing = [t.name for t in all_defined if t not in all_tables_set]
    assert not missing, f"TableSchema defined but not in ALL_TABLES: {missing}"

    for t in all_defined:
        assert t.name in TABLE_BY_NAME, f"Table {t.name} missing from TABLE_BY_NAME"
        assert get_table(t.name) is t, f"get_table('{t.name}') failed to return instance"


def test_system_b_episode_segment_schema_registration():
    """验证 SYSTEM_B_EPISODE_SEGMENT 已经正确注册并可通过 get_table 访问。"""
    from qrp_atlas.contracts import SYSTEM_B_EPISODE_SEGMENT, get_table

    table = get_table("system_b_episode_segment")
    assert table is SYSTEM_B_EPISODE_SEGMENT
    assert table.primary_key == ("segment_id",)


def test_ddl_and_contracts_schema_consistency(tmp_path):
    """验证 deploy/duckdb/003_stock_collections_and_m4.sql 与 contracts/schema.py 100% 字段一致。"""
    from pathlib import Path
    from qrp_atlas.contracts.schema import TABLE_BY_NAME

    ddl_path = Path("deploy/duckdb/003_stock_collections_and_m4.sql")
    assert ddl_path.exists()
    sql_text = ddl_path.read_text(encoding="utf-8")

    db_path = tmp_path / "ddl_test.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        con.execute(sql_text)
        ddl_tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        expected_ddl_tables = {
            "stock_collection",
            "theme",
            "theme_membership_history",
            "theme_effective_member_daily",
            "theme_custom_index_daily",
            "theme_custom_index_state",
            "theme_custom_index_episode",
            "theme_m4_observation",
            "theme_production_run",
        }
        assert ddl_tables == expected_ddl_tables

        for table_name in expected_ddl_tables:
            table_schema = TABLE_BY_NAME[table_name]
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


def test_m6_ddl_and_contracts_schema_consistency(tmp_path):
    """验证 deploy/duckdb/005_market_m6_observation.sql 与 contracts/schema.py 100% 字段一致。"""
    from pathlib import Path
    from qrp_atlas.contracts.schema import TABLE_BY_NAME

    ddl_path = Path("deploy/duckdb/005_market_m6_observation.sql")
    assert ddl_path.exists()
    sql_text = ddl_path.read_text(encoding="utf-8")

    db_path = tmp_path / "m6_ddl_test.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        con.execute(sql_text)
        ddl_tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        assert ddl_tables == {"market_m6_observation"}

        table_schema = TABLE_BY_NAME["market_m6_observation"]
        info = con.execute("PRAGMA table_info(market_m6_observation)").fetchall()
        actual_col_names = [r[1] for r in info]
        expected_col_names = list(table_schema.column_names())
        assert actual_col_names == expected_col_names

        for r, col in zip(info, table_schema.columns):
            _, name, col_type, notnull, _, pk = r
            assert name == col.name
            assert col_type.upper() == col.dtype.upper()
            assert bool(notnull) == (not col.nullable)

        actual_pk = tuple(r[1] for r in info if r[5])
        assert set(actual_pk) == set(table_schema.primary_key)
    finally:
        con.close()
