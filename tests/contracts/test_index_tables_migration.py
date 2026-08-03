from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from scripts.migrate_index_tables import migrate
from qrp_atlas.contracts import normalize_index_code


def test_normalize_index_code_preserves_tushare_multi_letter_market_suffix() -> None:
    assert normalize_index_code("000300.CSI") == "000300.CSI"


def test_migrate_index_tables_adds_tushare_fields_and_normalizes_legacy_codes(tmp_path: Path) -> None:
    db_path = tmp_path / "quant.db"
    connection = duckdb.connect(str(db_path))
    try:
        connection.execute(
            """
            CREATE TABLE index_daily (
                trade_date DATE,
                index_code VARCHAR,
                index_name VARCHAR,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                created_at TIMESTAMP,
                PRIMARY KEY (trade_date, index_code)
            )
            """
        )
        connection.execute(
            """
            INSERT INTO index_daily
                (trade_date, index_code, index_name, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [date(2026, 7, 29), "sh000001", "上证综指", 100.0, 102.0, 99.0, 101.0, 1200],
        )
    finally:
        connection.close()

    result = migrate(db_path, do_backup=False)

    assert result["action"] == "migrated"
    assert result["created_index_basic"] is True
    assert result["normalized_index_daily_rows"] == 1
    connection = duckdb.connect(str(db_path), read_only=True)
    try:
        columns = set(connection.execute("DESCRIBE index_daily").fetchdf()["column_name"])
        assert {"pre_close", "change", "pct_change", "amount"}.issubset(columns)
        assert connection.execute("SELECT index_code FROM index_daily").fetchone()[0] == "000001.SH"
        assert connection.execute("SELECT COUNT(*) FROM index_basic").fetchone()[0] == 0
    finally:
        connection.close()

    repeated = migrate(db_path, do_backup=False)
    assert repeated["action"] == "noop"
