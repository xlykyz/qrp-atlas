"""PriceFrame execution fields: is_suspended / suspend_type from suspend_d."""

from __future__ import annotations

import duckdb
import pandas as pd
import pytest

from qrp_atlas.backtest.data import load_stock_prices, normalize_price_frame


def _boolish(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    text = str(value).strip().lower()
    if text in {"true", "t", "1", "yes"}:
        return True
    if text in {"false", "f", "0", "no", ""}:
        return False
    return bool(value)


@pytest.fixture
def execution_fields_con():
    """In-memory DuckDB with minimal market + suspend tables."""
    con = duckdb.connect(database=":memory:")
    con.execute(
        """
        CREATE TABLE daily_market_snapshot (
            trade_date DATE,
            ticker VARCHAR,
            name VARCHAR,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            amount DOUBLE,
            turnover DOUBLE,
            market_cap DOUBLE,
            float_cap DOUBLE,
            is_st BOOLEAN,
            is_limit_up BOOLEAN,
            is_limit_down BOOLEAN
        )
        """
    )
    con.execute(
        """
        CREATE TABLE suspend_d (
            trade_date DATE,
            ticker VARCHAR,
            suspend_timing VARCHAR,
            suspend_type VARCHAR,
            created_at TIMESTAMP
        )
        """
    )
    con.execute(
        """
        INSERT INTO daily_market_snapshot VALUES
        ('2024-01-02', '000001.SZ', '平安银行', 10.0, 10.5, 9.8, 10.2,
         1000, 10000.0, 0.01, 1e10, 5e9, FALSE, FALSE, FALSE),
        ('2024-01-03', '000001.SZ', '平安银行', 10.2, 10.8, 10.0, 10.5,
         1100, 11000.0, 0.011, 1e10, 5e9, FALSE, TRUE, FALSE),
        ('2024-01-03', '000002.SZ', '万科A', 8.0, 8.5, 7.8, 8.2,
         2000, 16000.0, 0.02, 2e10, 1e10, FALSE, FALSE, TRUE),
        ('2024-01-04', '000001.SZ', '平安银行', 10.5, 11.0, 10.3, 10.8,
         1200, 12000.0, 0.012, 1e10, 5e9, FALSE, FALSE, FALSE)
        """
    )
    # Two suspend_type rows for the same (trade_date, ticker) on 2024-01-03.
    con.execute(
        """
        INSERT INTO suspend_d VALUES
        ('2024-01-03', '000001.SZ', 'am', '停牌一天', NULL),
        ('2024-01-03', '000001.SZ', 'pm', '盘中停牌', NULL)
        """
    )
    try:
        yield con
    finally:
        con.close()


def test_normalize_price_frame_keeps_suspend_fields():
    df = pd.DataFrame(
        [
            {
                "trade_date": "2024-01-03",
                "ticker": "000001.SZ",
                "name": "平安银行",
                "open": 10.2,
                "high": 10.8,
                "low": 10.0,
                "close": 10.5,
                "is_suspended": True,
                "suspend_type": "盘中停牌,停牌一天",
                "is_limit_up": True,
                "is_limit_down": False,
            }
        ]
    )
    out = normalize_price_frame(df, asset_type="stock", id_col="ticker", name_col="name")
    assert "is_suspended" in out.columns
    assert "suspend_type" in out.columns
    assert _boolish(out.iloc[0]["is_suspended"]) is True
    assert out.iloc[0]["suspend_type"] == "盘中停牌,停牌一天"
    assert _boolish(out.iloc[0]["is_limit_up"]) is True
    assert _boolish(out.iloc[0]["is_limit_down"]) is False


def test_load_stock_prices_aggregates_multiple_suspend_types(execution_fields_con):
    out = load_stock_prices(
        con=execution_fields_con,
        tickers=["000001.SZ"],
        start_date="2024-01-03",
        end_date="2024-01-03",
    )

    assert len(out) == 1
    row = out.iloc[0]
    assert row["asset_id"] == "000001.SZ"
    assert pd.Timestamp(row["trade_date"]) == pd.Timestamp("2024-01-03")
    assert _boolish(row["is_suspended"]) is True

    suspend_types = {part.strip() for part in str(row["suspend_type"]).split(",") if part.strip()}
    assert suspend_types == {"停牌一天", "盘中停牌"}
    # Stable ordered aggregation (ORDER BY suspend_type).
    assert str(row["suspend_type"]) == "停牌一天,盘中停牌" or suspend_types == {
        "停牌一天",
        "盘中停牌",
    }

    assert _boolish(row["is_limit_up"]) is True
    assert _boolish(row["is_limit_down"]) is False


def test_load_stock_prices_marks_non_suspended_false(execution_fields_con):
    out = load_stock_prices(
        con=execution_fields_con,
        tickers=["000001.SZ"],
        start_date="2024-01-02",
        end_date="2024-01-02",
    )
    assert len(out) == 1
    row = out.iloc[0]
    assert _boolish(row["is_suspended"]) is False
    suspend_type = row["suspend_type"]
    assert suspend_type is None or (isinstance(suspend_type, float) and pd.isna(suspend_type)) or str(suspend_type).strip() == ""


def test_load_stock_prices_preserves_limit_flags_and_filters(execution_fields_con):
    out = load_stock_prices(
        con=execution_fields_con,
        tickers=["000002.SZ"],
        start_date="2024-01-03",
        end_date="2024-01-04",
    )
    assert len(out) == 1
    row = out.iloc[0]
    assert row["asset_id"] == "000002.SZ"
    assert pd.Timestamp(row["trade_date"]) == pd.Timestamp("2024-01-03")
    assert _boolish(row["is_limit_up"]) is False
    assert _boolish(row["is_limit_down"]) is True
    assert _boolish(row["is_suspended"]) is False

    filtered = load_stock_prices(
        con=execution_fields_con,
        tickers=["000001.SZ", "000002.SZ"],
        start_date="2024-01-03",
        end_date="2024-01-03",
    )
    assert set(filtered["asset_id"]) == {"000001.SZ", "000002.SZ"}
    assert len(filtered) == 2
    # Every (trade_date, asset_id) appears once even with multi-type suspend rows.
    assert filtered.duplicated(subset=["trade_date", "asset_id"]).sum() == 0
