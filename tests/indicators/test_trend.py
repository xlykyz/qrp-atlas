"""个股趋势指标测试。"""

from __future__ import annotations

import pandas as pd
import pytest

from qrp_atlas.contracts import CLOSE, TICKER, TRADE_DATE
from qrp_atlas.indicators.stock.trend import (
    CLOSE_ABOVE_MA5,
    CLOSE_ABOVE_MA5_DAYS,
    CLOSE_BELOW_MA5,
    CLOSE_BELOW_MA5_DAYS,
    MA5,
    calculate_stock_trend,
)


def test_ma5_values(multi_day_stock_df: pd.DataFrame) -> None:
    result = calculate_stock_trend(multi_day_stock_df)
    stock1 = result[result[TICKER] == "000001.SZ"].sort_values(TRADE_DATE).reset_index(drop=True)

    assert pytest.approx(stock1.loc[4, MA5], abs=1e-6) == (10 + 9 + 10 + 11 + 12) / 5
    assert pytest.approx(stock1.loc[5, MA5], abs=1e-6) == (9 + 10 + 11 + 12 + 13) / 5
    assert pd.isna(stock1.loc[0, MA5])


def test_above_below_flags(multi_day_stock_df: pd.DataFrame) -> None:
    result = calculate_stock_trend(multi_day_stock_df)
    stock1 = result[result[TICKER] == "000001.SZ"].sort_values(TRADE_DATE).reset_index(drop=True)

    assert stock1.loc[4, CLOSE_ABOVE_MA5] is True or bool(stock1.loc[4, CLOSE_ABOVE_MA5]) is True
    assert stock1.loc[5, CLOSE_ABOVE_MA5] is True or bool(stock1.loc[5, CLOSE_ABOVE_MA5]) is True
    assert bool(stock1.loc[4, CLOSE_BELOW_MA5]) is False


def test_consecutive_above_days(multi_day_stock_df: pd.DataFrame) -> None:
    result = calculate_stock_trend(multi_day_stock_df)
    stock1 = result[result[TICKER] == "000001.SZ"].sort_values(TRADE_DATE).reset_index(drop=True)

    assert stock1.loc[0, CLOSE_ABOVE_MA5_DAYS] == 0
    assert stock1.loc[4, CLOSE_ABOVE_MA5_DAYS] == 1
    assert stock1.loc[5, CLOSE_ABOVE_MA5_DAYS] == 2


def test_consecutive_below_days(multi_day_stock_df: pd.DataFrame) -> None:
    result = calculate_stock_trend(multi_day_stock_df)
    stock2 = result[result[TICKER] == "000002.SZ"].sort_values(TRADE_DATE).reset_index(drop=True)

    assert stock2.loc[4, CLOSE_BELOW_MA5_DAYS] == 1
    assert stock2.loc[5, CLOSE_BELOW_MA5_DAYS] == 2


def test_empty_trend() -> None:
    result = calculate_stock_trend(pd.DataFrame())
    assert result.empty
    assert MA5 in result.columns


def test_missing_required_column_raises() -> None:
    df = pd.DataFrame({"ticker": ["000001.SZ"], "close": [10.0]})
    with pytest.raises(ValueError, match="缺少必要列"):
        calculate_stock_trend(df)


def test_multiple_tickers_isolated(multi_day_stock_df: pd.DataFrame) -> None:
    result = calculate_stock_trend(multi_day_stock_df)
    tickers = set(result[TICKER].unique())
    assert tickers == {"000001.SZ", "000002.SZ"}
