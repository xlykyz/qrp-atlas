"""indicators 测试用小型 DataFrame，不依赖真实数据库。"""

from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture
def daily_market_df() -> pd.DataFrame:
    """单日全市场行情快照，覆盖涨跌平、涨跌停、创业板/科创板。"""
    return pd.DataFrame(
        [
            {"ticker": "000001.SZ", "pct_change": 5.0, "close": 10.5, "pre_close": 10.0, "is_st": False},
            {"ticker": "000002.SZ", "pct_change": -3.0, "close": 9.7, "pre_close": 10.0, "is_st": False},
            {"ticker": "600000.SH", "pct_change": 0.0, "close": 10.0, "pre_close": 10.0, "is_st": False},
            {"ticker": "300750.SZ", "pct_change": 20.0, "close": 12.0, "pre_close": 10.0, "is_st": False},
            {"ticker": "688001.SH", "pct_change": -20.0, "close": 8.0, "pre_close": 10.0, "is_st": False},
            {"ticker": "000003.SZ", "pct_change": -6.0, "close": 9.4, "pre_close": 10.0, "is_st": False},
            {"ticker": "300751.SZ", "pct_change": -15.0, "close": 8.5, "pre_close": 10.0, "is_st": False},
        ]
    )


@pytest.fixture
def multi_day_stock_df() -> pd.DataFrame:
    """多日多股行情：000001 最后两日高于 ma5，000002 最后两日低于 ma5。"""
    return pd.DataFrame(
        [
            {"ticker": "000001.SZ", "trade_date": "2024-01-01", "close": 10.0},
            {"ticker": "000001.SZ", "trade_date": "2024-01-02", "close": 9.0},
            {"ticker": "000001.SZ", "trade_date": "2024-01-03", "close": 10.0},
            {"ticker": "000001.SZ", "trade_date": "2024-01-04", "close": 11.0},
            {"ticker": "000001.SZ", "trade_date": "2024-01-05", "close": 12.0},
            {"ticker": "000001.SZ", "trade_date": "2024-01-06", "close": 13.0},
            {"ticker": "000002.SZ", "trade_date": "2024-01-01", "close": 12.0},
            {"ticker": "000002.SZ", "trade_date": "2024-01-02", "close": 11.0},
            {"ticker": "000002.SZ", "trade_date": "2024-01-03", "close": 10.0},
            {"ticker": "000002.SZ", "trade_date": "2024-01-04", "close": 9.0},
            {"ticker": "000002.SZ", "trade_date": "2024-01-05", "close": 8.0},
            {"ticker": "000002.SZ", "trade_date": "2024-01-06", "close": 7.0},
        ]
    )
