"""系统 B 基础状态检测测试。"""

from __future__ import annotations

import pandas as pd
import pytest

from qrp_atlas.contracts import TICKER
from qrp_atlas.indicators.system_b.detector import (
    SYSTEM_B_EXIT_TRIGGERED,
    SYSTEM_B_TREND_VALID,
    detect_system_b_basic_state,
)


def test_trend_valid_for_rising_stock(multi_day_stock_df: pd.DataFrame) -> None:
    result = detect_system_b_basic_state(multi_day_stock_df)
    row = result[result[TICKER] == "000001.SZ"].iloc[0]
    assert bool(row[SYSTEM_B_TREND_VALID]) is True
    assert bool(row[SYSTEM_B_EXIT_TRIGGERED]) is False


def test_exit_triggered_for_falling_stock(multi_day_stock_df: pd.DataFrame) -> None:
    result = detect_system_b_basic_state(multi_day_stock_df)
    row = result[result[TICKER] == "000002.SZ"].iloc[0]
    assert bool(row[SYSTEM_B_TREND_VALID]) is False
    assert bool(row[SYSTEM_B_EXIT_TRIGGERED]) is True


def test_one_row_per_ticker(multi_day_stock_df: pd.DataFrame) -> None:
    result = detect_system_b_basic_state(multi_day_stock_df)
    assert len(result) == 2
    assert set(result[TICKER].unique()) == {"000001.SZ", "000002.SZ"}


def test_empty_detector() -> None:
    result = detect_system_b_basic_state(pd.DataFrame())
    assert result.empty


def test_mixed_state_when_last_day_crosses() -> None:
    df = pd.DataFrame(
        [
            {"ticker": "000003.SZ", "trade_date": "2024-01-01", "close": 10.0},
            {"ticker": "000003.SZ", "trade_date": "2024-01-02", "close": 11.0},
            {"ticker": "000003.SZ", "trade_date": "2024-01-03", "close": 12.0},
            {"ticker": "000003.SZ", "trade_date": "2024-01-04", "close": 13.0},
            {"ticker": "000003.SZ", "trade_date": "2024-01-05", "close": 14.0},
            {"ticker": "000003.SZ", "trade_date": "2024-01-06", "close": 9.0},
        ]
    )
    result = detect_system_b_basic_state(df)
    row = result.iloc[0]
    # 最后2日: 第5天 close=14 > ma5=(10+11+12+13+14)/5=12.0 (>= 成立)
    #         第6天 close=9 < ma5=(11+12+13+14+9)/5=11.8 (< 成立, 但 >= 不成立)
    assert bool(row[SYSTEM_B_TREND_VALID]) is False
    assert bool(row[SYSTEM_B_EXIT_TRIGGERED]) is False
