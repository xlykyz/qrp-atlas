"""系统 B 基础状态检测测试。"""

from __future__ import annotations

import pandas as pd
import pytest

from qrp_atlas.contracts import TICKER
from qrp_atlas.indicators.stock.trend import calculate_stock_trend
from qrp_atlas.indicators.system_b.detector import (
    SYSTEM_B_EXIT_TRIGGERED,
    SYSTEM_B_TREND_VALID,
    detect_system_b_basic_state,
    detect_system_b_basic_state_from_prices,
)


def test_trend_valid_for_rising_stock(multi_day_stock_df: pd.DataFrame) -> None:
    result = detect_system_b_basic_state(calculate_stock_trend(multi_day_stock_df))
    row = result[result[TICKER] == "000001.SZ"].iloc[0]
    assert bool(row[SYSTEM_B_TREND_VALID]) is True
    assert bool(row[SYSTEM_B_EXIT_TRIGGERED]) is False


def test_exit_triggered_for_falling_stock(multi_day_stock_df: pd.DataFrame) -> None:
    result = detect_system_b_basic_state(calculate_stock_trend(multi_day_stock_df))
    row = result[result[TICKER] == "000002.SZ"].iloc[0]
    assert bool(row[SYSTEM_B_TREND_VALID]) is False
    assert bool(row[SYSTEM_B_EXIT_TRIGGERED]) is True


def test_convenience_function_calculates_trend_from_prices(
    multi_day_stock_df: pd.DataFrame,
) -> None:
    expected = detect_system_b_basic_state(calculate_stock_trend(multi_day_stock_df))
    result = detect_system_b_basic_state_from_prices(multi_day_stock_df)
    pd.testing.assert_frame_equal(result, expected)


def test_trend_valid_when_close_equals_ma5_for_two_days() -> None:
    price_df = pd.DataFrame(
        [
            {"ticker": "000003.SZ", "trade_date": f"2024-01-0{day}", "close": 10.0}
            for day in range(1, 7)
        ]
    )

    result = detect_system_b_basic_state(calculate_stock_trend(price_df))
    row = result.iloc[0]

    assert bool(row[SYSTEM_B_TREND_VALID]) is True
    assert bool(row[SYSTEM_B_EXIT_TRIGGERED]) is False


def test_one_row_per_ticker(multi_day_stock_df: pd.DataFrame) -> None:
    result = detect_system_b_basic_state(calculate_stock_trend(multi_day_stock_df))
    assert len(result) == 2
    assert set(result[TICKER].unique()) == {"000001.SZ", "000002.SZ"}


def test_empty_detector() -> None:
    result = detect_system_b_basic_state(pd.DataFrame())
    assert result.empty


def test_detector_requires_precalculated_trend_columns(
    multi_day_stock_df: pd.DataFrame,
) -> None:
    with pytest.raises(ValueError, match="缺少必要趋势列"):
        detect_system_b_basic_state(multi_day_stock_df)


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
    result = detect_system_b_basic_state(calculate_stock_trend(df))
    row = result.iloc[0]
    assert bool(row[SYSTEM_B_TREND_VALID]) is False
    assert bool(row[SYSTEM_B_EXIT_TRIGGERED]) is False


def test_one_observation_is_not_a_two_day_state() -> None:
    price_df = pd.DataFrame(
        [
            {"ticker": "000004.SZ", "trade_date": "2024-01-01", "close": 10.0},
            {"ticker": "000004.SZ", "trade_date": "2024-01-02", "close": 10.0},
            {"ticker": "000004.SZ", "trade_date": "2024-01-03", "close": 10.0},
            {"ticker": "000004.SZ", "trade_date": "2024-01-04", "close": 10.0},
            {"ticker": "000004.SZ", "trade_date": "2024-01-05", "close": 10.0},
        ]
    )

    result = detect_system_b_basic_state(calculate_stock_trend(price_df))
    row = result.iloc[0]

    assert bool(row[SYSTEM_B_TREND_VALID]) is False
    assert bool(row[SYSTEM_B_EXIT_TRIGGERED]) is False
