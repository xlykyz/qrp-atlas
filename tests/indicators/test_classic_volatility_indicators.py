from __future__ import annotations

import math

import pandas as pd
import pytest

from qrp_atlas.indicators import IndicatorRequest, calculate_indicators


def _frame(closes, highs=None, lows=None):
    highs = highs or [value + 1.0 for value in closes]
    lows = lows or [value - 1.0 for value in closes]
    return pd.DataFrame(
        {
            "ticker": "A",
            "trade_date": pd.date_range("2024-01-01", periods=len(closes)),
            "high": highs,
            "low": lows,
            "close": closes,
        }
    )


def test_true_range_and_wilder_atr_use_explicit_sma_seed() -> None:
    frame = _frame([10.0, 12.0, 9.0], highs=[11.0, 13.0, 12.0], lows=[9.0, 11.0, 8.0])
    result = calculate_indicators(
        frame,
        (
            IndicatorRequest("true_range", alias="tr"),
            IndicatorRequest("atr", {"window": 2}, alias="atr"),
        ),
    )
    assert result["tr"].tolist() == pytest.approx([2.0, 3.0, 4.0])
    assert math.isnan(result.loc[0, "atr"])
    assert result.loc[1, "atr"] == pytest.approx(2.5)
    assert result.loc[2, "atr"] == pytest.approx(3.25)


def test_bollinger_return_volatility_and_downside_volatility_are_hand_verifiable() -> None:
    band = calculate_indicators(
        _frame([1.0, 2.0, 3.0]),
        (IndicatorRequest("bollinger_bands", {"window": 3, "multiplier": 2.0}, alias="bb"),),
    )
    std = math.sqrt(2.0 / 3.0)
    assert band.loc[2, "bb_middle"] == pytest.approx(2.0)
    assert band.loc[2, "bb_upper"] == pytest.approx(2.0 + 2.0 * std)
    assert band.loc[2, "bb_lower"] == pytest.approx(2.0 - 2.0 * std)
    assert band.loc[2, "bb_bandwidth"] == pytest.approx(2.0 * std)

    volatility = calculate_indicators(
        _frame([100.0, 110.0, 99.0]),
        (
            IndicatorRequest("return_volatility", {"window": 2, "annualization": 1.0}, alias="vol"),
            IndicatorRequest(
                "downside_volatility",
                {"window": 2, "annualization": 1.0, "target": 0.0},
                alias="downside",
            ),
        ),
    )
    assert volatility.loc[2, "vol"] == pytest.approx(0.1)
    assert volatility.loc[2, "downside"] == pytest.approx(math.sqrt(0.005))


def test_drawdown_and_ulcer_windows_do_not_use_future_prices() -> None:
    base = _frame([100.0, 80.0, 90.0])
    requests = (
        IndicatorRequest("rolling_current_drawdown", {"window": 3}, alias="current_dd"),
        IndicatorRequest("rolling_max_drawdown", {"window": 3}, alias="max_dd"),
        IndicatorRequest("ulcer_index", {"window": 3}, alias="ulcer"),
    )
    first = calculate_indicators(base, requests)
    extended = calculate_indicators(pd.concat([base, _frame([1000.0]).assign(trade_date=["2024-01-04"])]), requests)
    pd.testing.assert_series_equal(first.loc[2, list(first.columns[-3:])], extended.loc[2, list(first.columns[-3:])])
    assert first.loc[2, "current_dd"] == pytest.approx(-0.1)
    assert first.loc[2, "max_dd"] == pytest.approx(-0.2)
    assert first.loc[2, "ulcer"] == pytest.approx(math.sqrt((0.0 + 400.0 + 100.0) / 3.0))


def test_atr_breakout_bands_exclude_the_current_bar() -> None:
    frame = _frame([10.0, 10.0, 50.0], highs=[11.0, 11.0, 100.0], lows=[9.0, 9.0, 1.0])
    result = calculate_indicators(
        frame,
        (IndicatorRequest("atr_breakout_bands", {"window": 2, "multiplier": 1.0}, alias="band"),),
    )
    assert result.loc[2, "band_atr"] == pytest.approx(2.0)
    assert result.loc[2, "band_upper"] == pytest.approx(12.0)
    assert result.loc[2, "band_lower"] == pytest.approx(8.0)


def test_invalid_prices_and_zero_denominators_remain_nan() -> None:
    frame = _frame([1.0, 0.0, math.inf], highs=[2.0, 1.0, math.inf], lows=[1.0, 0.0, 1.0])
    result = calculate_indicators(
        frame,
        (
            IndicatorRequest("atr", {"window": 2}, alias="atr"),
            IndicatorRequest("bollinger_bands", {"window": 2}, alias="bb"),
        ),
    )
    assert result["atr"].isna().all()
    assert result["bb_percent_b"].isna().all()
