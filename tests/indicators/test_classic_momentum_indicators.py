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


def test_wilder_rsi_and_flat_zero_denominator_semantics() -> None:
    rising = calculate_indicators(
        _frame([1.0, 2.0, 3.0]),
        (IndicatorRequest("rsi", {"window": 2}, alias="rsi"),),
    )
    assert rising.loc[2, "rsi"] == pytest.approx(100.0)
    flat = calculate_indicators(
        _frame([1.0, 1.0, 1.0]),
        (IndicatorRequest("rsi", {"window": 2}, alias="rsi"),),
    )
    assert math.isnan(flat.loc[2, "rsi"])


def test_stochastic_williams_and_cci_formulas() -> None:
    frame = _frame([1.0, 2.0, 3.0], highs=[2.0, 3.0, 4.0], lows=[0.5, 1.0, 2.0])
    result = calculate_indicators(
        frame,
        (
            IndicatorRequest("stochastic_oscillator", {"window": 3, "d_window": 1}, alias="stoch"),
            IndicatorRequest("williams_r", {"window": 3}, alias="williams"),
            IndicatorRequest("cci", {"window": 3, "constant": 0.015}, alias="cci"),
        ),
    )
    expected_k = 100.0 * (3.0 - 0.5) / (4.0 - 0.5)
    assert result.loc[2, "stoch_percent_k"] == pytest.approx(expected_k)
    assert result.loc[2, "stoch_percent_d"] == pytest.approx(expected_k)
    assert result.loc[2, "williams"] == pytest.approx(-100.0 * (4.0 - 3.0) / 3.5)
    typical = pd.Series([(2.0 + 0.5 + 1.0) / 3.0, 2.0, 3.0])
    expected_cci = (typical.iloc[-1] - typical.mean()) / (0.015 * (typical - typical.mean()).abs().mean())
    assert result.loc[2, "cci"] == pytest.approx(expected_cci)


def test_adx_directional_movement_is_wilder_smoothed_and_isolated() -> None:
    frame = _frame(
        [1.0, 2.0, 3.0, 4.0],
        highs=[2.0, 3.0, 4.0, 5.0],
        lows=[0.5, 1.0, 2.0, 3.0],
    )
    result = calculate_indicators(
        frame,
        (IndicatorRequest("adx", {"window": 2}, alias="direction"),),
    )
    assert result.loc[2, "direction_adx"] == pytest.approx(100.0)
    assert result.loc[2, "direction_plus_di"] > 0.0
    assert result.loc[2, "direction_minus_di"] == pytest.approx(0.0)

    multi = pd.concat([frame.assign(ticker="B"), _frame([10.0, 9.0, 8.0, 7.0]).assign(ticker="A")])
    shuffled = multi.sample(frac=1.0, random_state=9)
    first = calculate_indicators(shuffled, (IndicatorRequest("adx", {"window": 2}, alias="adx"),))
    second = calculate_indicators(shuffled.sample(frac=1.0, random_state=10), (IndicatorRequest("adx", {"window": 2}, alias="adx"),))
    pd.testing.assert_frame_equal(first, second)
    assert first[first["ticker"] == "A"]["adx_minus_di"].iloc[-1] > 0.0
    assert first[first["ticker"] == "B"]["adx_plus_di"].iloc[-1] > 0.0
