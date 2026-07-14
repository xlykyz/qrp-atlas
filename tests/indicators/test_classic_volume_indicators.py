from __future__ import annotations

import math

import pandas as pd
import pytest

from qrp_atlas.indicators import IndicatorRequest, calculate_indicators


def _frame(closes, volumes, amounts=None, highs=None, lows=None):
    highs = highs or [value + 1.0 for value in closes]
    lows = lows or [value - 1.0 for value in closes]
    amounts = amounts or [close * volume for close, volume in zip(closes, volumes)]
    return pd.DataFrame(
        {
            "ticker": "A",
            "trade_date": pd.date_range("2024-01-01", periods=len(closes)),
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volumes,
            "amount": amounts,
        }
    )


def test_obv_vwap_volume_average_and_relative_volume() -> None:
    frame = _frame([10.0, 11.0, 10.0], [100.0, 200.0, 300.0])
    result = calculate_indicators(
        frame,
        (
            IndicatorRequest("obv", alias="obv"),
            IndicatorRequest("rolling_vwap", {"window": 2}, alias="vwap"),
            IndicatorRequest("volume_sma", {"window": 2}, alias="volume_sma"),
            IndicatorRequest("relative_volume", {"window": 2}, alias="relative_volume"),
        ),
    )
    assert result["obv"].tolist() == pytest.approx([0.0, 200.0, -100.0])
    assert result.loc[1, "vwap"] == pytest.approx((10.0 * 100.0 + 11.0 * 200.0) / 300.0)
    assert result.loc[1, "volume_sma"] == pytest.approx(150.0)
    assert result.loc[2, "relative_volume"] == pytest.approx(2.0)


def test_money_flow_cmf_and_amihud_formulas() -> None:
    frame = _frame(
        [10.0, 11.0, 12.0],
        [100.0, 100.0, 100.0],
        amounts=[10.0, 10.0, 10.0],
        highs=[12.0, 13.0, 14.0],
        lows=[8.0, 9.0, 10.0],
    )
    result = calculate_indicators(
        frame,
        (
            IndicatorRequest("mfi", {"window": 2}, alias="mfi"),
            IndicatorRequest("cmf", {"window": 2}, alias="cmf"),
            IndicatorRequest("amihud_illiquidity", {"window": 2, "scale": 1.0}, alias="amihud"),
        ),
    )
    assert result.loc[1, "mfi"] == pytest.approx(100.0)
    assert result.loc[2, "cmf"] == pytest.approx(0.0)
    expected = ((11.0 / 10.0 - 1.0) / 10.0 + (12.0 / 11.0 - 1.0) / 10.0) / 2.0
    assert result.loc[2, "amihud"] == pytest.approx(expected)


def test_price_volume_correlation_and_nonfinite_zero_volume_semantics() -> None:
    frame = _frame([100.0, 110.0, 132.0], [100.0, 110.0, 132.0])
    result = calculate_indicators(
        frame,
        (IndicatorRequest("price_volume_correlation", {"window": 2}, alias="correlation"),),
    )
    assert result.loc[2, "correlation"] == pytest.approx(1.0)

    invalid = _frame([10.0, 11.0, 12.0], [0.0, 0.0, 0.0], amounts=[0.0, math.inf, 0.0])
    output = calculate_indicators(
        invalid,
        (
            IndicatorRequest("rolling_vwap", {"window": 2}, alias="vwap"),
            IndicatorRequest("relative_volume", {"window": 2}, alias="rvol"),
            IndicatorRequest("amihud_illiquidity", {"window": 2}, alias="amihud"),
        ),
    )
    assert output["vwap"].isna().all()
    assert output["rvol"].isna().all()
    assert output["amihud"].isna().all()
