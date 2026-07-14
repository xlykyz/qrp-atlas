from __future__ import annotations

import math

import pandas as pd
import pytest

from qrp_atlas.indicators import IndicatorRequest, IndicatorRequestError, calculate_indicators


def _frame(closes: list[float], ticker: str = "A") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ticker": ticker,
            "trade_date": pd.date_range("2024-01-01", periods=len(closes)),
            "high": [value + 1.0 for value in closes],
            "low": [value - 1.0 for value in closes],
            "close": closes,
        }
    )


def test_moving_averages_macd_regression_and_efficiency_are_hand_verifiable() -> None:
    frame = _frame([1.0, 2.0, 3.0, 4.0])
    result = calculate_indicators(
        frame,
        (
            IndicatorRequest("ema", {"window": 3}, alias="ema"),
            IndicatorRequest("wma", {"window": 3}, alias="wma"),
            IndicatorRequest(
                "macd",
                {"fast_window": 2, "slow_window": 3, "signal_window": 2},
                alias="macd",
            ),
            IndicatorRequest("linear_regression_trend", {"window": 3}, alias="trend"),
            IndicatorRequest("kaufman_efficiency_ratio", {"window": 2}, alias="er"),
        ),
    )

    assert result["ema"].iloc[:2].isna().all()
    assert result.loc[2, "ema"] == pytest.approx(2.0)
    assert result.loc[3, "ema"] == pytest.approx(3.0)
    assert result.loc[2, "wma"] == pytest.approx(14.0 / 6.0)
    assert result.loc[3, "wma"] == pytest.approx(20.0 / 6.0)
    assert result.loc[2, "macd_line"] == pytest.approx(0.5)
    assert result.loc[3, "macd_signal"] == pytest.approx(0.5)
    assert result.loc[3, "macd_histogram"] == pytest.approx(0.0)
    assert result.loc[2, "trend_slope"] == pytest.approx(1.0)
    assert result.loc[2, "trend_normalized_slope"] == pytest.approx(0.5)
    assert result.loc[2, "trend_r_squared"] == pytest.approx(1.0)
    assert result.loc[2, "er"] == pytest.approx(1.0)


def test_roc_is_a_compatibility_entry_for_existing_period_return() -> None:
    result = calculate_indicators(
        _frame([10.0, 12.0, 15.0]),
        (
            IndicatorRequest("roc", {"lookback": 2}, alias="roc"),
            IndicatorRequest("period_return", {"lookback": 2}, alias="period_return"),
        ),
    )
    pd.testing.assert_series_equal(result["roc"], result["period_return"], check_names=False)
    assert result.loc[2, "roc"] == pytest.approx(0.5)


def test_classic_trend_indicators_are_order_stable_isolated_and_input_immutable() -> None:
    original = pd.concat([_frame([1.0, 2.0, 3.0], "B"), _frame([10.0, 10.0, 10.0], "A")])
    shuffled = original.sample(frac=1.0, random_state=7).reset_index(drop=True)
    snapshot = shuffled.copy(deep=True)
    requests = (
        IndicatorRequest("ema", {"window": 2}, alias="ema"),
        IndicatorRequest("kaufman_efficiency_ratio", {"window": 2}, alias="er"),
    )
    first = calculate_indicators(shuffled, requests)
    second = calculate_indicators(shuffled.sample(frac=1.0, random_state=11), requests)
    pd.testing.assert_frame_equal(first, second)
    pd.testing.assert_frame_equal(shuffled, snapshot)
    assert math.isnan(first[first["ticker"] == "A"]["er"].iloc[-1])
    assert first[first["ticker"] == "B"]["er"].iloc[-1] == pytest.approx(1.0)


def test_macd_rejects_invalid_window_relationship() -> None:
    with pytest.raises(IndicatorRequestError, match="fast_window must be less"):
        calculate_indicators(
            _frame([1.0, 2.0, 3.0]),
            (IndicatorRequest("macd", {"fast_window": 3, "slow_window": 3}),),
        )


def test_all_classic_calculations_preserve_empty_schema_and_missing_fields_fail() -> None:
    empty = pd.DataFrame(
        columns=["ticker", "trade_date", "high", "low", "close", "volume", "amount"]
    )
    codes = [
        "ema", "wma", "macd", "linear_regression_trend",
        "kaufman_efficiency_ratio", "true_range", "atr", "bollinger_bands",
        "keltner_channel", "return_volatility", "downside_volatility",
        "rolling_current_drawdown", "rolling_max_drawdown", "ulcer_index",
        "rsi", "stochastic_oscillator", "williams_r", "cci", "adx",
        "obv", "rolling_vwap", "volume_sma", "relative_volume", "mfi",
        "cmf", "amihud_illiquidity", "price_volume_correlation",
        "atr_breakout_bands",
    ]
    result = calculate_indicators(
        empty, tuple(IndicatorRequest(code, alias=f"indicator_{index}") for index, code in enumerate(codes))
    )
    assert result.empty
    assert list(result.columns[:7]) == list(empty.columns)
    assert len(result.columns) > len(empty.columns)

    with pytest.raises(IndicatorRequestError, match="missing required fields"):
        calculate_indicators(
            empty.drop(columns=["volume"]),
            (IndicatorRequest("relative_volume", {"window": 2}),),
        )
