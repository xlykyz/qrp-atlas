from __future__ import annotations

import math

import pandas as pd
import pytest

from qrp_atlas.indicators import (
    IndicatorConflictError,
    IndicatorRequest,
    IndicatorRequestError,
    UnknownIndicatorError,
    calculate_indicators,
    resolve_indicator_requests,
)
from qrp_atlas.indicators.stock import calculate_stock_trend
from qrp_atlas.indicators.system_b import calculate_system_b_basic_states_from_prices


def _prices() -> pd.DataFrame:
    rows = []
    for ticker, closes in (("B", [100, 100, 100, 100, 100]), ("A", [1, 2, 3, 4, 5])):
        for day, close in enumerate(closes, 1):
            rows.append(
                {
                    "ticker": ticker,
                    "trade_date": f"2024-01-{day:02d}",
                    "open": close,
                    "high": close + 1,
                    "low": close - 1,
                    "close": close,
                }
            )
    return pd.DataFrame(rows).sample(frac=1, random_state=17).reset_index(drop=True)


def test_same_indicator_supports_multiple_windows_aliases_and_explicit_output() -> None:
    result = calculate_indicators(
        _prices(),
        (
            IndicatorRequest("sma", {"window": 2}, alias="fast"),
            IndicatorRequest(
                "sma", {"window": 4}, alias="slow_request", output_fields={"value": "slow"}
            ),
        ),
    )
    asset = result[result["ticker"] == "A"].reset_index(drop=True)
    assert math.isnan(asset.loc[0, "fast"])
    assert asset.loc[1, "fast"] == 1.5
    assert math.isnan(asset.loc[2, "slow"])
    assert asset.loc[3, "slow"] == 2.5
    assert result[["ticker", "trade_date"]].values.tolist() == sorted(
        result[["ticker", "trade_date"]].values.tolist()
    )


def test_default_alias_is_stable_and_ticker_groups_are_independent() -> None:
    request = IndicatorRequest("period_return", {"lookback": 2})
    first = calculate_indicators(_prices(), (request,))
    second = calculate_indicators(_prices().sample(frac=1, random_state=99), (request,))
    assert "period_return_lookback_2" in first
    pd.testing.assert_frame_equal(first, second)
    flat = first[first["ticker"] == "B"]["period_return_lookback_2"].dropna()
    assert flat.eq(0.0).all()


def test_alias_and_output_conflicts_are_rejected() -> None:
    with pytest.raises(IndicatorConflictError, match="duplicate indicator alias"):
        resolve_indicator_requests(
            (
                IndicatorRequest("sma", {"window": 2}, alias="same"),
                IndicatorRequest("sma", {"window": 3}, alias="same"),
            )
        )
    with pytest.raises(IndicatorConflictError, match="output field conflict"):
        calculate_indicators(_prices(), (IndicatorRequest("sma", {"window": 2}, alias="close"),))


@pytest.mark.parametrize(
    "indicator_request,error",
    [
        (IndicatorRequest("missing"), UnknownIndicatorError),
        (IndicatorRequest("sma", {"window": 1}), IndicatorRequestError),
        (IndicatorRequest("sma", {"window": 2, "extra": 1}), IndicatorRequestError),
        (IndicatorRequest("sma", {"window": 2}, alias="bad alias"), IndicatorRequestError),
        (IndicatorRequest("sma", {"window": 2}, output_fields={"other": "x"}), IndicatorRequestError),
    ],
)
def test_unknown_indicator_and_invalid_requests_fail_explicitly(indicator_request, error) -> None:
    with pytest.raises(error):
        resolve_indicator_requests((indicator_request,))


def test_donchian_excludes_current_bar_and_has_full_warmup() -> None:
    prices = _prices()
    prices.loc[(prices["ticker"] == "A") & (prices["trade_date"] == "2024-01-04"), "high"] = 999
    result = calculate_indicators(
        prices,
        (
            IndicatorRequest("donchian_high", {"window": 3}, alias="upper"),
            IndicatorRequest("donchian_low", {"window": 2}, alias="lower"),
        ),
    )
    asset = result[result["ticker"] == "A"].reset_index(drop=True)
    assert asset["upper"].iloc[:3].isna().all()
    assert asset.loc[3, "upper"] == 4  # prior highs are 2, 3, 4; current 999 is excluded
    assert asset.loc[4, "upper"] == 999
    assert asset["lower"].iloc[:2].isna().all()


def test_rolling_zscore_uses_only_available_values_and_zero_std_is_nan() -> None:
    request = IndicatorRequest("rolling_zscore", {"window": 3}, alias="z")
    original = calculate_indicators(_prices(), (request,))
    changed = _prices()
    changed.loc[(changed["ticker"] == "A") & (changed["trade_date"] == "2024-01-05"), "close"] = 500
    changed = calculate_indicators(changed, (request,))
    a_original = original[original["ticker"] == "A"].reset_index(drop=True)
    a_changed = changed[changed["ticker"] == "A"].reset_index(drop=True)
    pd.testing.assert_series_equal(a_original.loc[:3, "z"], a_changed.loc[:3, "z"])
    assert original[original["ticker"] == "B"]["z"].isna().all()


def test_rolling_mean_and_population_std_publish_defaults_and_warmup() -> None:
    result = calculate_indicators(
        _prices(),
        (
            IndicatorRequest("rolling_mean", {"window": 2}, alias="mean"),
            IndicatorRequest("rolling_std", {"window": 2}, alias="std"),
        ),
    )
    asset = result[result["ticker"] == "A"].reset_index(drop=True)
    assert math.isnan(asset.loc[0, "mean"])
    assert asset.loc[1, "mean"] == 1.5
    assert asset.loc[1, "std"] == 0.5


def test_legacy_compatibility_indicators_match_public_calculators_row_by_row() -> None:
    """Compatibility registry entries delegate to the established indicator APIs."""

    rows = []
    for ticker, closes in (
        ("A", [10, 10, 10, 10, 10, 10, 11, 11]),
        ("B", [20, 20, 20, 20, 20, 19, 19, 20]),
    ):
        for day, close in enumerate(closes, 1):
            rows.append(
                {
                    "ticker": ticker,
                    "trade_date": f"2024-02-{day:02d}",
                    "open": close,
                    "high": close + 1,
                    "low": close - 1,
                    "close": close,
                }
            )
    prices = pd.DataFrame(rows).sample(frac=1, random_state=23).reset_index(drop=True)

    trend_columns = (
        "ma5",
        "close_above_ma5",
        "close_below_ma5",
        "close_above_ma5_days",
        "close_below_ma5_days",
    )
    system_b_columns = ("system_b_trend_valid", "system_b_exit_triggered")
    actual = calculate_indicators(
        prices,
        (
            IndicatorRequest(
                "stock_trend_legacy",
                alias="trend",
                output_fields={column: column for column in trend_columns},
            ),
            IndicatorRequest(
                "system_b_states",
                alias="system_b",
                output_fields={column: column for column in system_b_columns},
            ),
        ),
    )
    expected_trend = calculate_stock_trend(prices)
    expected_system_b = calculate_system_b_basic_states_from_prices(prices)

    pd.testing.assert_frame_equal(
        actual.loc[:, trend_columns].reset_index(drop=True),
        expected_trend.loc[:, trend_columns].reset_index(drop=True),
    )
    pd.testing.assert_frame_equal(
        actual.loc[:, system_b_columns].reset_index(drop=True),
        expected_system_b.loc[:, system_b_columns].reset_index(drop=True),
    )

    asset_a = actual[actual["ticker"] == "A"].reset_index(drop=True)
    asset_b = actual[actual["ticker"] == "B"].reset_index(drop=True)
    assert asset_a["ma5"].iloc[:4].isna().all()  # warm-up
    assert not asset_a.loc[4, "close_above_ma5"]  # close equals MA5
    assert asset_a.loc[7, "system_b_trend_valid"]  # two consecutive days at/above MA5
    assert asset_b.loc[6, "system_b_exit_triggered"]  # two consecutive days below MA5
