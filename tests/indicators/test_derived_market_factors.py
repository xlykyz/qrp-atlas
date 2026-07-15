from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from qrp_atlas.indicators import (
    FACTOR_DEFINITIONS,
    FactorRequest,
    FactorRequestError,
    calculate_indicators,
    compute_amihud_illiquidity_factor,
    compute_downside_volatility_factor,
    compute_price_efficiency_factor,
    compute_price_volume_correlation_factor,
    compute_realized_volatility_factor,
    compute_relative_volume_factor,
    compute_rolling_max_drawdown_factor,
    compute_trend_r_squared_factor,
    compute_trend_slope_factor,
    generate_factor_frame,
    get_calculation_definition,
    get_factor_definition,
)
from qrp_atlas.indicators.parameterized import IndicatorRequest


NEW_FACTOR_SOURCES = {
    "trend_slope": ("linear_regression_trend", "normalized_slope"),
    "trend_r_squared": ("linear_regression_trend", "r_squared"),
    "price_efficiency": ("kaufman_efficiency_ratio", "value"),
    "realized_volatility": ("return_volatility", "value"),
    "downside_volatility": ("downside_volatility", "value"),
    "rolling_max_drawdown": ("rolling_max_drawdown", "value"),
    "relative_volume": ("relative_volume", "value"),
    "amihud_illiquidity": ("amihud_illiquidity", "value"),
    "price_volume_correlation": ("price_volume_correlation", "value"),
}

EXPECTED_FACTOR_CODES = {
    "momentum",
    "intermediate_momentum",
    "short_term_reversal",
    "distance_to_high",
    "dividend_yield_ttm",
    "earnings_yield_ttm",
    "free_float_turnover_rate",
    "trend_slope",
    "trend_r_squared",
    "price_efficiency",
    "realized_volatility",
    "downside_volatility",
    "rolling_max_drawdown",
    "high_low_range_volatility",
    "average_turnover",
    "turnover_change",
    "relative_volume",
    "average_traded_amount",
    "amihud_illiquidity",
    "price_volume_correlation",
    "log_market_cap",
    "roe",
    "book_to_price",
    "sales_to_price_ttm",
    "turnover_rate",
    "volume_ratio",
}


def _day(value: str) -> pd.Timestamp:
    return pd.Timestamp(value)


def _asset_prices(asset_id: str = "A") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "trade_date": pd.date_range("2024-01-01", periods=6),
            "asset_id": [asset_id] * 6,
            "close": [100.0, 110.0, 99.0, 120.0, 108.0, 130.0],
            "volume": [100.0, 120.0, 90.0, 180.0, 150.0, 240.0],
            "amount": [10000.0, 12000.0, 9000.0, 18000.0, 15000.0, 24000.0],
        }
    )


def _requests(window: int = 3) -> list[FactorRequest]:
    return [
        FactorRequest("trend_slope", {"window": window}, alias="trend_slope"),
        FactorRequest("trend_r_squared", {"window": window}, alias="trend_r_squared"),
        FactorRequest("price_efficiency", {"window": window}, alias="price_efficiency"),
        FactorRequest(
            "realized_volatility",
            {"window": window, "annualization": 252.0},
            alias="realized_volatility",
        ),
        FactorRequest(
            "downside_volatility",
            {"window": window, "annualization": 252.0, "target": 0.0},
            alias="downside_volatility",
        ),
        FactorRequest(
            "rolling_max_drawdown",
            {"window": window},
            alias="rolling_max_drawdown",
        ),
        FactorRequest("relative_volume", {"window": window}, alias="relative_volume"),
        FactorRequest(
            "amihud_illiquidity",
            {"window": window, "scale": 1.0},
            alias="amihud_illiquidity",
        ),
        FactorRequest(
            "price_volume_correlation",
            {"window": window},
            alias="price_volume_correlation",
        ),
    ]


def test_complete_factor_registration_and_indicator_parameter_contracts() -> None:
    assert set(FACTOR_DEFINITIONS) == EXPECTED_FACTOR_CODES
    for factor_code, (indicator_code, _) in NEW_FACTOR_SOURCES.items():
        factor = get_factor_definition(factor_code)
        indicator = get_calculation_definition(indicator_code)
        assert set(factor.parameter_schema) == set(indicator.parameter_schema)
        for name, indicator_spec in indicator.parameter_schema.items():
            factor_spec = factor.parameter_schema[name]
            assert factor_spec.type == indicator_spec.type
            assert factor_spec.default == indicator_spec.default
            assert factor_spec.has_default == indicator_spec.has_default
            assert factor_spec.minimum == indicator_spec.minimum
            assert factor_spec.maximum == indicator_spec.maximum
        assert factor.formula
        assert factor.direction
        assert "T+1" in factor.time_semantics
        assert "NaN" in factor.nan_semantics


def test_manual_formulas_for_all_derived_market_factors() -> None:
    prices = _asset_prices()
    out = generate_factor_frame(
        _requests(),
        trade_dates=["2024-01-06"],
        asset_ids=["A"],
        prices=prices,
    ).iloc[0]

    close_window = np.array([120.0, 108.0, 130.0])
    x = np.arange(3, dtype=float)
    x_centered = x - x.mean()
    y_centered = close_window - close_window.mean()
    slope = float(np.dot(x_centered, y_centered) / np.dot(x_centered, x_centered))
    fitted_centered = slope * x_centered
    r_squared = 1.0 - float(
        np.dot(y_centered - fitted_centered, y_centered - fitted_centered)
        / np.dot(y_centered, y_centered)
    )
    returns = np.array([120.0 / 99.0 - 1.0, 108.0 / 120.0 - 1.0, 130.0 / 108.0 - 1.0])
    volume_changes = np.array(
        [180.0 / 90.0 - 1.0, 150.0 / 180.0 - 1.0, 240.0 / 150.0 - 1.0]
    )
    amounts = np.array([18000.0, 15000.0, 24000.0])

    assert out["trend_slope"] == pytest.approx(slope / np.mean(np.abs(close_window)))
    assert out["trend_r_squared"] == pytest.approx(r_squared)
    assert out["price_efficiency"] == pytest.approx(abs(130.0 - 99.0) / (21.0 + 12.0 + 22.0))
    assert out["realized_volatility"] == pytest.approx(np.std(returns, ddof=0) * math.sqrt(252.0))
    assert out["downside_volatility"] == pytest.approx(
        math.sqrt(np.mean(np.minimum(returns, 0.0) ** 2)) * math.sqrt(252.0)
    )
    assert out["rolling_max_drawdown"] == pytest.approx(-0.1)
    assert out["relative_volume"] == pytest.approx(240.0 / np.mean([90.0, 180.0, 150.0]))
    assert out["amihud_illiquidity"] == pytest.approx(np.mean(np.abs(returns) / amounts))
    assert out["price_volume_correlation"] == pytest.approx(
        np.corrcoef(returns, volume_changes)[0, 1]
    )


def test_factor_values_equal_the_existing_indicator_outputs() -> None:
    prices = _asset_prices()
    indicator_input = prices.rename(columns={"asset_id": "ticker"})
    indicator_requests = [
        IndicatorRequest(
            "linear_regression_trend",
            {"window": 3},
            alias="linear",
            output_fields={
                "slope": "unused_slope",
                "normalized_slope": "trend_slope",
                "r_squared": "trend_r_squared",
            },
        ),
        IndicatorRequest("kaufman_efficiency_ratio", {"window": 3}, alias="price_efficiency"),
        IndicatorRequest(
            "return_volatility",
            {"window": 3, "annualization": 252.0},
            alias="realized_volatility",
        ),
        IndicatorRequest(
            "downside_volatility",
            {"window": 3, "annualization": 252.0, "target": 0.0},
            alias="downside_volatility",
        ),
        IndicatorRequest("rolling_max_drawdown", {"window": 3}, alias="rolling_max_drawdown"),
        IndicatorRequest("relative_volume", {"window": 3}, alias="relative_volume"),
        IndicatorRequest(
            "amihud_illiquidity",
            {"window": 3, "scale": 1.0},
            alias="amihud_illiquidity",
        ),
        IndicatorRequest(
            "price_volume_correlation",
            {"window": 3},
            alias="price_volume_correlation",
        ),
    ]
    indicators = calculate_indicators(indicator_input, indicator_requests)
    expected = indicators.loc[indicators["trade_date"] == _day("2024-01-06")].iloc[0]
    factors = generate_factor_frame(
        _requests(),
        trade_dates=["2024-01-06"],
        asset_ids=["A"],
        prices=prices,
    ).iloc[0]
    for factor_code in NEW_FACTOR_SOURCES:
        assert factors[factor_code] == pytest.approx(expected[factor_code])


def test_public_factor_calculators_match_generate_factor_frame() -> None:
    prices = _asset_prices()
    universe = pd.DataFrame(
        {"trade_date": [_day("2024-01-06")], "asset_id": ["A"]}
    )
    calls = [
        (compute_trend_slope_factor, {"window": 3}, "trend_slope"),
        (compute_trend_r_squared_factor, {"window": 3}, "trend_r_squared"),
        (compute_price_efficiency_factor, {"window": 3}, "price_efficiency"),
        (
            compute_realized_volatility_factor,
            {"window": 3, "annualization": 252.0},
            "realized_volatility",
        ),
        (
            compute_downside_volatility_factor,
            {"window": 3, "annualization": 252.0, "target": 0.0},
            "downside_volatility",
        ),
        (
            compute_rolling_max_drawdown_factor,
            {"window": 3},
            "rolling_max_drawdown",
        ),
        (compute_relative_volume_factor, {"window": 3}, "relative_volume"),
        (
            compute_amihud_illiquidity_factor,
            {"window": 3, "scale": 1.0},
            "amihud_illiquidity",
        ),
        (
            compute_price_volume_correlation_factor,
            {"window": 3},
            "price_volume_correlation",
        ),
    ]
    for calculator, parameters, factor_code in calls:
        direct = calculator(prices, universe=universe, **parameters)
        via_registry = generate_factor_frame(
            [FactorRequest(factor_code, parameters, alias=factor_code)],
            universe=universe,
            prices=prices,
        )
        pd.testing.assert_frame_equal(direct, via_registry)


def test_window_boundaries_are_explicit() -> None:
    prices = _asset_prices()
    dates = prices["trade_date"].tolist()
    out = generate_factor_frame(
        _requests(), trade_dates=dates, asset_ids=["A"], prices=prices
    ).set_index("trade_date")

    third = out.loc[_day("2024-01-03")]
    fourth = out.loc[_day("2024-01-04")]
    assert pd.notna(third["trend_slope"])
    assert pd.notna(third["trend_r_squared"])
    assert pd.notna(third["rolling_max_drawdown"])
    for code in {
        "price_efficiency",
        "realized_volatility",
        "downside_volatility",
        "relative_volume",
        "amihud_illiquidity",
        "price_volume_correlation",
    }:
        assert pd.isna(third[code])
        assert pd.notna(fourth[code])


def test_multi_asset_isolation_unsorted_input_stable_sort_and_immutability() -> None:
    a = _asset_prices("A")
    b = _asset_prices("B")
    b["close"] = [40.0, 38.0, 41.0, 37.0, 43.0, 39.0]
    b["volume"] = [70.0, 100.0, 80.0, 120.0, 90.0, 160.0]
    b["amount"] = [4000.0, 3800.0, 4100.0, 3700.0, 4300.0, 3900.0]
    mixed = pd.concat([a, b], ignore_index=True).sample(frac=1.0, random_state=7).reset_index(drop=True)
    before = mixed.copy(deep=True)
    dates = list(reversed(a["trade_date"].tolist()))

    combined = generate_factor_frame(
        _requests(), trade_dates=dates, asset_ids=["B", "A"], prices=mixed
    )
    a_only = generate_factor_frame(
        _requests(), trade_dates=dates, asset_ids=["A"], prices=a
    )

    pd.testing.assert_frame_equal(mixed, before)
    assert not combined.duplicated(["trade_date", "asset_id"]).any()
    assert combined[["trade_date", "asset_id"]].to_records(index=False).tolist() == sorted(
        combined[["trade_date", "asset_id"]].to_records(index=False).tolist()
    )
    pd.testing.assert_frame_equal(
        combined.loc[combined["asset_id"] == "A"].reset_index(drop=True), a_only
    )


def test_future_rows_do_not_change_historical_factor_values() -> None:
    prices = _asset_prices()
    target = ["2024-01-05"]
    historical = generate_factor_frame(
        _requests(), trade_dates=target, asset_ids=["A"], prices=prices.iloc[:5]
    )
    with_future = prices.copy()
    with_future.loc[5, ["close", "volume", "amount"]] = [1_000_000.0, 1.0, 1.0]
    recomputed = generate_factor_frame(
        _requests(), trade_dates=target, asset_ids=["A"], prices=with_future
    )
    pd.testing.assert_frame_equal(historical, recomputed)


def test_nan_inf_zero_and_negative_semantics() -> None:
    invalid = _asset_prices()
    invalid.loc[5, "close"] = math.inf
    invalid.loc[5, "volume"] = -math.inf
    invalid.loc[5, "amount"] = 0.0
    row = generate_factor_frame(
        _requests(), trade_dates=["2024-01-06"], asset_ids=["A"], prices=invalid
    ).iloc[0]
    for code in NEW_FACTOR_SOURCES:
        assert pd.isna(row[code])

    zero_current_volume = _asset_prices()
    zero_current_volume.loc[5, "volume"] = 0.0
    row = generate_factor_frame(
        [FactorRequest("relative_volume", {"window": 3}, alias="rv")],
        trade_dates=["2024-01-06"],
        asset_ids=["A"],
        prices=zero_current_volume,
    ).iloc[0]
    assert row["rv"] == 0.0

    zero_baseline = _asset_prices()
    zero_baseline.loc[2:4, "volume"] = 0.0
    row = generate_factor_frame(
        [FactorRequest("relative_volume", {"window": 3}, alias="rv")],
        trade_dates=["2024-01-06"],
        asset_ids=["A"],
        prices=zero_baseline,
    ).iloc[0]
    assert pd.isna(row["rv"])

    invalid_price = _asset_prices()
    invalid_price.loc[4, "close"] = -1.0
    row = generate_factor_frame(
        [
            FactorRequest("realized_volatility", {"window": 2}, alias="rv"),
            FactorRequest("rolling_max_drawdown", {"window": 2}, alias="mdd"),
            FactorRequest("amihud_illiquidity", {"window": 2}, alias="amihud"),
        ],
        trade_dates=["2024-01-06"],
        asset_ids=["A"],
        prices=invalid_price,
    ).iloc[0]
    assert pd.isna(row["rv"])
    assert pd.isna(row["mdd"])
    assert pd.isna(row["amihud"])


def test_parameter_and_alias_conflicts_are_rejected() -> None:
    prices = _asset_prices()
    invalid_requests = [
        FactorRequest("trend_slope", {"window": 1}),
        FactorRequest("realized_volatility", {"annualization": 0.0}),
        FactorRequest("downside_volatility", {"target": math.inf}),
        FactorRequest("amihud_illiquidity", {"scale": 0.0}),
        FactorRequest("relative_volume", {"unknown": 1}),
    ]
    for request in invalid_requests:
        with pytest.raises(FactorRequestError):
            generate_factor_frame(
                [request], trade_dates=["2024-01-06"], asset_ids=["A"], prices=prices
            )

    with pytest.raises(FactorRequestError, match="duplicate factor output"):
        generate_factor_frame(
            [
                FactorRequest("trend_slope", {"window": 3}, alias="same"),
                FactorRequest("trend_r_squared", {"window": 3}, alias="same"),
            ],
            trade_dates=["2024-01-06"],
            asset_ids=["A"],
            prices=prices,
        )


def test_empty_universe_missing_fields_and_duplicate_keys() -> None:
    empty = generate_factor_frame(
        list(NEW_FACTOR_SOURCES), trade_dates=[], asset_ids=[]
    )
    assert empty.empty
    assert list(empty.columns) == ["trade_date", "asset_id", *NEW_FACTOR_SOURCES]

    with pytest.raises(FactorRequestError, match="missing required columns"):
        generate_factor_frame(
            ["relative_volume"],
            trade_dates=["2024-01-06"],
            asset_ids=["A"],
            prices=_asset_prices().drop(columns="volume"),
        )

    duplicated = pd.concat([_asset_prices(), _asset_prices().iloc[[0]]], ignore_index=True)
    with pytest.raises(FactorRequestError, match="duplicate"):
        generate_factor_frame(
            ["trend_slope"],
            trade_dates=["2024-01-06"],
            asset_ids=["A"],
            prices=duplicated,
        )
