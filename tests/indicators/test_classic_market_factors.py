"""Tests for the independent classic cross-sectional market-factor pack."""

from __future__ import annotations

import math

import pandas as pd
import pytest

from qrp_atlas.indicators import (
    FactorRequest,
    FactorRequestError,
    compute_average_traded_amount_factor,
    compute_average_turnover_factor,
    compute_distance_to_high_factor,
    compute_high_low_range_volatility_factor,
    compute_intermediate_momentum_factor,
    compute_short_term_reversal_factor,
    compute_turnover_change_factor,
    generate_factor_frame,
    get_factor_definition,
    normalize_trade_date,
    resolve_factor_requests,
)


def _day(value: str) -> pd.Timestamp:
    return normalize_trade_date(value)


def _market_prices() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    dates = pd.date_range("2024-01-02", periods=6, freq="D")
    a_close = [10.0, 11.0, 12.0, 9.0, 10.0, 15.0]
    a_high = [11.0, 12.0, 13.0, 10.0, 12.0, 16.0]
    a_low = [9.0, 10.0, 11.0, 8.0, 9.0, 14.0]
    b_close = [100.0, 90.0, 80.0, 70.0, 60.0, 50.0]
    for i, trade_date in enumerate(dates):
        rows.append(
            {
                "trade_date": trade_date,
                "asset_id": "A",
                "close": a_close[i],
                "high": a_high[i],
                "low": a_low[i],
                "turnover": float(i + 1),
                "amount": float((i + 1) * 100),
            }
        )
        rows.append(
            {
                "trade_date": trade_date,
                "asset_id": "B",
                "close": b_close[i],
                "high": b_close[i] + 5.0,
                "low": b_close[i] - 5.0,
                "turnover": float((i + 1) * 10),
                "amount": float((i + 1) * 1000),
            }
        )
    # Deliberately scramble both assets and dates to verify stable normalization.
    return pd.DataFrame(rows).sample(frac=1.0, random_state=7).reset_index(drop=True)


def _requests() -> list[FactorRequest]:
    return [
        FactorRequest(
            "intermediate_momentum",
            {"lookback": 4, "skip_recent": 1},
            alias="imom",
        ),
        FactorRequest("short_term_reversal", {"lookback": 2}, alias="reversal"),
        FactorRequest("distance_to_high", {"lookback": 3}, alias="high_distance"),
        FactorRequest(
            "high_low_range_volatility", {"lookback": 2}, alias="range_vol"
        ),
        FactorRequest("average_turnover", {"lookback": 3}, alias="avg_turnover"),
        FactorRequest(
            "turnover_change",
            {"recent_window": 2, "prior_window": 2},
            alias="turnover_delta",
        ),
        FactorRequest(
            "average_traded_amount", {"lookback": 3}, alias="avg_amount"
        ),
    ]


def test_market_factor_formulas_multi_asset_isolation_and_stable_sort() -> None:
    prices = _market_prices()
    out = generate_factor_frame(
        _requests(),
        trade_dates=["2024-01-07"],
        asset_ids=["B", "A"],
        prices=prices,
    )

    assert list(out["asset_id"]) == ["A", "B"]
    assert not out.duplicated(["trade_date", "asset_id"]).any()
    by_asset = out.set_index("asset_id")

    a = by_asset.loc["A"]
    assert a["imom"] == pytest.approx(10.0 / 11.0 - 1.0)
    assert a["reversal"] == pytest.approx(15.0 / 9.0 - 1.0)
    assert a["high_distance"] == pytest.approx(15.0 / 16.0 - 1.0)
    expected_range = ((12.0 - 9.0) / 10.0 + (16.0 - 14.0) / 15.0) / 2.0
    assert a["range_vol"] == pytest.approx(expected_range)
    assert a["avg_turnover"] == pytest.approx(5.0)
    assert a["turnover_delta"] == pytest.approx(5.5 / 3.5 - 1.0)
    assert a["avg_amount"] == pytest.approx(500.0)

    b = by_asset.loc["B"]
    assert b["imom"] == pytest.approx(60.0 / 90.0 - 1.0)
    assert b["reversal"] == pytest.approx(50.0 / 70.0 - 1.0)
    assert b["avg_turnover"] == pytest.approx(50.0)
    assert b["avg_amount"] == pytest.approx(5000.0)


def test_warmup_nan_invalid_values_and_explicit_zero_semantics() -> None:
    prices = pd.DataFrame(
        {
            "trade_date": pd.date_range("2024-02-01", periods=5, freq="D"),
            "asset_id": ["A"] * 5,
            "close": [10.0, 0.0, 12.0, math.inf, 15.0],
            "high": [11.0, 12.0, math.nan, 14.0, 16.0],
            "low": [9.0, 13.0, 10.0, 12.0, 14.0],
            "turnover": [0.0, 0.0, 2.0, math.inf, 4.0],
            "amount": [0.0, 0.0, 200.0, -1.0, 400.0],
        }
    )
    out = generate_factor_frame(
        [
            FactorRequest("short_term_reversal", {"lookback": 1}, alias="rev"),
            FactorRequest("distance_to_high", {"lookback": 2}, alias="dth"),
            FactorRequest(
                "high_low_range_volatility", {"lookback": 2}, alias="hlv"
            ),
            FactorRequest("average_turnover", {"lookback": 2}, alias="ato"),
            FactorRequest(
                "turnover_change",
                {"recent_window": 1, "prior_window": 1},
                alias="tchg",
            ),
            FactorRequest(
                "average_traded_amount", {"lookback": 2}, alias="aamt"
            ),
        ],
        trade_dates=prices["trade_date"].tolist(),
        asset_ids=["A"],
        prices=prices,
    ).set_index("trade_date")

    assert math.isnan(out.loc[_day("2024-02-01"), "rev"])
    assert math.isnan(out.loc[_day("2024-02-02"), "rev"])
    assert math.isnan(out.loc[_day("2024-02-03"), "dth"])
    assert math.isnan(out.loc[_day("2024-02-02"), "hlv"])
    assert out.loc[_day("2024-02-02"), "ato"] == pytest.approx(0.0)
    assert math.isnan(out.loc[_day("2024-02-02"), "tchg"])
    assert out.loc[_day("2024-02-02"), "aamt"] == pytest.approx(0.0)
    assert math.isnan(out.loc[_day("2024-02-05"), "rev"])
    assert math.isnan(out.loc[_day("2024-02-05"), "ato"])
    assert math.isnan(out.loc[_day("2024-02-05"), "aamt"])


def test_no_future_leakage_and_input_immutability() -> None:
    prices = _market_prices()
    before = prices.copy(deep=True)
    target = "2024-01-06"
    baseline = generate_factor_frame(
        _requests(), trade_dates=[target], asset_ids=["A", "B"], prices=prices
    )

    changed_future = prices.copy(deep=True)
    future_mask = changed_future["trade_date"] > _day(target)
    changed_future.loc[future_mask, ["close", "high", "low"]] = 1_000_000.0
    changed_future.loc[future_mask, ["turnover", "amount"]] = 9_000_000.0
    rerun = generate_factor_frame(
        _requests(),
        trade_dates=[target],
        asset_ids=["A", "B"],
        prices=changed_future,
    )

    pd.testing.assert_frame_equal(prices, before)
    pd.testing.assert_frame_equal(baseline, rerun)


def test_parameter_relationships_aliases_and_collisions() -> None:
    with pytest.raises(FactorRequestError, match="lookback > skip_recent"):
        resolve_factor_requests(
            [
                FactorRequest(
                    "intermediate_momentum", {"lookback": 21, "skip_recent": 21}
                )
            ]
        )
    with pytest.raises(FactorRequestError, match="must be integer"):
        resolve_factor_requests(
            [FactorRequest("average_turnover", {"lookback": True})]
        )
    with pytest.raises(FactorRequestError, match="below minimum"):
        resolve_factor_requests(
            [FactorRequest("average_traded_amount", {"lookback": 0})]
        )
    with pytest.raises(FactorRequestError, match="duplicate factor output"):
        resolve_factor_requests(
            [
                FactorRequest("average_turnover", alias="same"),
                FactorRequest("average_traded_amount", alias="same"),
            ]
        )
    with pytest.raises(FactorRequestError, match="reserved"):
        resolve_factor_requests([FactorRequest("distance_to_high", alias="asset_id")])

    resolved = resolve_factor_requests(
        [
            FactorRequest(
                "intermediate_momentum", {"lookback": 4, "skip_recent": 1}
            ),
            FactorRequest("average_turnover", {"lookback": 2}),
        ]
    )
    assert [item.output_column for item in resolved] == [
        "intermediate_momentum_lookback_4_skip_recent_1",
        "average_turnover_lookback_2",
    ]


def test_empty_universe_has_stable_columns_and_missing_inputs_raise() -> None:
    out = generate_factor_frame(
        ["intermediate_momentum", "average_turnover", "average_traded_amount"],
        trade_dates=[],
        asset_ids=[],
    )
    assert list(out.columns) == [
        "trade_date",
        "asset_id",
        "intermediate_momentum",
        "average_turnover",
        "average_traded_amount",
    ]
    assert out.empty

    with pytest.raises(FactorRequestError, match="requires a prices panel"):
        generate_factor_frame(
            ["distance_to_high"], trade_dates=["2024-01-02"], asset_ids=["A"]
        )
    with pytest.raises(FactorRequestError, match="missing required columns"):
        generate_factor_frame(
            [FactorRequest("average_turnover", {"lookback": 2})],
            trade_dates=["2024-01-03"],
            asset_ids=["A"],
            prices=pd.DataFrame(
                {
                    "trade_date": ["2024-01-02", "2024-01-03"],
                    "asset_id": ["A", "A"],
                    "close": [1.0, 2.0],
                }
            ),
        )


def test_duplicate_keys_raise_for_new_market_factors() -> None:
    prices = pd.DataFrame(
        [
            {
                "trade_date": "2024-01-02",
                "asset_id": "A",
                "turnover": 1.0,
            },
            {
                "trade_date": "2024-01-02",
                "asset_id": "A",
                "turnover": 2.0,
            },
        ]
    )
    with pytest.raises(FactorRequestError, match="duplicate"):
        generate_factor_frame(
            [FactorRequest("average_turnover", {"lookback": 1})],
            trade_dates=["2024-01-02"],
            asset_ids=["A"],
            prices=prices,
        )


def test_public_calculators_match_registry_entry_point() -> None:
    prices = _market_prices()
    universe = pd.DataFrame(
        {"trade_date": [_day("2024-01-07")], "asset_id": ["A"]}
    )
    direct_calls = [
        (
            compute_intermediate_momentum_factor(
                prices, universe=universe, lookback=4, skip_recent=1
            ),
            "intermediate_momentum",
            FactorRequest(
                "intermediate_momentum", {"lookback": 4, "skip_recent": 1}
            ),
        ),
        (
            compute_short_term_reversal_factor(
                prices, universe=universe, lookback=2
            ),
            "short_term_reversal",
            FactorRequest("short_term_reversal", {"lookback": 2}),
        ),
        (
            compute_distance_to_high_factor(prices, universe=universe, lookback=3),
            "distance_to_high",
            FactorRequest("distance_to_high", {"lookback": 3}),
        ),
        (
            compute_high_low_range_volatility_factor(
                prices, universe=universe, lookback=2
            ),
            "high_low_range_volatility",
            FactorRequest("high_low_range_volatility", {"lookback": 2}),
        ),
        (
            compute_average_turnover_factor(prices, universe=universe, lookback=3),
            "average_turnover",
            FactorRequest("average_turnover", {"lookback": 3}),
        ),
        (
            compute_turnover_change_factor(
                prices, universe=universe, recent_window=2, prior_window=2
            ),
            "turnover_change",
            FactorRequest(
                "turnover_change", {"recent_window": 2, "prior_window": 2}
            ),
        ),
        (
            compute_average_traded_amount_factor(
                prices, universe=universe, lookback=3
            ),
            "average_traded_amount",
            FactorRequest("average_traded_amount", {"lookback": 3}),
        ),
    ]
    for direct, output_column, request in direct_calls:
        via_registry = generate_factor_frame(
            [FactorRequest(request.code, request.parameters, alias=output_column)],
            universe=universe,
            prices=prices,
        )
        pd.testing.assert_frame_equal(direct, via_registry)


def test_market_factor_metadata_documents_formula_direction_and_timing() -> None:
    codes = {
        "intermediate_momentum",
        "short_term_reversal",
        "distance_to_high",
        "high_low_range_volatility",
        "average_turnover",
        "turnover_change",
        "average_traded_amount",
    }
    for code in codes:
        definition = get_factor_definition(code)
        assert definition.formula
        assert definition.direction
        assert "T+1" in definition.time_semantics
        assert definition.inputs
        assert "NaN" in definition.nan_semantics
