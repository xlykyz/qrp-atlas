"""Regression coverage for same-day valuation and trading-activity factors."""

from __future__ import annotations

import math

import pandas as pd
import pytest

from qrp_atlas.indicators import (
    FACTOR_DEFINITIONS,
    FactorRequest,
    FactorRequestError,
    UnknownFactorError,
    compute_daily_basic_factor,
    compute_log_market_cap_factor,
    generate_factor_frame,
    get_factor_definition,
    list_factors,
)
from qrp_atlas.indicators.cross_section.universe import build_historical_universe


REGISTERED_DAILY_BASIC_FACTORS = (
    "earnings_yield_ttm",
    "sales_to_price_ttm",
    "dividend_yield_ttm",
    "turnover_rate",
    "free_float_turnover_rate",
    "volume_ratio",
)
FACTOR_OUTPUTS = (
    "earnings_yield_ttm",
    "sales_to_price_ttm",
    "dividend_yield_ttm",
    "log_total_market_cap",
    "log_circulating_market_cap",
    "turnover_rate",
    "free_float_turnover_rate",
    "volume_ratio",
)


def _factor_requests() -> list[str | FactorRequest]:
    return [
        "earnings_yield_ttm",
        "sales_to_price_ttm",
        "dividend_yield_ttm",
        FactorRequest(
            "log_market_cap",
            {"field": "total_mv"},
            alias="log_total_market_cap",
        ),
        FactorRequest(
            "log_market_cap",
            {"field": "circ_mv"},
            alias="log_circulating_market_cap",
        ),
        "turnover_rate",
        "free_float_turnover_rate",
        "volume_ratio",
    ]


def _day(value: str) -> pd.Timestamp:
    return pd.Timestamp(value).normalize()


def _daily_basic_panel() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": "2024-01-03",
                "asset_id": "B",
                "pe_ttm": 8.0,
                "ps_ttm": 4.0,
                "dv_ttm": 0.0,
                "total_mv": 400.0,
                "circ_mv": 160.0,
                "turnover_rate": 1.0,
                "turnover_rate_f": 2.0,
                "volume_ratio": 0.8,
            },
            {
                "trade_date": "2024-01-02",
                "asset_id": "A",
                "pe_ttm": 10.0,
                "ps_ttm": 2.0,
                "dv_ttm": 3.5,
                "total_mv": 100.0,
                "circ_mv": 40.0,
                "turnover_rate": 2.5,
                "turnover_rate_f": 5.0,
                "volume_ratio": 1.2,
            },
            {
                "trade_date": "2024-01-02",
                "asset_id": "B",
                "pe_ttm": 20.0,
                "ps_ttm": 5.0,
                "dv_ttm": 1.0,
                "total_mv": 200.0,
                "circ_mv": 80.0,
                "turnover_rate": 4.0,
                "turnover_rate_f": 8.0,
                "volume_ratio": 1.5,
            },
            {
                "trade_date": "2024-01-03",
                "asset_id": "A",
                "pe_ttm": 5.0,
                "ps_ttm": 2.5,
                "dv_ttm": 4.0,
                "total_mv": 150.0,
                "circ_mv": 60.0,
                "turnover_rate": 3.0,
                "turnover_rate_f": 6.0,
                "volume_ratio": 1.8,
            },
        ]
    )


def test_manual_formulas_and_units_for_all_daily_basic_factors() -> None:
    row = generate_factor_frame(
        _factor_requests(),
        trade_dates=["2024-01-02"],
        asset_ids=["A"],
        daily_basic_panel=_daily_basic_panel(),
    ).iloc[0]

    assert row["earnings_yield_ttm"] == pytest.approx(0.1)
    assert row["sales_to_price_ttm"] == pytest.approx(0.5)
    assert row["dividend_yield_ttm"] == pytest.approx(0.035)
    assert row["log_total_market_cap"] == pytest.approx(math.log(100.0))
    assert row["log_circulating_market_cap"] == pytest.approx(math.log(40.0))
    assert row["turnover_rate"] == pytest.approx(2.5)
    assert row["free_float_turnover_rate"] == pytest.approx(5.0)
    assert row["volume_ratio"] == pytest.approx(1.2)


def test_generate_factor_frame_equals_shared_daily_basic_primitive() -> None:
    panel = _daily_basic_panel()
    universe = build_historical_universe(
        ["2024-01-02", "2024-01-03"], asset_ids=["A", "B"], source="explicit"
    )
    generated = generate_factor_frame(
        _factor_requests(),
        universe=universe,
        daily_basic_panel=panel,
    )

    for code in REGISTERED_DAILY_BASIC_FACTORS:
        direct = compute_daily_basic_factor(
            panel,
            universe=universe,
            factor_code=code,
        )
        pd.testing.assert_frame_equal(
            generated[["trade_date", "asset_id", code]], direct
        )
    for field, output in (
        ("total_mv", "log_total_market_cap"),
        ("circ_mv", "log_circulating_market_cap"),
    ):
        direct = compute_log_market_cap_factor(
            panel,
            universe=universe,
            field=field,
            output_column=output,
        )
        pd.testing.assert_frame_equal(
            generated[["trade_date", "asset_id", output]], direct
        )


def test_multi_asset_isolation_unordered_input_stable_sort_and_immutability() -> None:
    panel = _daily_basic_panel().sample(frac=1.0, random_state=42).reset_index(drop=True)
    before = panel.copy(deep=True)
    combined = generate_factor_frame(
        _factor_requests(),
        trade_dates=["2024-01-03", "2024-01-02"],
        asset_ids=["B", "A"],
        daily_basic_panel=panel,
    )
    a_only = generate_factor_frame(
        _factor_requests(),
        trade_dates=["2024-01-03", "2024-01-02"],
        asset_ids=["A"],
        daily_basic_panel=panel.loc[panel["asset_id"] == "A"],
    )

    pd.testing.assert_frame_equal(panel, before)
    assert not combined.duplicated(["trade_date", "asset_id"]).any()
    keys = combined[["trade_date", "asset_id"]].to_records(index=False).tolist()
    assert keys == sorted(keys)
    pd.testing.assert_frame_equal(
        combined.loc[combined["asset_id"] == "A"].reset_index(drop=True), a_only
    )


def test_same_day_point_window_has_no_adjacent_date_substitution_or_future_leakage() -> None:
    panel = _daily_basic_panel()
    panel = panel.loc[
        ~((panel["trade_date"] == "2024-01-03") & (panel["asset_id"] == "A"))
    ].copy()
    panel = pd.concat(
        [
            panel,
            pd.DataFrame(
                [
                    {
                        "trade_date": "2024-01-04",
                        "asset_id": "A",
                        "pe_ttm": 0.001,
                        "ps_ttm": 0.001,
                        "dv_ttm": 99.0,
                        "total_mv": 1e30,
                        "circ_mv": 1e30,
                        "turnover_rate": 99.0,
                        "turnover_rate_f": 99.0,
                        "volume_ratio": 99.0,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )

    missing_day = generate_factor_frame(
        _factor_requests(),
        trade_dates=["2024-01-03"],
        asset_ids=["A"],
        daily_basic_panel=panel,
    ).iloc[0]
    assert missing_day[list(FACTOR_OUTPUTS)].isna().all()

    historical = generate_factor_frame(
        _factor_requests(),
        trade_dates=["2024-01-02"],
        asset_ids=["A"],
        daily_basic_panel=panel.loc[panel["trade_date"] != "2024-01-04"],
    )
    with_future = generate_factor_frame(
        _factor_requests(),
        trade_dates=["2024-01-02"],
        asset_ids=["A"],
        daily_basic_panel=panel,
    )
    pd.testing.assert_frame_equal(historical, with_future)


def test_nan_inf_zero_and_negative_value_semantics() -> None:
    field_names = [
        "pe_ttm",
        "ps_ttm",
        "dv_ttm",
        "total_mv",
        "circ_mv",
        "turnover_rate",
        "turnover_rate_f",
        "volume_ratio",
    ]
    rows = []
    for asset, value in [
        ("ZERO", 0.0),
        ("NEG", -1.0),
        ("INF", math.inf),
        ("NEG_INF", -math.inf),
        ("NAN", math.nan),
    ]:
        row = {"trade_date": "2024-01-02", "asset_id": asset}
        row.update(dict.fromkeys(field_names, value))
        rows.append(row)

    out = generate_factor_frame(
        _factor_requests(),
        trade_dates=["2024-01-02"],
        asset_ids=["ZERO", "NEG", "INF", "NEG_INF", "NAN"],
        daily_basic_panel=pd.DataFrame(rows),
    ).set_index("asset_id")

    assert math.isnan(out.loc["ZERO", "earnings_yield_ttm"])
    assert math.isnan(out.loc["ZERO", "sales_to_price_ttm"])
    assert math.isnan(out.loc["ZERO", "log_total_market_cap"])
    assert math.isnan(out.loc["ZERO", "log_circulating_market_cap"])
    for code in (
        "dividend_yield_ttm",
        "turnover_rate",
        "free_float_turnover_rate",
        "volume_ratio",
    ):
        assert out.loc["ZERO", code] == 0.0
    assert out.loc["NEG", "earnings_yield_ttm"] == pytest.approx(-1.0)
    assert out.loc[
        "NEG", [code for code in FACTOR_OUTPUTS if code != "earnings_yield_ttm"]
    ].isna().all()
    for asset in ("INF", "NEG_INF", "NAN"):
        assert out.loc[asset, list(FACTOR_OUTPUTS)].isna().all()


def test_negative_pe_is_inverted_but_nonpositive_ps_is_not() -> None:
    panel = _daily_basic_panel()
    panel.loc[panel["asset_id"] == "A", "pe_ttm"] = -10.0
    panel.loc[panel["asset_id"] == "A", "ps_ttm"] = 0.0
    row = generate_factor_frame(
        ["earnings_yield_ttm", "sales_to_price_ttm"],
        trade_dates=["2024-01-02"],
        asset_ids=["A"],
        daily_basic_panel=panel,
    ).iloc[0]
    assert row["earnings_yield_ttm"] == pytest.approx(-0.1)
    assert math.isnan(row["sales_to_price_ttm"])


def test_parameter_and_alias_conflicts_are_rejected() -> None:
    panel = _daily_basic_panel()
    with pytest.raises(FactorRequestError, match="unknown parameters"):
        generate_factor_frame(
            [FactorRequest("earnings_yield_ttm", {"window": 20})],
            trade_dates=["2024-01-02"],
            asset_ids=["A"],
            daily_basic_panel=panel,
        )
    with pytest.raises(FactorRequestError, match="duplicate factor output"):
        generate_factor_frame(
            [
                FactorRequest("turnover_rate", alias="same"),
                FactorRequest("volume_ratio", alias="same"),
            ],
            trade_dates=["2024-01-02"],
            asset_ids=["A"],
            daily_basic_panel=panel,
        )


def test_empty_universe_missing_panel_fields_and_duplicate_keys() -> None:
    empty = generate_factor_frame(_factor_requests(), trade_dates=[], asset_ids=[])
    assert empty.empty
    assert list(empty.columns) == ["trade_date", "asset_id", *FACTOR_OUTPUTS]

    with pytest.raises(FactorRequestError, match="prepared daily_basic_panel"):
        generate_factor_frame(
            ["earnings_yield_ttm"],
            trade_dates=["2024-01-02"],
            asset_ids=["A"],
        )
    with pytest.raises(FactorRequestError, match="missing required columns"):
        generate_factor_frame(
            ["earnings_yield_ttm"],
            trade_dates=["2024-01-02"],
            asset_ids=["A"],
            daily_basic_panel=_daily_basic_panel().drop(columns="pe_ttm"),
        )

    duplicated = pd.concat(
        [_daily_basic_panel(), _daily_basic_panel().iloc[[0]]], ignore_index=True
    )
    with pytest.raises(FactorRequestError, match="duplicate"):
        generate_factor_frame(
            ["volume_ratio"],
            trade_dates=["2024-01-03"],
            asset_ids=["B"],
            daily_basic_panel=duplicated,
        )


def test_new_and_existing_factor_registration_is_complete() -> None:
    baseline = {
        "momentum",
        "intermediate_momentum",
        "short_term_reversal",
        "distance_to_high",
        "high_low_range_volatility",
        "average_turnover",
        "turnover_change",
        "average_traded_amount",
        "log_market_cap",
        "roe",
        "book_to_price",
    }
    registered = baseline | {
        "amihud_illiquidity",
        "downside_volatility",
        "price_efficiency",
        "price_volume_correlation",
        "realized_volatility",
        "relative_volume",
        "rolling_max_drawdown",
        "trend_r_squared",
        "trend_slope",
    } | set(REGISTERED_DAILY_BASIC_FACTORS)
    assert set(FACTOR_DEFINITIONS) == registered
    assert {definition.code for definition in list_factors()} == registered
    assert {"log_total_market_cap", "log_circulating_market_cap"}.isdisjoint(
        registered
    )
    for code in REGISTERED_DAILY_BASIC_FACTORS:
        definition = get_factor_definition(code)
        assert definition.inputs
        assert definition.formula
        assert definition.direction
        assert "T+1" in definition.time_semantics
        assert definition.parameter_schema == {}
        assert "NaN" in definition.nan_semantics
    for alias in ("log_total_market_cap", "log_circulating_market_cap"):
        with pytest.raises(UnknownFactorError):
            get_factor_definition(alias)


def test_public_shared_primitive_rejects_unknown_daily_basic_code() -> None:
    universe = build_historical_universe(
        ["2024-01-02"], asset_ids=["A"], source="explicit"
    )
    with pytest.raises(UnknownFactorError, match="unknown daily-basic factor code"):
        compute_daily_basic_factor(
            _daily_basic_panel(),
            universe=universe,
            factor_code="not_registered",
        )
