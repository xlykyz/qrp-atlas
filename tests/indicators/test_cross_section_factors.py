"""Tests for formal cross-sectional factor generation (task 04-B)."""

from __future__ import annotations

import math
from pathlib import Path
from datetime import date, datetime

import pandas as pd
import pytest

from qrp_atlas.backtest import prepare_financial_factor_panel
from qrp_atlas.indicators import (
    FactorRequest,
    FactorRequestError,
    UnknownFactorError,
    build_historical_universe,
    generate_factor_frame,
    get_factor_definition,
    list_factors,
    normalize_trade_date,
    process_cross_section,
    resolve_factor_requests,
)


def _day(value: str) -> pd.Timestamp:
    return normalize_trade_date(value)


def _prices() -> pd.DataFrame:
    """Multi-asset prices with deliberate non-overlap to catch cross-asset bleed."""
    rows: list[dict] = []
    for i in range(10):
        d = pd.Timestamp("2024-01-02") + pd.Timedelta(days=i)
        rows.append(
            {
                "trade_date": d,
                "asset_id": "A",
                "close": float(i + 1),
                "market_cap": 100.0 * (i + 1),
                "float_cap": 80.0 * (i + 1),
            }
        )
    for i in range(10):
        d = pd.Timestamp("2024-01-05") + pd.Timedelta(days=i)
        rows.append(
            {
                "trade_date": d,
                "asset_id": "B",
                "close": float(100 + i),
                "market_cap": 1000.0 + 10.0 * i,
                "float_cap": 500.0 + 5.0 * i,
            }
        )
    for i, close in enumerate([10.0, 12.0, math.nan, 15.0]):
        d = pd.Timestamp("2024-01-08") + pd.Timedelta(days=i)
        rows.append(
            {
                "trade_date": d,
                "asset_id": "C",
                "close": close,
                "market_cap": math.nan if i == 1 else 50.0 + i,
                "float_cap": 40.0,
            }
        )
    return pd.DataFrame(rows)


def _versioned_financials() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ticker": "A",
                "report_period": date(2023, 12, 31),
                "available_trade_date": date(2024, 1, 8),
                "announcement_date": date(2024, 1, 6),
                "published_at": datetime(2024, 1, 6, 18, 0, 0),
                "ingested_at": datetime(2024, 1, 7, 1, 0, 0),
                "revision_id": "a_old",
                "roe": 0.10,
                "bps": 5.0,
                "update_flag": "0",
            },
            {
                "ticker": "A",
                "report_period": date(2023, 12, 31),
                "available_trade_date": date(2024, 1, 11),
                "announcement_date": date(2024, 1, 10),
                "published_at": datetime(2024, 1, 10, 18, 0, 0),
                "ingested_at": datetime(2024, 1, 11, 1, 0, 0),
                "revision_id": "a_new",
                "roe": 0.15,
                "bps": 6.0,
                "update_flag": "1",
            },
            {
                "ticker": "B",
                "report_period": date(2023, 9, 30),
                "available_trade_date": date(2024, 1, 5),
                "announcement_date": date(2024, 1, 4),
                "published_at": datetime(2024, 1, 4, 18, 0, 0),
                "ingested_at": datetime(2024, 1, 5, 1, 0, 0),
                "revision_id": "b1",
                "roe": 0.20,
                "bps": 10.0,
                "update_flag": "0",
            },
            {
                "ticker": "B",
                "report_period": date(2023, 12, 31),
                "available_trade_date": date(2024, 1, 10),
                "announcement_date": date(2024, 1, 9),
                "published_at": datetime(2024, 1, 9, 18, 0, 0),
                "ingested_at": datetime(2024, 1, 10, 1, 0, 0),
                "revision_id": "b2",
                "roe": 0.25,
                "bps": 12.0,
                "update_flag": "0",
            },
            {
                "ticker": "C",
                "report_period": date(2023, 12, 31),
                "available_trade_date": date(2024, 1, 9),
                "announcement_date": date(2024, 1, 8),
                "published_at": datetime(2024, 1, 8, 18, 0, 0),
                "ingested_at": datetime(2024, 1, 9, 1, 0, 0),
                "revision_id": "c1",
                "roe": math.nan,
                "bps": 0.0,
                "update_flag": "0",
            },
        ]
    )


def _financial_panel(
    trade_dates: list[str],
    asset_ids: list[str],
    financials: pd.DataFrame | None = None,
) -> pd.DataFrame:
    universe = build_historical_universe(trade_dates, asset_ids=asset_ids, source="explicit")
    return prepare_financial_factor_panel(
        universe,
        financials=financials if financials is not None else _versioned_financials(),
    )


def test_list_and_get_factor_definitions() -> None:
    codes = [item.code for item in list_factors()]
    expected = {
        "average_traded_amount",
        "average_turnover",
        "book_to_price",
        "distance_to_high",
        "dividend_yield_ttm",
        "earnings_yield_ttm",
        "free_float_turnover_rate",
        "high_low_range_volatility",
        "intermediate_momentum",
        "log_circulating_market_cap",
        "log_market_cap",
        "log_total_market_cap",
        "momentum",
        "roe",
        "sales_to_price_ttm",
        "short_term_reversal",
        "turnover_change",
        "turnover_rate",
        "volume_ratio",
    }
    assert codes == sorted(codes)
    assert expected.issubset(codes)
    momentum = get_factor_definition("momentum")
    assert "close[T]" in momentum.formula
    assert "T+1" in momentum.time_semantics
    with pytest.raises(UnknownFactorError):
        get_factor_definition("not_a_factor")


def test_momentum_window_and_no_cross_asset_bleed() -> None:
    prices = _prices()
    original = prices.copy(deep=True)
    out = generate_factor_frame(
        [FactorRequest("momentum", {"lookback": 2})],
        trade_dates=["2024-01-04", "2024-01-06", "2024-01-11"],
        asset_ids=["A", "B"],
        prices=prices,
    )
    assert list(out.columns) == ["trade_date", "asset_id", "momentum_lookback_2"]
    by = out.set_index(["trade_date", "asset_id"])["momentum_lookback_2"]
    assert by.loc[(_day("2024-01-04"), "A")] == pytest.approx(2.0)
    assert by.loc[(_day("2024-01-06"), "A")] == pytest.approx(5.0 / 3.0 - 1.0)
    assert math.isnan(by.loc[(_day("2024-01-06"), "B")])
    assert by.loc[(_day("2024-01-11"), "B")] == pytest.approx(106.0 / 104.0 - 1.0)
    assert math.isnan(by.loc[(_day("2024-01-04"), "B")])
    pd.testing.assert_frame_equal(prices, original)


def test_momentum_does_not_read_future_prices() -> None:
    prices = _prices()
    future = prices.copy()
    future.loc[
        (future["asset_id"] == "A") & (future["trade_date"] == pd.Timestamp("2024-01-10")),
        "close",
    ] = 9999.0
    out_base = generate_factor_frame(
        [FactorRequest("momentum", {"lookback": 3})],
        trade_dates=["2024-01-05"],
        asset_ids=["A"],
        prices=prices,
    )
    out_future = generate_factor_frame(
        [FactorRequest("momentum", {"lookback": 3})],
        trade_dates=["2024-01-05"],
        asset_ids=["A"],
        prices=future,
    )
    v1 = out_base.iloc[0]["momentum_lookback_3"]
    v2 = out_future.iloc[0]["momentum_lookback_3"]
    assert v1 == pytest.approx(v2)
    assert v1 == pytest.approx(3.0)
    truncated = prices[prices["trade_date"] <= "2024-01-05"]
    out_trunc = generate_factor_frame(
        [FactorRequest("momentum", {"lookback": 3})],
        trade_dates=["2024-01-05"],
        asset_ids=["A"],
        prices=truncated,
    )
    assert out_trunc.iloc[0]["momentum_lookback_3"] == pytest.approx(3.0)


def test_momentum_middle_nan_keeps_bar_slot_and_endpoint_rules() -> None:
    # Ordered bars: 10, 12, NaN, 20 with lookback=2
    prices = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A", "close": 10.0},
            {"trade_date": "2024-01-03", "asset_id": "A", "close": 12.0},
            {"trade_date": "2024-01-04", "asset_id": "A", "close": math.nan},
            {"trade_date": "2024-01-05", "asset_id": "A", "close": 20.0},
            {"trade_date": "2024-01-06", "asset_id": "A", "close": -1.0},
        ]
    )
    out = generate_factor_frame(
        [FactorRequest("momentum", {"lookback": 2}, alias="mom2")],
        trade_dates=["2024-01-04", "2024-01-05", "2024-01-06"],
        asset_ids=["A"],
        prices=prices,
    )
    by = out.set_index("trade_date")["mom2"]
    # T=01-04 uses close[T]=NaN / close[T-2]=10 -> NaN (current endpoint invalid)
    assert math.isnan(by.loc[_day("2024-01-04")])
    # T=01-05 uses close=20 / close[T-2]=12 -> valid, middle NaN still occupies a slot
    assert by.loc[_day("2024-01-05")] == pytest.approx(20.0 / 12.0 - 1.0)
    # T=01-06 uses close=-1 treated as invalid endpoint
    assert math.isnan(by.loc[_day("2024-01-06")])


def test_log_market_cap_invalid_values() -> None:
    panel = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A", "market_cap": 100.0},
            {"trade_date": "2024-01-02", "asset_id": "B", "market_cap": 0.0},
            {"trade_date": "2024-01-02", "asset_id": "C", "market_cap": -5.0},
            {"trade_date": "2024-01-02", "asset_id": "D", "market_cap": math.nan},
            {"trade_date": "2024-01-02", "asset_id": "E", "market_cap": math.inf},
            {"trade_date": "2024-01-03", "asset_id": "A", "market_cap": 200.0},
        ]
    )
    out = generate_factor_frame(
        ["log_market_cap"],
        trade_dates=["2024-01-02", "2024-01-03"],
        asset_ids=["A", "B", "C", "D", "E"],
        prices=panel,
    )
    by = out.set_index(["trade_date", "asset_id"])["log_market_cap"]
    assert by.loc[(_day("2024-01-02"), "A")] == pytest.approx(math.log(100.0))
    assert math.isnan(by.loc[(_day("2024-01-02"), "B")])
    assert math.isnan(by.loc[(_day("2024-01-02"), "C")])
    assert math.isnan(by.loc[(_day("2024-01-02"), "D")])
    assert math.isnan(by.loc[(_day("2024-01-02"), "E")])
    assert by.loc[(_day("2024-01-03"), "A")] == pytest.approx(math.log(200.0))
    assert math.isnan(by.loc[(_day("2024-01-03"), "B")])


def test_roe_and_book_to_price_share_one_prepared_panel() -> None:
    dates = ["2024-01-07", "2024-01-08", "2024-01-11"]
    assets = ["A", "B", "C"]
    panel = _financial_panel(dates, assets)
    assert list(panel.columns) == ["trade_date", "asset_id", "roe", "bps"]
    # same panel used for both factors; no per-factor DB query path
    out = generate_factor_frame(
        ["roe", "book_to_price"],
        trade_dates=dates,
        asset_ids=assets,
        prices=_prices(),
        financial_panel=panel,
    )
    by = out.set_index(["trade_date", "asset_id"])
    assert math.isnan(by.loc[(_day("2024-01-07"), "A"), "roe"])
    assert by.loc[(_day("2024-01-08"), "A"), "roe"] == pytest.approx(0.10)
    assert by.loc[(_day("2024-01-11"), "A"), "roe"] == pytest.approx(0.15)
    assert by.loc[(_day("2024-01-08"), "B"), "roe"] == pytest.approx(0.20)
    assert by.loc[(_day("2024-01-11"), "B"), "roe"] == pytest.approx(0.25)
    assert math.isnan(by.loc[(_day("2024-01-11"), "C"), "roe"])
    assert math.isnan(by.loc[(_day("2024-01-07"), "A"), "book_to_price"])
    assert by.loc[(_day("2024-01-08"), "A"), "book_to_price"] == pytest.approx(5.0 / 7.0)
    assert by.loc[(_day("2024-01-11"), "A"), "book_to_price"] == pytest.approx(6.0 / 10.0)
    assert math.isnan(by.loc[(_day("2024-01-11"), "C"), "book_to_price"])


def test_prepare_financial_panel_revision_isolation_and_single_query_per_date() -> None:
    calls: list[tuple[object, tuple[str, ...]]] = []

    def fake_query(*, as_of_date, table, tickers=None, **kwargs):
        calls.append((as_of_date, tuple(tickers or ())))
        return _versioned_financials()

    universe = build_historical_universe(
        ["2024-01-08", "2024-01-11"],
        asset_ids=["A"],
        source="explicit",
    )
    panel = prepare_financial_factor_panel(universe, financial_query=fake_query)
    by = panel.set_index("trade_date")
    assert by.loc[_day("2024-01-08"), "roe"] == pytest.approx(0.10)
    assert by.loc[_day("2024-01-11"), "roe"] == pytest.approx(0.15)
    assert by.loc[_day("2024-01-08"), "bps"] == pytest.approx(5.0)
    assert by.loc[_day("2024-01-11"), "bps"] == pytest.approx(6.0)
    # one query per target trade_date, shared by both fields
    assert len(calls) == 2


def test_partial_missing_keeps_assets_with_nan() -> None:
    dates = ["2024-01-09"]
    assets = ["A", "B", "C", "MISSING"]
    out = generate_factor_frame(
        ["momentum", "roe", "log_market_cap"],
        trade_dates=dates,
        asset_ids=assets,
        prices=_prices(),
        financial_panel=_financial_panel(dates, assets),
    )
    assert set(out["asset_id"]) == {"A", "B", "C", "MISSING"}
    missing = out[out["asset_id"] == "MISSING"].iloc[0]
    assert math.isnan(missing["momentum"])
    assert math.isnan(missing["roe"])
    assert math.isnan(missing["log_market_cap"])
    c_row = out[out["asset_id"] == "C"].iloc[0]
    assert math.isnan(c_row["momentum"])
    assert math.isnan(c_row["roe"])


def test_multi_factor_merge_unique_primary_key_and_column_order() -> None:
    dates = ["2024-01-08", "2024-01-11"]
    assets = ["A", "B"]
    out = generate_factor_frame(
        [
            FactorRequest("momentum", {"lookback": 2}, alias="mom2"),
            "log_market_cap",
            FactorRequest("roe"),
            "book_to_price",
        ],
        trade_dates=dates,
        asset_ids=assets,
        prices=_prices(),
        financial_panel=_financial_panel(dates, assets),
    )
    assert list(out.columns) == [
        "trade_date",
        "asset_id",
        "mom2",
        "log_market_cap",
        "roe",
        "book_to_price",
    ]
    assert not out.duplicated(subset=["trade_date", "asset_id"]).any()
    assert out["trade_date"].is_monotonic_increasing
    assert out.loc[0, "asset_id"] == "A"
    assert out.loc[1, "asset_id"] == "B"


def test_empty_universe_and_empty_dates() -> None:
    empty_assets = generate_factor_frame(
        ["momentum", "roe"],
        trade_dates=["2024-01-08"],
        asset_ids=[],
        prices=_prices(),
        financial_panel=_financial_panel(["2024-01-08"], []),
    )
    assert empty_assets.empty
    assert list(empty_assets.columns) == ["trade_date", "asset_id", "momentum", "roe"]

    empty_dates = generate_factor_frame(
        ["momentum"],
        trade_dates=[],
        asset_ids=["A"],
        prices=_prices(),
    )
    assert empty_dates.empty
    assert list(empty_dates.columns) == ["trade_date", "asset_id", "momentum"]


def test_unknown_factor_illegal_parameters_and_reserved_alias() -> None:
    with pytest.raises(UnknownFactorError):
        generate_factor_frame(
            ["unknown"], trade_dates=["2024-01-02"], asset_ids=["A"], prices=_prices()
        )
    with pytest.raises(FactorRequestError):
        generate_factor_frame(
            [FactorRequest("momentum", {"lookback": 0})],
            trade_dates=["2024-01-02"],
            asset_ids=["A"],
            prices=_prices(),
        )
    with pytest.raises(FactorRequestError):
        generate_factor_frame(
            [FactorRequest("log_market_cap", {"field": "not_a_field"})],
            trade_dates=["2024-01-02"],
            asset_ids=["A"],
            prices=_prices(),
        )
    with pytest.raises(FactorRequestError):
        resolve_factor_requests([])
    with pytest.raises(FactorRequestError, match="reserved"):
        resolve_factor_requests([FactorRequest("momentum", alias="trade_date")])
    with pytest.raises(FactorRequestError, match="reserved"):
        resolve_factor_requests([FactorRequest("roe", alias="asset_id")])


def test_duplicate_price_and_size_keys_raise() -> None:
    prices = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A", "close": 1.0, "market_cap": 10.0},
            {"trade_date": "2024-01-02", "asset_id": "A", "close": 2.0, "market_cap": 20.0},
        ]
    )
    with pytest.raises(FactorRequestError, match="duplicate"):
        generate_factor_frame(
            ["momentum"],
            trade_dates=["2024-01-02"],
            asset_ids=["A"],
            prices=prices,
        )
    with pytest.raises(FactorRequestError, match="duplicate"):
        generate_factor_frame(
            ["log_market_cap"],
            trade_dates=["2024-01-02"],
            asset_ids=["A"],
            size_panel=prices,
        )


def test_financial_factor_requires_prepared_panel() -> None:
    with pytest.raises(FactorRequestError, match="financial_panel"):
        generate_factor_frame(
            ["roe"],
            trade_dates=["2024-01-08"],
            asset_ids=["A"],
        )
    with pytest.raises(FactorRequestError, match="financial_panel"):
        generate_factor_frame(
            ["book_to_price"],
            trade_dates=["2024-01-08"],
            asset_ids=["A"],
            prices=_prices(),
        )


def test_input_immutability_and_output_stability() -> None:
    prices = _prices()
    financial_panel = _financial_panel(["2024-01-08", "2024-01-11"], ["B", "A"])
    prices_before = prices.copy(deep=True)
    panel_before = financial_panel.copy(deep=True)
    out1 = generate_factor_frame(
        ["momentum", "roe", "book_to_price", "log_market_cap"],
        trade_dates=["2024-01-08", "2024-01-11"],
        asset_ids=["B", "A"],
        prices=prices,
        financial_panel=financial_panel,
    )
    out2 = generate_factor_frame(
        ["momentum", "roe", "book_to_price", "log_market_cap"],
        trade_dates=["2024-01-08", "2024-01-11"],
        asset_ids=["B", "A"],
        prices=prices,
        financial_panel=financial_panel,
    )
    pd.testing.assert_frame_equal(prices, prices_before)
    pd.testing.assert_frame_equal(financial_panel, panel_before)
    pd.testing.assert_frame_equal(out1, out2)
    assert list(out1["asset_id"].unique()) == ["A", "B"]


def test_factor_frame_feeds_process_cross_section() -> None:
    factors = generate_factor_frame(
        [FactorRequest("momentum", {"lookback": 2}, alias="momentum")],
        trade_dates=["2024-01-08", "2024-01-11"],
        asset_ids=["A", "B"],
        prices=_prices(),
    )
    ranked = process_cross_section(
        factors,
        feature_columns=["momentum"],
        operators=("rank", "zscore"),
    )
    assert "momentum_rank" in ranked.columns
    assert "momentum_zscore" in ranked.columns
    day1 = ranked[ranked["trade_date"] == _day("2024-01-08")].set_index("asset_id")
    assert set(day1.index) == {"A", "B"}
    assert day1["momentum_rank"].notna().all()


def test_multi_date_isolation_for_size_and_momentum() -> None:
    out = generate_factor_frame(
        [FactorRequest("momentum", {"lookback": 1}, alias="mom1"), "log_market_cap"],
        trade_dates=["2024-01-03", "2024-01-04"],
        asset_ids=["A"],
        prices=_prices(),
    )
    by = out.set_index("trade_date")
    assert by.loc[_day("2024-01-03"), "mom1"] == pytest.approx(2.0 / 1.0 - 1.0)
    assert by.loc[_day("2024-01-04"), "mom1"] == pytest.approx(3.0 / 2.0 - 1.0)
    assert by.loc[_day("2024-01-03"), "log_market_cap"] == pytest.approx(math.log(200.0))
    assert by.loc[_day("2024-01-04"), "log_market_cap"] == pytest.approx(math.log(300.0))


def test_indicators_module_has_no_backtest_import() -> None:
    import qrp_atlas.indicators.cross_section.factors as factors_mod

    source = Path(factors_mod.__file__).read_text(encoding="utf-8")
    assert "from qrp_atlas.backtest" not in source
    assert "import qrp_atlas.backtest" not in source
    assert "db_path" not in source
