"""Tests for formal cross-sectional factor generation (task 04-B)."""

from __future__ import annotations

import math
from datetime import date, datetime

import pandas as pd
import pytest

from qrp_atlas.indicators import (
    FactorRequest,
    FactorRequestError,
    UnknownFactorError,
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
    # Asset A: 1..10 over 10 days starting 2024-01-02
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
    # Asset B: prices start later and use a different sequence
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
    # Asset C: sparse / incomplete history
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


def _financials() -> pd.DataFrame:
    return pd.DataFrame(
        [
            # A report available from 2024-01-08
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
            # A revision available later; must not pollute 2024-01-08/09
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
            # B available earlier
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
            # B newer report period
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
            # C missing ROE / non-positive BPS edge cases
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


def test_list_and_get_factor_definitions() -> None:
    codes = [item.code for item in list_factors()]
    assert codes == ["book_to_price", "log_market_cap", "momentum", "roe"]
    momentum = get_factor_definition("momentum")
    assert "close[T]" in momentum.formula
    assert "T+1" in momentum.time_semantics
    with pytest.raises(UnknownFactorError):
        get_factor_definition("not_a_factor")


def test_momentum_window_and_no_cross_asset_bleed() -> None:
    prices = _prices()
    # mutate a copy later to prove no bleed / immutability
    original = prices.copy(deep=True)

    out = generate_factor_frame(
        [FactorRequest("momentum", {"lookback": 2})],
        trade_dates=["2024-01-04", "2024-01-06", "2024-01-11"],
        asset_ids=["A", "B"],
        prices=prices,
    )
    assert list(out.columns) == ["trade_date", "asset_id", "momentum_lookback_2"]
    by = out.set_index(["trade_date", "asset_id"])["momentum_lookback_2"]

    # A on 2024-01-04: closes 1,2,3 -> 3/1 - 1 = 2.0
    assert by.loc[(_day("2024-01-04"), "A")] == pytest.approx(2.0)
    # A on 2024-01-06: closes ... 3,4,5 -> 5/3 - 1
    assert by.loc[(_day("2024-01-06"), "A")] == pytest.approx(5.0 / 3.0 - 1.0)
    # B first date 2024-01-05; on 2024-01-06 only 2 bars -> insufficient for lookback=2
    assert math.isnan(by.loc[(_day("2024-01-06"), "B")])
    # B on 2024-01-11: dates 01-05..01-11 = 7 bars, close starts 100
    # close[T]=106, close[T-2]=104 -> 106/104 - 1
    assert by.loc[(_day("2024-01-11"), "B")] == pytest.approx(106.0 / 104.0 - 1.0)
    # B must never use A's prices; on 2024-01-04 B not yet listed -> NaN
    assert math.isnan(by.loc[(_day("2024-01-04"), "B")])

    pd.testing.assert_frame_equal(prices, original)


def test_momentum_does_not_read_future_prices() -> None:
    prices = _prices()
    # Inject a future-only spike for A that must not affect earlier T
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
    # Both should only use data up to 2024-01-05; future spike on 01-10 irrelevant
    v1 = out_base.iloc[0]["momentum_lookback_3"]
    v2 = out_future.iloc[0]["momentum_lookback_3"]
    assert v1 == pytest.approx(v2)
    # closes on A: 01-02=1 ... 01-05=4 -> 4/1 - 1 = 3
    assert v1 == pytest.approx(3.0)

    # Dropping T+1 bar must not change T value
    truncated = prices[prices["trade_date"] <= "2024-01-05"]
    out_trunc = generate_factor_frame(
        [FactorRequest("momentum", {"lookback": 3})],
        trade_dates=["2024-01-05"],
        asset_ids=["A"],
        prices=truncated,
    )
    assert out_trunc.iloc[0]["momentum_lookback_3"] == pytest.approx(3.0)


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
    # missing same-day size for B on day 2 stays NaN (no forward fill from future)
    assert math.isnan(by.loc[(_day("2024-01-03"), "B")])


def test_roe_pit_visibility_and_revision_isolation() -> None:
    out = generate_factor_frame(
        ["roe"],
        trade_dates=["2024-01-07", "2024-01-08", "2024-01-11"],
        asset_ids=["A", "B", "C"],
        financials=_financials(),
    )
    by = out.set_index(["trade_date", "asset_id"])["roe"]
    # Before availability: invisible
    assert math.isnan(by.loc[(_day("2024-01-07"), "A")])
    # On availability day: old revision
    assert by.loc[(_day("2024-01-08"), "A")] == pytest.approx(0.10)
    # After later revision: new value
    assert by.loc[(_day("2024-01-11"), "A")] == pytest.approx(0.15)
    # B switches report period after 2024-01-10
    assert by.loc[(_day("2024-01-08"), "B")] == pytest.approx(0.20)
    assert by.loc[(_day("2024-01-11"), "B")] == pytest.approx(0.25)
    # C has NaN ROE after availability
    assert math.isnan(by.loc[(_day("2024-01-11"), "C")])


def test_book_to_price_pit_and_invalid_denominator() -> None:
    prices = _prices()
    out = generate_factor_frame(
        ["book_to_price"],
        trade_dates=["2024-01-07", "2024-01-08", "2024-01-11"],
        asset_ids=["A", "B", "C"],
        prices=prices,
        financials=_financials(),
    )
    by = out.set_index(["trade_date", "asset_id"])["book_to_price"]
    # A before availability
    assert math.isnan(by.loc[(_day("2024-01-07"), "A")])
    # A on 01-08: bps=5 / close=7
    assert by.loc[(_day("2024-01-08"), "A")] == pytest.approx(5.0 / 7.0)
    # A on 01-11 uses revised bps=6 / close=10
    assert by.loc[(_day("2024-01-11"), "A")] == pytest.approx(6.0 / 10.0)
    # C has bps=0 -> NaN even after availability
    assert math.isnan(by.loc[(_day("2024-01-11"), "C")])


def test_partial_missing_keeps_assets_with_nan() -> None:
    out = generate_factor_frame(
        ["momentum", "roe", "log_market_cap"],
        trade_dates=["2024-01-09"],
        asset_ids=["A", "B", "C", "MISSING"],
        prices=_prices(),
        financials=_financials(),
    )
    assert set(out["asset_id"]) == {"A", "B", "C", "MISSING"}
    missing = out[out["asset_id"] == "MISSING"].iloc[0]
    assert math.isnan(missing["momentum"])
    assert math.isnan(missing["roe"])
    assert math.isnan(missing["log_market_cap"])
    # C has only 2 bars by 01-09 for lookback default 20 -> NaN momentum, but row retained
    c_row = out[out["asset_id"] == "C"].iloc[0]
    assert math.isnan(c_row["momentum"])
    assert math.isnan(c_row["roe"])  # roe NaN in fixture


def test_multi_factor_merge_unique_primary_key_and_column_order() -> None:
    out = generate_factor_frame(
        [
            FactorRequest("momentum", {"lookback": 2}, alias="mom2"),
            "log_market_cap",
            FactorRequest("roe"),
            "book_to_price",
        ],
        trade_dates=["2024-01-08", "2024-01-11"],
        asset_ids=["A", "B"],
        prices=_prices(),
        financials=_financials(),
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
    # stable sort: date then asset
    assert out.loc[0, "asset_id"] == "A"
    assert out.loc[1, "asset_id"] == "B"


def test_empty_universe_and_empty_dates() -> None:
    empty_assets = generate_factor_frame(
        ["momentum", "roe"],
        trade_dates=["2024-01-08"],
        asset_ids=[],
        prices=_prices(),
        financials=_financials(),
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


def test_unknown_factor_and_illegal_parameters() -> None:
    with pytest.raises(UnknownFactorError):
        generate_factor_frame(["unknown"], trade_dates=["2024-01-02"], asset_ids=["A"], prices=_prices())
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


def test_input_immutability_and_output_stability() -> None:
    prices = _prices()
    financials = _financials()
    prices_before = prices.copy(deep=True)
    financials_before = financials.copy(deep=True)

    out1 = generate_factor_frame(
        ["momentum", "roe", "book_to_price", "log_market_cap"],
        trade_dates=["2024-01-08", "2024-01-11"],
        asset_ids=["B", "A"],
        prices=prices,
        financials=financials,
    )
    out2 = generate_factor_frame(
        ["momentum", "roe", "book_to_price", "log_market_cap"],
        trade_dates=["2024-01-08", "2024-01-11"],
        asset_ids=["B", "A"],
        prices=prices,
        financials=financials,
    )
    pd.testing.assert_frame_equal(prices, prices_before)
    pd.testing.assert_frame_equal(financials, financials_before)
    pd.testing.assert_frame_equal(out1, out2)
    # universe order should not change stable sort
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
    # ranks computed independently per day
    day1 = ranked[ranked["trade_date"] == _day("2024-01-08")].set_index("asset_id")
    assert set(day1.index) == {"A", "B"}
    assert day1["momentum_rank"].notna().all()


def test_financial_query_injection_and_as_of_filter() -> None:
    calls: list[tuple[object, tuple[str, ...]]] = []

    def fake_query(*, as_of_date, table, tickers=None, **kwargs):
        calls.append((as_of_date, tuple(tickers or ())))
        # Return both old and future-only revision; helper must filter by as_of.
        return _financials()

    out = generate_factor_frame(
        ["roe"],
        trade_dates=["2024-01-08", "2024-01-11"],
        asset_ids=["A"],
        financial_query=fake_query,
    )
    by = out.set_index("trade_date")["roe"]
    assert by.loc[_day("2024-01-08")] == pytest.approx(0.10)
    assert by.loc[_day("2024-01-11")] == pytest.approx(0.15)
    assert len(calls) == 2


def test_multi_date_isolation_for_size_and_momentum() -> None:
    out = generate_factor_frame(
        [FactorRequest("momentum", {"lookback": 1}, alias="mom1"), "log_market_cap"],
        trade_dates=["2024-01-03", "2024-01-04"],
        asset_ids=["A"],
        prices=_prices(),
    )
    by = out.set_index("trade_date")
    # day isolation: values differ by date and do not mix
    assert by.loc[_day("2024-01-03"), "mom1"] == pytest.approx(2.0 / 1.0 - 1.0)
    assert by.loc[_day("2024-01-04"), "mom1"] == pytest.approx(3.0 / 2.0 - 1.0)
    assert by.loc[_day("2024-01-03"), "log_market_cap"] == pytest.approx(math.log(200.0))
    assert by.loc[_day("2024-01-04"), "log_market_cap"] == pytest.approx(math.log(300.0))
