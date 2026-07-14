"""Industry residual PIT preparation and research tests (task 06-B)."""

from __future__ import annotations

import math
from datetime import datetime
from zoneinfo import ZoneInfo

import duckdb
import numpy as np
import pandas as pd
import pytest

from qrp_atlas.backtest import (
    prepare_industry_residual_panel,
    run_industry_residual_research,
)
from qrp_atlas.backtest.pit_queries import query_industry_as_of
from qrp_atlas.backtest.residual_data import ResidualDataError
from qrp_atlas.indicators.cross_section.conventions import normalize_trade_date


def _prices(asset_map: dict[str, list[float]], *, start: str = "2024-01-01") -> pd.DataFrame:
    rows = []
    n = len(next(iter(asset_map.values())))
    dates = pd.bdate_range(start, periods=n)
    for asset_id, closes in asset_map.items():
        for date, close in zip(dates, closes, strict=True):
            rows.append(
                {
                    "trade_date": date,
                    "asset_id": asset_id,
                    "ticker": asset_id,
                    "open": float(close),
                    "high": float(close) * 1.01 if math.isfinite(float(close)) else float(close),
                    "low": float(close) * 0.99 if math.isfinite(float(close)) else float(close),
                    "close": float(close),
                }
            )
    return pd.DataFrame(rows)


def _industry_prices(
    code_map: dict[str, list[float]], *, start: str = "2024-01-01"
) -> pd.DataFrame:
    rows = []
    n = len(next(iter(code_map.values())))
    dates = pd.bdate_range(start, periods=n)
    for code, closes in code_map.items():
        for date, close in zip(dates, closes, strict=True):
            rows.append(
                {
                    "trade_date": date,
                    "industry_code": code,
                    "close": float(close),
                }
            )
    return pd.DataFrame(rows)


def _flat_panel_industry(
    assets: list[str],
    dates: pd.DatetimeIndex,
    code_by_asset_date,
) -> pd.DataFrame:
    rows = []
    for date in dates:
        for asset in assets:
            rows.append(
                {
                    "trade_date": date,
                    "asset_id": asset,
                    "industry_code": code_by_asset_date(asset, date),
                }
            )
    return pd.DataFrame(rows)


def test_historical_industry_switch_uses_old_then_new_benchmark() -> None:
    n = 20
    dates = pd.bdate_range("2024-01-01", periods=n)
    assets = _prices({"A": [100 + i for i in range(n)]})
    # Distinct industry return paths.
    i1 = [1000 * (1.01**i) for i in range(n)]
    i2 = [1000 * (0.99**i) for i in range(n)]
    bench = _industry_prices({"I1": i1, "I2": i2})
    switch = dates[10]

    def code(asset, date):
        return "I1" if date < switch else "I2"

    industry = _flat_panel_industry(["A"], dates, code)
    prep = prepare_industry_residual_panel(
        assets,
        industry_benchmark_prices=bench,
        industry_panel=industry,
        window=5,
        min_periods=5,
        z_window=5,
        compute_residuals=False,
    )
    panel = prep.panel
    before = panel[panel["trade_date"] == dates[9]].iloc[0]
    on_switch = panel[panel["trade_date"] == switch].iloc[0]
    assert before["industry_code"] == "I1"
    assert before["benchmark_id"] == "I1"
    assert on_switch["industry_code"] == "I2"
    assert on_switch["benchmark_id"] == "I2"
    # Exact benchmark return alignment for old/new industry.
    assert math.isclose(
        float(before["benchmark_return"]),
        i1[9] / i1[8] - 1.0,
        rel_tol=1e-12,
    )
    assert math.isclose(
        float(on_switch["benchmark_return"]),
        i2[10] / i2[9] - 1.0,
        rel_tol=1e-12,
    )


def test_future_industry_membership_not_visible() -> None:
    n = 12
    dates = pd.bdate_range("2024-01-01", periods=n)
    assets = _prices({"A": [100 + i for i in range(n)]})
    bench = _industry_prices(
        {"OLD": [1000 + i for i in range(n)], "NEW": [2000 + i for i in range(n)]}
    )
    industry = pd.DataFrame(
        [
            {
                "trade_date": date,
                "asset_id": "A",
                "industry_code": "OLD" if date < dates[8] else "NEW",
            }
            for date in dates
        ]
    )
    prep = prepare_industry_residual_panel(
        assets,
        industry_benchmark_prices=bench,
        industry_panel=industry,
        window=3,
        min_periods=3,
        z_window=3,
        compute_residuals=False,
    )
    early = prep.panel[prep.panel["trade_date"] == dates[5]].iloc[0]
    assert early["industry_code"] == "OLD"
    assert early["benchmark_id"] == "OLD"


def test_multi_industry_no_cross_contamination() -> None:
    n = 15
    assets = _prices({"A": [100 + i for i in range(n)], "B": [100 + 2 * i for i in range(n)]})
    bench = _industry_prices(
        {
            "I1": [1000 * (1.01**i) for i in range(n)],
            "I2": [1000 * (1.02**i) for i in range(n)],
        }
    )
    dates = pd.bdate_range("2024-01-01", periods=n)
    industry = _flat_panel_industry(
        ["A", "B"], dates, lambda asset, date: "I1" if asset == "A" else "I2"
    )
    prep = prepare_industry_residual_panel(
        assets,
        industry_benchmark_prices=bench,
        industry_panel=industry,
        window=4,
        min_periods=4,
        z_window=4,
    )
    a = prep.panel[prep.panel["asset_id"] == "A"]
    b = prep.panel[prep.panel["asset_id"] == "B"]
    assert set(a["benchmark_id"].dropna()) == {"I1"}
    assert set(b["benchmark_id"].dropna()) == {"I2"}
    # Same date different benchmarks.
    day = dates[5]
    ar = float(a[a["trade_date"] == day]["benchmark_return"].iloc[0])
    br = float(b[b["trade_date"] == day]["benchmark_return"].iloc[0])
    assert not math.isclose(ar, br)


def test_missing_benchmark_no_fill_and_no_market_fallback() -> None:
    n = 10
    assets = _prices({"A": [100 + i for i in range(n)]})
    dates = pd.bdate_range("2024-01-01", periods=n)
    # Drop one industry date.
    closes = [1000 + i for i in range(n)]
    bench = _industry_prices({"I1": closes}).iloc[[0, 1, 2, 3, 4, 6, 7, 8, 9]].copy()
    industry = _flat_panel_industry(["A"], dates, lambda a, d: "I1")
    prep = prepare_industry_residual_panel(
        assets,
        industry_benchmark_prices=bench,
        industry_panel=industry,
        window=3,
        min_periods=3,
        z_window=3,
        compute_residuals=False,
    )
    row = prep.panel[prep.panel["trade_date"] == dates[5]].iloc[0]
    assert math.isnan(float(row["benchmark_return"]))
    assert row["preparation_diagnostic_code"] == "MISSING_INDUSTRY_BENCHMARK"
    assert any("MISSING_INDUSTRY_BENCHMARK" in item for item in prep.diagnostics)


def test_missing_industry_no_market_fallback() -> None:
    n = 8
    assets = _prices({"A": [100 + i for i in range(n)], "B": [100 + i for i in range(n)]})
    dates = pd.bdate_range("2024-01-01", periods=n)
    bench = _industry_prices({"I1": [1000 + i for i in range(n)]})
    industry = pd.DataFrame(
        [{"trade_date": date, "asset_id": "A", "industry_code": "I1"} for date in dates]
        # B never has industry
    )
    prep = prepare_industry_residual_panel(
        assets,
        industry_benchmark_prices=bench,
        industry_panel=industry,
        window=3,
        min_periods=3,
        z_window=3,
        compute_residuals=False,
    )
    b = prep.panel[prep.panel["asset_id"] == "B"]
    assert b["industry_code"].isna().all() or (b["industry_code"] == None).all()  # noqa: E711
    assert b["benchmark_return"].isna().all()
    assert prep.metadata["missing_industry_count"] == n
    assert any("MISSING_INDUSTRY" in item for item in prep.diagnostics)


def test_duplicate_same_day_industry_membership_rejected() -> None:
    n = 5
    assets = _prices({"A": [100 + i for i in range(n)]})
    dates = pd.bdate_range("2024-01-01", periods=n)
    bench = _industry_prices({"I1": [1000 + i for i in range(n)], "I2": [1000 + i for i in range(n)]})
    industry = pd.DataFrame(
        [
            {"trade_date": dates[0], "asset_id": "A", "industry_code": "I1"},
            {"trade_date": dates[0], "asset_id": "A", "industry_code": "I2"},
        ]
    )
    with pytest.raises(ResidualDataError, match="duplicate"):
        prepare_industry_residual_panel(
            assets,
            industry_benchmark_prices=bench,
            industry_panel=industry,
            window=3,
            min_periods=3,
            z_window=3,
            compute_residuals=False,
        )


def test_illegal_industry_prices_do_not_form_valid_return() -> None:
    n = 8
    assets = _prices({"A": [100 + i for i in range(n)]})
    dates = pd.bdate_range("2024-01-01", periods=n)
    closes = [1000, 1001, 1002, 0.0, 1004, 1005, 1006, 1007]
    bench = _industry_prices({"I1": closes})
    industry = _flat_panel_industry(["A"], dates, lambda a, d: "I1")
    prep = prepare_industry_residual_panel(
        assets,
        industry_benchmark_prices=bench,
        industry_panel=industry,
        window=3,
        min_periods=3,
        z_window=3,
    )
    # Return at illegal current and next day involving illegal previous should be NaN.
    assert prep.panel["benchmark_return"].isna().any()
    bad = prep.panel[prep.panel["benchmark_return"].isna()]
    if "residual_return" in bad.columns:
        assert bad["residual_return"].isna().all()


def test_timezone_aware_dates_keep_local_calendar() -> None:
    n = 6
    tz = ZoneInfo("Asia/Shanghai")
    dates = [datetime(2024, 1, 2 + i, 0, 30, tzinfo=tz) for i in range(n)]
    assets = pd.DataFrame(
        [
            {
                "trade_date": d,
                "asset_id": "A",
                "ticker": "A",
                "close": 100 + i,
                "open": 100 + i,
            }
            for i, d in enumerate(dates)
        ]
    )
    bench = pd.DataFrame(
        [
            {"trade_date": d, "industry_code": "I1", "close": 1000 + i}
            for i, d in enumerate(dates)
        ]
    )
    industry = pd.DataFrame(
        [{"trade_date": d, "asset_id": "A", "industry_code": "I1"} for d in dates]
    )
    prep = prepare_industry_residual_panel(
        assets,
        industry_benchmark_prices=bench,
        industry_panel=industry,
        window=3,
        min_periods=3,
        z_window=3,
        compute_residuals=False,
    )
    assert normalize_trade_date(prep.panel.iloc[0]["trade_date"]) == normalize_trade_date(
        "2024-01-02"
    )


def test_shuffled_input_deterministic_and_immutable() -> None:
    n = 12
    assets = _prices({"A": [100 + i for i in range(n)], "B": [90 + i for i in range(n)]})
    dates = pd.bdate_range("2024-01-01", periods=n)
    bench = _industry_prices(
        {"I1": [1000 + i for i in range(n)], "I2": [1100 + i for i in range(n)]}
    )
    industry = _flat_panel_industry(
        ["A", "B"], dates, lambda asset, date: "I1" if asset == "A" else "I2"
    )
    assets_shuf = assets.sample(frac=1.0, random_state=7).reset_index(drop=True)
    bench_shuf = bench.sample(frac=1.0, random_state=3).reset_index(drop=True)
    industry_shuf = industry.sample(frac=1.0, random_state=5).reset_index(drop=True)
    assets_before = assets_shuf.copy(deep=True)
    prep1 = prepare_industry_residual_panel(
        assets_shuf,
        industry_benchmark_prices=bench_shuf,
        industry_panel=industry_shuf,
        window=4,
        min_periods=4,
        z_window=4,
    )
    prep2 = prepare_industry_residual_panel(
        assets.sample(frac=1.0, random_state=11).reset_index(drop=True),
        industry_benchmark_prices=bench.sample(frac=1.0, random_state=13).reset_index(drop=True),
        industry_panel=industry.sample(frac=1.0, random_state=17).reset_index(drop=True),
        window=4,
        min_periods=4,
        z_window=4,
    )
    pd.testing.assert_frame_equal(prep1.panel, prep2.panel)
    pd.testing.assert_frame_equal(assets_before, assets_shuf)


def test_future_industry_or_price_mutation_does_not_change_past_residual() -> None:
    n = 16
    assets = _prices({"A": [100 + i + (0.5 if i > 10 else 0) for i in range(n)]})
    dates = pd.bdate_range("2024-01-01", periods=n)
    bench = _industry_prices({"I1": [1000 + i for i in range(n)]})
    industry = _flat_panel_industry(["A"], dates, lambda a, d: "I1")
    prep1 = prepare_industry_residual_panel(
        assets,
        industry_benchmark_prices=bench,
        industry_panel=industry,
        window=5,
        min_periods=5,
        z_window=5,
    )
    past_mask = prep1.panel["trade_date"] <= dates[8]
    past1 = prep1.panel.loc[past_mask, ["residual_return", "residual_zscore", "benchmark_return"]].copy()

    # Mutate future industry and future industry prices only.
    industry2 = industry.copy()
    industry2.loc[industry2["trade_date"] > dates[8], "industry_code"] = "I2"
    bench2 = bench.copy()
    # add I2 future prices
    extra = pd.DataFrame(
        {
            "trade_date": dates[9:],
            "industry_code": "I2",
            "close": [2000 + i for i in range(len(dates[9:]))],
        }
    )
    bench2 = pd.concat([bench2, extra], ignore_index=True)
    bench2.loc[bench2["trade_date"] > dates[8], "close"] = (
        bench2.loc[bench2["trade_date"] > dates[8], "close"] * 1.5
    )
    prep2 = prepare_industry_residual_panel(
        assets,
        industry_benchmark_prices=bench2,
        industry_panel=industry2,
        window=5,
        min_periods=5,
        z_window=5,
    )
    past2 = prep2.panel.loc[prep2.panel["trade_date"] <= dates[8], ["residual_return", "residual_zscore", "benchmark_return"]]
    pd.testing.assert_frame_equal(
        past1.reset_index(drop=True), past2.reset_index(drop=True), check_dtype=False
    )


def test_mutually_exclusive_benchmark_inputs() -> None:
    n = 5
    assets = _prices({"A": [100 + i for i in range(n)]})
    dates = pd.bdate_range("2024-01-01", periods=n)
    prices = _industry_prices({"I1": [1000 + i for i in range(n)]})
    returns = pd.DataFrame(
        {
            "trade_date": dates,
            "industry_code": "I1",
            "benchmark_return": [0.01] * n,
        }
    )
    industry = _flat_panel_industry(["A"], dates, lambda a, d: "I1")
    with pytest.raises(ResidualDataError, match="not both"):
        prepare_industry_residual_panel(
            assets,
            industry_benchmark_prices=prices,
            industry_benchmark_returns=returns,
            industry_panel=industry,
            compute_residuals=False,
        )


def test_run_industry_residual_research_metadata() -> None:
    n = 25
    assets = _prices(
        {
            "A": list(np.cumsum(np.random.default_rng(1).normal(0.001, 0.01, n)) + 100),
            "B": list(np.cumsum(np.random.default_rng(2).normal(0.0, 0.01, n)) + 100),
        }
    )
    dates = pd.bdate_range("2024-01-01", periods=n)
    bench = _industry_prices(
        {
            "I1": list(np.cumsum(np.random.default_rng(3).normal(0.001, 0.005, n)) + 1000),
            "I2": list(np.cumsum(np.random.default_rng(4).normal(0.0, 0.005, n)) + 1000),
        }
    )
    industry = _flat_panel_industry(
        ["A", "B"], dates, lambda asset, date: "I1" if asset == "A" else "I2"
    )
    result = run_industry_residual_research(
        assets,
        industry_benchmark_prices=bench,
        industry_panel=industry,
        window=5,
        min_periods=5,
        z_window=5,
        n_groups=3,
        horizons=(1, 3),
    )
    assert not result.residual_panel.empty
    assert result.metadata["benchmark_kind"] == "industry"
    assert result.metadata["industry_count"] == 2
    assert "industry_summary" in result.metadata


def test_query_industry_as_of_integration(tmp_path) -> None:
    from datetime import date, datetime

    from qrp_atlas.contracts import INDUSTRY_MEMBERSHIP_HISTORY, init_database

    db_path = tmp_path / "pit.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        init_database(con)
        # Seed a minimal trading calendar around the switch.
        cal_rows = []
        d = date(2024, 1, 1)
        while d <= date(2024, 1, 31):
            if d.weekday() < 5:
                cal_rows.append((d, True, d.year, d.month, (d.month - 1) // 3 + 1))
            d = date.fromordinal(d.toordinal() + 1)
        con.executemany("INSERT INTO trading_calendar VALUES (?, ?, ?, ?, ?)", cal_rows)

        industry = pd.DataFrame(
            [
                {
                    "asset_id": "A",
                    "classification_system": "sw2021",
                    "industry_level": 1,
                    "industry_code": "OLD",
                    "industry_name": "Old",
                    "effective_from": date(2020, 1, 2),
                    "effective_to": date(2024, 1, 10),
                    "available_trade_date": date(2020, 1, 3),
                    "source": "test",
                    "source_record_id": "old",
                    "revision_id": "rev_old",
                    "ingested_at": datetime(2020, 1, 3, 1, 0, 0),
                },
                {
                    "asset_id": "A",
                    "classification_system": "sw2021",
                    "industry_level": 1,
                    "industry_code": "NEW",
                    "industry_name": "New",
                    "effective_from": date(2024, 1, 10),
                    "effective_to": None,
                    "available_trade_date": date(2024, 1, 10),
                    "source": "test",
                    "source_record_id": "new",
                    "revision_id": "rev_new",
                    "ingested_at": datetime(2024, 1, 10, 1, 0, 0),
                },
            ]
        )
        con.register("tmp_ind", industry)
        cols = ", ".join(industry.columns)
        con.execute(
            f"INSERT INTO {INDUSTRY_MEMBERSHIP_HISTORY.name} ({cols}) SELECT {cols} FROM tmp_ind"
        )
        con.unregister("tmp_ind")
    finally:
        con.close()

    n = 15
    assets = _prices({"A": [100 + i for i in range(n)]}, start="2024-01-02")
    bench = _industry_prices(
        {
            "OLD": [1000 + i for i in range(n)],
            "NEW": [2000 + i for i in range(n)],
        },
        start="2024-01-02",
    )
    prep = prepare_industry_residual_panel(
        assets,
        industry_benchmark_prices=bench,
        db_path=db_path,
        classification_system="sw2021",
        industry_level=1,
        window=3,
        min_periods=3,
        z_window=3,
        compute_residuals=False,
    )
    early = prep.panel[prep.panel["trade_date"] == normalize_trade_date("2024-01-03")].iloc[0]
    late = prep.panel[prep.panel["trade_date"] == normalize_trade_date("2024-01-11")].iloc[0]
    assert early["industry_code"] == "OLD"
    assert late["industry_code"] == "NEW"
    pit_early = query_industry_as_of(
        as_of_date="2024-01-03", asset_ids="A", industry_level=1, db_path=db_path
    )
    assert pit_early.iloc[0]["industry_code"] == "OLD"
